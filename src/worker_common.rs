// SPDX-License-Identifier: MPL-2.0

use io_uring::types::{SubmitArgs, Timespec};
use io_uring::IoUring;
use io_uring::{cqueue, opcode, squeue};
use std::collections::{HashSet, VecDeque};
use std::io;
use std::time::Duration;

pub const OP_CONNECT: u64 = 0;
pub const OP_SEND: u64 = 1;
pub const OP_RECV: u64 = 2;

const CANCEL_USER_DATA_BIT: u64 = 1 << 63;
// Keep cancellation-triggered async-poll task work bounded on older kernels.
const SHUTDOWN_CANCEL_BATCH: usize = 32;
const SHUTDOWN_WAIT: Duration = Duration::from_millis(100);

/// Retains submissions when the SQ is full instead of losing an operation on
/// an unchecked submit/push retry. Pending connection flags cover both queued
/// and kernel-owned operations, keeping their buffers stable in either case.
pub struct WorkerRing {
    ring: IoUring,
    deferred: VecDeque<squeue::Entry>,
}

impl WorkerRing {
    pub fn new(entries: u32) -> io::Result<Self> {
        Ok(Self {
            ring: IoUring::new(entries)?,
            deferred: VecDeque::new(),
        })
    }

    /// # Safety
    /// All pointers in the entry must remain valid until completion or until
    /// `shutdown` has cancelled and drained the operation.
    pub unsafe fn push(&mut self, entry: squeue::Entry) {
        if !self.deferred.is_empty() || unsafe { self.ring.submission().push(&entry) }.is_err() {
            self.deferred.push_back(entry);
        }
    }

    pub fn wait(&mut self, timeout: Duration) -> io::Result<()> {
        {
            let mut sq = self.ring.submission();
            while let Some(entry) = self.deferred.front() {
                // SAFETY: push's caller retains the buffers through completion.
                if unsafe { sq.push(entry) }.is_err() {
                    break;
                }
                self.deferred.pop_front();
            }
        }
        // Submit another batch promptly when locally queued work remains.
        let wait_count = usize::from(!timeout.is_zero() && self.deferred.is_empty());
        let ts = Timespec::new()
            .sec(timeout.as_secs())
            .nsec(timeout.subsec_nanos());
        let args = SubmitArgs::new().timespec(&ts);
        drain_after_submit(self.ring.submitter().submit_with_args(wait_count, &args))
    }

    pub fn completion(&mut self) -> cqueue::CompletionQueue<'_> {
        self.ring.completion()
    }

    pub fn shutdown(&mut self, pending: Vec<u64>) -> io::Result<()> {
        // These entries never reached the kernel. Cancelling their user_data
        // yields ENOENT, which the common cancellation path already handles.
        self.deferred.clear();
        cancel_pending_operations(&mut self.ring, pending)
    }
}

fn drain_after_submit(result: io::Result<usize>) -> io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        // Always allow the caller to reap CQEs, including on EBUSY. Retrying
        // before draining can leave an overflowing completion queue stuck.
        Err(ref error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINTR | libc::EBUSY | libc::ETIME)
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub fn encode_user_data(conn_key: usize, op: u64) -> u64 {
    (op << 48) | (conn_key as u64)
}

pub fn decode_user_data(data: u64) -> (usize, u64) {
    let conn_key = (data & 0x0000_FFFF_FFFF_FFFF) as usize;
    let op = data >> 48;
    (conn_key, op)
}

pub fn pending_user_data(
    conn_key: usize,
    connect_pending: bool,
    send_pending: bool,
    recv_pending: bool,
) -> impl Iterator<Item = u64> {
    [
        connect_pending.then(|| encode_user_data(conn_key, OP_CONNECT)),
        send_pending.then(|| encode_user_data(conn_key, OP_SEND)),
        recv_pending.then(|| encode_user_data(conn_key, OP_RECV)),
    ]
    .into_iter()
    .flatten()
}

/// Cancel and reap every operation before its socket and backing buffers are dropped.
pub fn cancel_pending_operations(ring: &mut IoUring, pending: Vec<u64>) -> io::Result<()> {
    let mut shutdown = ShutdownCompletions::new(&pending);
    drain_shutdown_completions(ring, &mut shutdown);

    for pending_batch in pending.chunks(SHUTDOWN_CANCEL_BATCH) {
        let batch: Vec<_> = pending_batch
            .iter()
            .filter(|target| shutdown.operations.contains(target))
            .copied()
            .collect();
        if batch.is_empty() {
            continue;
        }

        for &target in &batch {
            let cancel_data = cancel_user_data(target);
            let cancel = opcode::AsyncCancel::new(target)
                .build()
                .user_data(cancel_data);

            loop {
                if unsafe { ring.submission().push(&cancel) }.is_ok() {
                    shutdown.cancellations.insert(cancel_data);
                    break;
                }
                submit_shutdown_batch(ring, &mut shutdown)?;
            }
        }

        submit_shutdown_batch(ring, &mut shutdown)?;
        wait_for_shutdown_batch(ring, &mut shutdown, &batch)?;
    }

    debug_assert!(shutdown.is_empty());
    Ok(())
}

fn wait_for_shutdown_batch(
    ring: &mut IoUring,
    shutdown: &mut ShutdownCompletions,
    targets: &[u64],
) -> io::Result<()> {
    while !shutdown.batch_is_complete(targets) {
        drain_shutdown_completions(ring, shutdown);
        if shutdown.batch_is_complete(targets) {
            break;
        }

        let ts = Timespec::new()
            .sec(SHUTDOWN_WAIT.as_secs())
            .nsec(SHUTDOWN_WAIT.subsec_nanos());
        let args = SubmitArgs::new().timespec(&ts);
        match ring.submitter().submit_with_args(1, &args) {
            Ok(_) => {}
            Err(ref error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EINTR | libc::EBUSY | libc::ETIME)
                ) => {}
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn submit_shutdown_batch(ring: &mut IoUring, shutdown: &mut ShutdownCompletions) -> io::Result<()> {
    loop {
        match ring.submitter().submit() {
            Ok(_) => break,
            Err(ref error) if matches!(error.raw_os_error(), Some(libc::EINTR | libc::EBUSY)) => {
                drain_shutdown_completions(ring, shutdown);
            }
            Err(error) => return Err(error),
        }
    }
    drain_shutdown_completions(ring, shutdown);
    Ok(())
}

fn drain_shutdown_completions(ring: &mut IoUring, shutdown: &mut ShutdownCompletions) {
    for cqe in ring.completion() {
        shutdown.record(cqe.user_data(), cqe.result());
    }
}

struct ShutdownCompletions {
    operations: HashSet<u64>,
    cancellations: HashSet<u64>,
}

impl ShutdownCompletions {
    fn new(pending: &[u64]) -> Self {
        Self {
            operations: pending.iter().copied().collect(),
            cancellations: HashSet::with_capacity(pending.len()),
        }
    }

    fn is_empty(&self) -> bool {
        self.operations.is_empty() && self.cancellations.is_empty()
    }

    fn batch_is_complete(&self, targets: &[u64]) -> bool {
        targets.iter().all(|target| {
            !self.operations.contains(target)
                && !self.cancellations.contains(&cancel_user_data(*target))
        })
    }

    fn record(&mut self, user_data: u64, result: i32) {
        if let Some(target) = cancel_target(user_data) {
            self.cancellations.remove(&user_data);
            // ENOENT means no request with this user_data remains in the ring.
            if result == -libc::ENOENT {
                self.operations.remove(&target);
            }
        } else {
            self.operations.remove(&user_data);
        }
    }
}

fn cancel_user_data(target: u64) -> u64 {
    debug_assert_eq!(target & CANCEL_USER_DATA_BIT, 0);
    target | CANCEL_USER_DATA_BIT
}

fn cancel_target(user_data: u64) -> Option<u64> {
    (user_data & CANCEL_USER_DATA_BIT != 0).then_some(user_data & !CANCEL_USER_DATA_BIT)
}

pub struct WorkerResult {
    pub latency_samples: Vec<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires Linux io_uring permission"]
    fn queued_operations_survive_a_full_submission_queue() {
        let mut ring = WorkerRing::new(8).unwrap();
        for key in 0..100u64 {
            // NOPs have no borrowed buffers.
            unsafe {
                ring.push(opcode::Nop::new().build().user_data(key));
            }
        }
        assert!(!ring.deferred.is_empty());
        let mut completed = HashSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while completed.len() < 100 && std::time::Instant::now() < deadline {
            ring.wait(Duration::from_millis(10)).unwrap();
            for cqe in ring.completion() {
                assert_eq!(cqe.result(), 0);
                assert!(completed.insert(cqe.user_data()));
            }
        }
        assert_eq!(completed.len(), 100);
        assert!(ring.deferred.is_empty());
    }

    #[test]
    #[ignore = "requires Linux io_uring permission"]
    fn overflow_and_busy_recovery_preserve_completions() {
        let mut ring = WorkerRing::new(8).unwrap();
        for batch in 0..3 {
            for offset in 0..8 {
                unsafe {
                    ring.push(opcode::Nop::new().build().user_data(batch * 8 + offset));
                }
            }
            ring.wait(Duration::ZERO).unwrap();
        }
        assert!(ring.ring.submission().cq_overflow());
        // Current kernels may return success while CQEs are already visible.
        // Inject EBUSY at the result boundary to verify that it permits draining.
        drain_after_submit(Err(io::Error::from_raw_os_error(libc::EBUSY))).unwrap();
        let mut completed: HashSet<_> = ring.completion().map(|cqe| cqe.user_data()).collect();
        assert_eq!(completed.len(), 16);
        ring.wait(Duration::ZERO).unwrap();
        completed.extend(ring.completion().map(|cqe| cqe.user_data()));
        assert_eq!(completed.len(), 24);
        assert!(!ring.ring.submission().cq_overflow());
    }

    #[test]
    fn pending_user_data_contains_only_active_operations() {
        assert_eq!(
            pending_user_data(42, true, false, true).collect::<Vec<_>>(),
            vec![
                encode_user_data(42, OP_CONNECT),
                encode_user_data(42, OP_RECV),
            ]
        );
    }

    #[test]
    fn cancellation_tag_round_trips_target() {
        let target = encode_user_data(123, OP_SEND);
        let tagged = cancel_user_data(target);

        assert_eq!(cancel_target(tagged), Some(target));
        assert_eq!(cancel_target(target), None);
    }

    #[test]
    fn shutdown_waits_for_cancel_after_operation_completion() {
        let target = encode_user_data(123, OP_RECV);
        let cancel = cancel_user_data(target);
        let mut shutdown = ShutdownCompletions::new(&[target]);
        shutdown.cancellations.insert(cancel);

        shutdown.record(target, -libc::ECANCELED);
        assert!(!shutdown.is_empty());

        shutdown.record(cancel, 0);
        assert!(shutdown.is_empty());
    }

    #[test]
    fn shutdown_waits_for_operation_after_cancel_completion() {
        let target = encode_user_data(456, OP_SEND);
        let cancel = cancel_user_data(target);
        let mut shutdown = ShutdownCompletions::new(&[target]);
        shutdown.cancellations.insert(cancel);

        shutdown.record(cancel, 0);
        assert!(!shutdown.is_empty());

        shutdown.record(target, -libc::ECANCELED);
        assert!(shutdown.is_empty());
    }

    #[test]
    fn completed_batch_does_not_wait_for_later_targets() {
        let current = encode_user_data(123, OP_RECV);
        let later = encode_user_data(456, OP_RECV);
        let cancel = cancel_user_data(current);
        let mut shutdown = ShutdownCompletions::new(&[current, later]);
        shutdown.cancellations.insert(cancel);

        shutdown.record(current, -libc::ECANCELED);
        assert!(!shutdown.batch_is_complete(&[current]));

        shutdown.record(cancel, 0);
        assert!(shutdown.batch_is_complete(&[current]));
        assert!(!shutdown.is_empty());
    }

    #[test]
    fn cancel_enoent_completes_missing_operation() {
        let target = encode_user_data(789, OP_CONNECT);
        let cancel = cancel_user_data(target);
        let mut shutdown = ShutdownCompletions::new(&[target]);
        shutdown.cancellations.insert(cancel);

        shutdown.record(cancel, -libc::ENOENT);

        assert!(shutdown.is_empty());
    }
}
