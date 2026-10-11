// SPDX-License-Identifier: MPL-2.0

use io_uring::opcode;
use io_uring::types::Fd;
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::os::fd::{IntoRawFd, RawFd};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::config::BenchConfig;
use crate::connection::{ConnState, Connection, EventOutcome};
use crate::scheduler::{ConnectPacer, Scheduler};
use crate::stats::{BenchStats, ErrorKind};
use crate::worker_common::{
    decode_user_data, encode_user_data, pending_user_data, WorkerResult, WorkerRing, OP_CONNECT,
    OP_RECV, OP_SEND,
};

/// Box allocation keeps every connection's I/O buffers stable while its
/// operations are queued locally or owned by the kernel.
pub fn run_worker(
    worker_id: usize,
    conn_range: std::ops::Range<usize>,
    config: Arc<BenchConfig>,
    stats: Arc<BenchStats>,
) -> WorkerResult {
    let num_conns = conn_range.len();
    if num_conns == 0 {
        return WorkerResult {
            latency_samples: Vec::new(),
        };
    }
    let ring_size = 4096u32.min((num_conns as u32 * 3).next_power_of_two().max(64));
    let mut ring = WorkerRing::new(ring_size)
        .unwrap_or_else(|e| panic!("worker {}: failed to create io_uring: {}", worker_id, e));
    let mut conns: Slab<Box<Connection>> = Slab::with_capacity(num_conns);
    let mut scheduler = Scheduler::new(num_conns);
    let mut cqes = Vec::new();

    // Connect SQEs reference this address until their completion or cancellation.
    let sock_addr_box = Box::new(SockAddr::from(config.addr));
    let sa_ptr = sock_addr_box.as_ref().as_ptr();
    let sa_len = sock_addr_box.as_ref().len();
    let workers = config.workers.min(config.clients).max(1);
    let mut pacer = ConnectPacer::new(config.connect_rate, workers, Instant::now());
    let mut pending_create = conn_range;
    let mut done_count = 0usize;
    let mut ifaddr_idx = 0usize;

    while !stats.stopped.load(Ordering::Relaxed) {
        let count = pacer.take(Instant::now(), pending_create.len());
        for client_idx in pending_create.by_ref().take(count) {
            let bind_ip = if config.ifaddrs.is_empty() {
                None
            } else {
                let ip = config.ifaddrs[ifaddr_idx % config.ifaddrs.len()];
                ifaddr_idx += 1;
                Some(ip)
            };
            match create_socket(&config, bind_ip) {
                Ok(fd) => {
                    let key = conns.insert(Box::new(Connection::new(fd, client_idx, &config)));
                    let entry = opcode::Connect::new(Fd(fd), sa_ptr, sa_len)
                        .build()
                        .user_data(encode_user_data(key, OP_CONNECT));
                    // SAFETY: the socket address and connections live until shutdown drains I/O.
                    unsafe {
                        ring.push(entry);
                    }
                }
                Err(e) => {
                    eprintln!("worker {}: socket creation failed: {}", worker_id, e);
                    stats.record_error(ErrorKind::Socket);
                    stats.clients_done.fetch_add(1, Ordering::Relaxed);
                    done_count += 1;
                }
            }
        }
        if done_count >= num_conns && pending_create.is_empty() {
            break;
        }

        let ramp = (!pending_create.is_empty()).then(|| pacer.next_at());
        let wait = scheduler.wait_duration(Instant::now(), ramp);
        if let Err(e) = ring.wait(wait) {
            eprintln!("worker {}: io_uring submit error: {}", worker_id, e);
            break;
        }

        // Reuse the completion buffer and visit only connections with I/O or
        // expired deadlines. EBUSY also reaches this draining path.
        cqes.extend(ring.completion());
        for cqe in cqes.drain(..) {
            let (key, op) = decode_user_data(cqe.user_data());
            if !conns.contains(key) {
                continue;
            }
            let conn = &mut conns[key];
            match op {
                OP_CONNECT => handle_connect_complete(
                    conn,
                    key,
                    cqe.result(),
                    &mut ring,
                    &stats,
                    &mut done_count,
                ),
                OP_SEND => handle_send_complete(conn, cqe.result(), &stats, &mut done_count),
                OP_RECV => handle_recv_complete(
                    conn,
                    key,
                    cqe.result(),
                    &mut ring,
                    &config,
                    &stats,
                    &mut done_count,
                ),
                _ => continue,
            }
            scheduler.mark_ready(key);
        }
        scheduler.wake_due(Instant::now());
        // A publisher may requeue itself. Limit this pass so I/O, timers and
        // shutdown still get serviced between batches of ready connections.
        for _ in 0..scheduler.ready_len() {
            let key = scheduler.pop_ready().unwrap();
            let conn = &mut conns[key];
            let again = drive_connection(conn, key, &mut ring, &config, &stats, &mut done_count);
            scheduler.schedule(key, next_deadline(conn));
            if again {
                scheduler.mark_ready(key);
            }
        }
        if done_count >= num_conns && pending_create.is_empty() {
            break;
        }
    }

    let pending = conns
        .iter()
        .flat_map(|(key, conn)| {
            pending_user_data(
                key,
                conn.state == ConnState::TcpConnecting,
                conn.send_pending,
                conn.recv_pending,
            )
        })
        .collect();
    // Cancel ring operations before shutting down sockets. Bulk shutdown first can
    // wake thousands of async-poll receives into task_work on older kernels.
    if let Err(error) = ring.shutdown(pending) {
        eprintln!("worker {}: io_uring shutdown failed: {}", worker_id, error);
        unsafe {
            libc::_exit(if stats.stopped.load(Ordering::Relaxed) {
                130
            } else {
                1
            });
        }
    }

    // The kernel no longer references connection buffers, so they can be dropped safely.
    let mut all_samples = Vec::new();
    for (_, conn) in conns.iter_mut() {
        all_samples.append(&mut conn.latency_samples);
    }

    for (_, conn) in conns.iter() {
        unsafe {
            libc::shutdown(conn.fd, libc::SHUT_RDWR);
            libc::close(conn.fd);
        }
    }

    drop(sock_addr_box);

    WorkerResult {
        latency_samples: all_samples,
    }
}

fn create_socket(
    config: &BenchConfig,
    bind_addr: Option<std::net::IpAddr>,
) -> std::io::Result<RawFd> {
    let domain = if config.addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.set_nodelay(true)?;
    socket.set_recv_buffer_size(config.socket_buf)?;
    socket.set_send_buffer_size(config.socket_buf)?;

    // Bind to a specific source IP; port 0 lets the OS pick an ephemeral port
    if let Some(ip) = bind_addr {
        let bind_sa = SockAddr::from(std::net::SocketAddr::new(ip, 0));
        socket.bind(&bind_sa)?;
    }

    Ok(socket.into_raw_fd())
}

fn handle_connect_complete(
    conn: &mut Connection,
    key: usize,
    result: i32,
    ring: &mut WorkerRing,
    stats: &BenchStats,
    done_count: &mut usize,
) {
    if result < 0 {
        eprintln!(
            "connection {}: TCP connect failed: {}",
            conn.client_index,
            std::io::Error::from_raw_os_error(-result)
        );
        mark_failed(conn, stats, done_count, ErrorKind::Connect);
        return;
    }
    stats.clients_connected.fetch_add(1, Ordering::Relaxed);
    conn.initiate_mqtt_connect();
    if conn.state == ConnState::Failed {
        stats.record_error(ErrorKind::MqttConnect);
        finish_mqtt_failure(stats, done_count);
        return;
    }
    submit_recv(ring, key, conn);
}

fn handle_send_complete(
    conn: &mut Connection,
    result: i32,
    stats: &BenchStats,
    done_count: &mut usize,
) {
    conn.send_pending = false;
    if matches!(conn.state, ConnState::Done | ConnState::Failed) {
        return;
    }
    if result <= 0 {
        if result < 0 && !matches!(-result, libc::ECONNRESET | libc::EPIPE) {
            eprintln!(
                "connection {}: send failed: {}",
                conn.client_index,
                std::io::Error::from_raw_os_error(-result)
            );
        }
        mark_failed(conn, stats, done_count, ErrorKind::Send);
        return;
    }
    conn.advance_send(result as usize);
}

fn handle_recv_complete(
    conn: &mut Connection,
    key: usize,
    result: i32,
    ring: &mut WorkerRing,
    config: &BenchConfig,
    stats: &BenchStats,
    done_count: &mut usize,
) {
    conn.recv_pending = false;
    if matches!(conn.state, ConnState::Done | ConnState::Failed) {
        return;
    }
    if result <= 0 {
        if result < 0 && -result != libc::ECONNRESET {
            eprintln!(
                "connection {}: recv error: {}",
                conn.client_index,
                std::io::Error::from_raw_os_error(-result)
            );
        }
        if conn.state != ConnState::Disconnecting {
            mark_failed(conn, stats, done_count, ErrorKind::Receive);
        }
        return;
    }
    let data = conn.recv_buf[..result as usize].to_vec();
    let events = conn.handle_incoming(&data);
    let outcome = conn.process_events(events, config);
    record_mqtt_outcome(stats, &outcome);
    if outcome.acked > 0 {
        stats
            .messages_acked
            .fetch_add(outcome.acked, Ordering::Relaxed);
    }
    if outcome.failed {
        finish_mqtt_failure(stats, done_count);
    }
    conn.check_receive_complete();
    if !matches!(
        conn.state,
        ConnState::Done | ConnState::Failed | ConnState::Disconnecting
    ) {
        submit_recv(ring, key, conn);
    }
}

fn drive_connection(
    conn: &mut Connection,
    key: usize,
    ring: &mut WorkerRing,
    config: &BenchConfig,
    stats: &BenchStats,
    done_count: &mut usize,
) -> bool {
    if matches!(
        conn.state,
        ConnState::Done | ConnState::Failed | ConnState::TcpConnecting
    ) {
        return false;
    }
    let events = conn.handle_tick();
    if !events.is_empty() {
        let outcome = conn.process_events(events, config);
        record_mqtt_outcome(stats, &outcome);
        if outcome.acked > 0 {
            stats
                .messages_acked
                .fetch_add(outcome.acked, Ordering::Relaxed);
        }
        if outcome.failed {
            finish_mqtt_failure(stats, done_count);
            return false;
        }
    }
    let published =
        conn.state == ConnState::Publishing && !conn.send_pending && conn.try_publish(config);
    if published {
        stats.messages_sent.fetch_add(1, Ordering::Relaxed);
    }
    conn.check_publish_complete();
    conn.check_receive_complete();
    if !conn.send_pending {
        conn.check_drain_complete();
        submit_send(ring, key, conn);
    }
    if conn.state == ConnState::Disconnecting && !conn.send_pending && !conn.has_pending_send() {
        conn.state = ConnState::Done;
        stats.clients_done.fetch_add(1, Ordering::Relaxed);
        *done_count += 1;
    }
    // A successful publish can be queued inside the MQTT engine without
    // producing bytes yet. Continue admitting work until backpressure stops it.
    published
        && conn.state == ConnState::Publishing
        && !conn.send_pending
        && !conn.has_pending_send()
        && config.interval_ms == 0
}

fn next_deadline(conn: &Connection) -> Option<Instant> {
    if matches!(
        conn.state,
        ConnState::Done | ConnState::Failed | ConnState::TcpConnecting
    ) {
        return None;
    }
    let publish =
        if conn.state == ConnState::Publishing && !conn.send_pending && !conn.has_pending_send() {
            conn.next_publish_at
        } else {
            None
        };
    let drain = if conn.state == ConnState::Draining && !conn.send_pending {
        conn.drain_deadline
    } else {
        None
    };
    conn.next_tick_at()
        .into_iter()
        .chain(publish)
        .chain(drain)
        .min()
}

fn mark_failed(
    conn: &mut Connection,
    stats: &BenchStats,
    done_count: &mut usize,
    error_kind: ErrorKind,
) {
    if conn.state != ConnState::Failed && conn.state != ConnState::Done {
        conn.state = ConnState::Failed;
        stats.record_error(error_kind);
        stats.clients_done.fetch_add(1, Ordering::Relaxed);
        *done_count += 1;
    }
}

fn record_mqtt_outcome(stats: &BenchStats, outcome: &EventOutcome) {
    if outcome.subscribed {
        stats.clients_subscribed.fetch_add(1, Ordering::Relaxed);
    }
    if outcome.received > 0 {
        stats
            .messages_received
            .fetch_add(outcome.received, Ordering::Relaxed);
    }
    stats.record_puback_no_match(outcome.puback_no_match);
    stats.record_errors(ErrorKind::MqttConnect, outcome.mqtt_connect_errors);
    stats.record_errors(ErrorKind::MqttSubscribe, outcome.mqtt_subscribe_errors);
    stats.record_errors(ErrorKind::MqttPublish, outcome.mqtt_publish_errors);
    stats.record_errors(ErrorKind::MqttClient, outcome.mqtt_client_errors);
    stats.record_errors(ErrorKind::MqttDisconnect, outcome.mqtt_disconnect_errors);
}

fn finish_mqtt_failure(stats: &BenchStats, done_count: &mut usize) {
    stats.clients_done.fetch_add(1, Ordering::Relaxed);
    *done_count += 1;
}

fn submit_send(ring: &mut WorkerRing, key: usize, conn: &mut Connection) {
    if !conn.has_pending_send() || conn.send_pending {
        return;
    }
    let slice = conn.pending_send_slice();
    let ptr = slice.as_ptr();
    let len = slice.len() as u32;

    let send_e = opcode::Send::new(Fd(conn.fd), ptr, len)
        .build()
        .user_data(encode_user_data(key, OP_SEND));

    conn.send_pending = true;
    unsafe {
        ring.push(send_e);
    }
}

fn submit_recv(ring: &mut WorkerRing, key: usize, conn: &mut Connection) {
    if conn.recv_pending {
        return;
    }
    let buf_ptr = conn.recv_buf.as_mut_ptr();
    let buf_len = conn.recv_buf.len() as u32;

    let recv_e = opcode::Recv::new(Fd(conn.fd), buf_ptr, buf_len)
        .build()
        .user_data(encode_user_data(key, OP_RECV));

    conn.recv_pending = true;
    unsafe {
        ring.push(recv_e);
    }
}

#[cfg(test)]
mod scheduling_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn publish_and_drain_deadlines_wake_without_mqtt_timers() {
        let config = BenchConfig {
            keep_alive: 0,
            ..BenchConfig::default()
        };
        let mut conn = Connection::new(-1, 0, &config);
        conn.initiate_mqtt_connect();
        let events = conn.handle_incoming(&[0x20, 0x03, 0, 0, 0]);
        assert!(conn.process_events(events, &config).connected);
        while conn.has_pending_send() {
            conn.advance_send(conn.pending_send_slice().len());
        }
        assert_eq!(conn.next_tick_at(), None);
        // Even a deadline that elapsed before rescheduling must wake the worker.
        let at = Instant::now() - Duration::from_millis(1);
        conn.next_publish_at = Some(at);
        assert_eq!(next_deadline(&conn), Some(at));
        conn.send_pending = true;
        assert_eq!(next_deadline(&conn), None);
        conn.send_pending = false;
        conn.state = ConnState::Draining;
        let drain = Instant::now() + Duration::from_secs(10);
        conn.drain_deadline = Some(drain);
        assert_eq!(next_deadline(&conn), Some(drain));
        conn.state = ConnState::Done;
        assert_eq!(next_deadline(&conn), None);
    }

    #[test]
    fn rejected_publish_consumes_its_due_interval() {
        let config = BenchConfig {
            keep_alive: 0,
            interval_ms: 10,
            topic: "invalid/#".to_string(),
            ..BenchConfig::default()
        };
        let mut conn = Connection::new(-1, 0, &config);
        conn.state = ConnState::Publishing;
        conn.next_publish_at = Some(Instant::now() - Duration::from_secs(1));
        // A wildcard topic rejects publishing. A rejected attempt must not
        // keep an overdue interval scheduled and cause an immediate retry loop.
        assert!(!conn.try_publish(&config));
        assert_eq!(next_deadline(&conn), None);
    }
}
