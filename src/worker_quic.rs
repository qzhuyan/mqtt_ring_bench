// SPDX-License-Identifier: MPL-2.0

use io_uring::opcode;
use io_uring::types::Fd;
use slab::Slab;
use socket2::{Domain, SockAddr, Socket, Type};
use std::os::fd::IntoRawFd;
use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::config::BenchConfig;
use crate::connection::{EventOutcome, QuicConnState, QuicConnection};
use crate::scheduler::{ConnectPacer, Scheduler};
use crate::stats::{BenchStats, ErrorKind};
use crate::worker_common::{
    decode_user_data, encode_user_data, pending_user_data, WorkerResult, WorkerRing, OP_RECV,
    OP_SEND,
};

pub fn run_quic_worker(
    worker_id: usize,
    conn_range: std::ops::Range<usize>,
    config: Arc<BenchConfig>,
    stats: Arc<BenchStats>,
    crypto: Arc<rustls::ClientConfig>,
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

    let mut conns: Slab<Box<QuicConnection>> = Slab::with_capacity(num_conns);
    let mut scheduler = Scheduler::new(num_conns);
    let mut cqes = Vec::new();

    let server_name = config
        .server_name
        .as_deref()
        .unwrap_or(&config.host)
        .to_string();

    let workers = config.workers.min(config.clients).max(1);
    let mut pacer = ConnectPacer::new(config.connect_rate, workers, Instant::now());
    let mut pending_create: Vec<usize> = conn_range.collect();
    pending_create.reverse();

    let mut done_count: usize = 0;
    let mut ifaddr_idx: usize = 0;

    loop {
        if stats.stopped.load(Ordering::Relaxed) {
            break;
        }

        let now = Instant::now();

        // Keep connection ramp-up in the same deadline-driven loop.
        let count = pacer.take(now, pending_create.len());
        for _ in 0..count {
            if let Some(client_idx) = pending_create.pop() {
                let bind_ip = if config.ifaddrs.is_empty() {
                    None
                } else {
                    let ip = config.ifaddrs[ifaddr_idx % config.ifaddrs.len()];
                    ifaddr_idx += 1;
                    Some(ip)
                };
                match create_udp_socket(&config, bind_ip) {
                    Ok(fd) => {
                        // "connect" the UDP socket so Send/Recv work without sendto/recvfrom
                        let ret = udp_connect(fd, &config.addr);
                        if ret < 0 {
                            let e = std::io::Error::last_os_error();
                            eprintln!(
                                "worker {}: UDP connect failed for client {}: {}",
                                worker_id, client_idx, e
                            );
                            unsafe {
                                libc::close(fd);
                            }
                            stats.record_error(ErrorKind::Connect);
                            stats.clients_done.fetch_add(1, Ordering::Relaxed);
                            done_count += 1;
                            continue;
                        }

                        match QuicConnection::new(fd, client_idx, &config, config.addr) {
                            Ok(mut conn) => {
                                let crypto_clone = (*crypto).clone();
                                match conn.initiate_quic_connect(crypto_clone, &server_name, now) {
                                    Ok(()) => {
                                        let key = conns.insert(Box::new(conn));
                                        // Submit initial handshake datagrams + recv
                                        submit_front_send(&mut ring, key, &mut conns[key]);
                                        submit_recv(&mut ring, key, &mut conns[key]);
                                        scheduler.mark_ready(key);
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "worker {}: QUIC connect failed for client {}: {}",
                                            worker_id, client_idx, e
                                        );
                                        unsafe {
                                            libc::close(fd);
                                        }
                                        stats.record_error(ErrorKind::Connect);
                                        stats.clients_done.fetch_add(1, Ordering::Relaxed);
                                        done_count += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "worker {}: QuicConnection::new failed for client {}: {}",
                                    worker_id, client_idx, e
                                );
                                unsafe {
                                    libc::close(fd);
                                }
                                stats.record_error(ErrorKind::Connect);
                                stats.clients_done.fetch_add(1, Ordering::Relaxed);
                                done_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("worker {}: UDP socket creation failed: {}", worker_id, e);
                        stats.record_error(ErrorKind::Socket);
                        stats.clients_done.fetch_add(1, Ordering::Relaxed);
                        done_count += 1;
                    }
                }
            } else {
                break;
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

        cqes.extend(ring.completion());
        for cqe in cqes.drain(..) {
            let (conn_key, op) = decode_user_data(cqe.user_data());
            let result = cqe.result();

            if !conns.contains(conn_key) {
                continue;
            }

            match op {
                OP_SEND => {
                    handle_send_complete(
                        &mut conns,
                        conn_key,
                        result,
                        &mut ring,
                        &stats,
                        &mut done_count,
                    );
                }
                OP_RECV => {
                    handle_recv_complete(
                        &mut conns,
                        conn_key,
                        result,
                        &mut ring,
                        &config,
                        &stats,
                        &mut done_count,
                    );
                }
                _ => continue,
            }
            scheduler.mark_ready(conn_key);
        }

        scheduler.wake_due(Instant::now());
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
        .flat_map(|(key, conn)| pending_user_data(key, false, conn.send_pending, conn.recv_pending))
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

    WorkerResult {
        latency_samples: all_samples,
    }
}

fn create_udp_socket(
    config: &BenchConfig,
    bind_addr: Option<std::net::IpAddr>,
) -> std::io::Result<RawFd> {
    let domain = if config.addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, None)?;
    socket.set_nonblocking(true)?;
    socket.set_recv_buffer_size(config.socket_buf)?;
    socket.set_send_buffer_size(config.socket_buf)?;

    let bind_sa = match bind_addr {
        Some(ip) => SockAddr::from(std::net::SocketAddr::new(ip, 0)),
        None => {
            // Bind to 0.0.0.0:0 so the OS assigns an ephemeral port
            if config.addr.is_ipv4() {
                SockAddr::from(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    0,
                ))
            } else {
                SockAddr::from(std::net::SocketAddr::new(
                    std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                    0,
                ))
            }
        }
    };
    socket.bind(&bind_sa)?;

    Ok(socket.into_raw_fd())
}

/// Connect a UDP socket to the given address using libc::connect.
/// This sets the default peer so Send/Recv work like on a connected socket.
fn udp_connect(fd: RawFd, addr: &std::net::SocketAddr) -> i32 {
    let sa = SockAddr::from(*addr);
    unsafe { libc::connect(fd, sa.as_ptr(), sa.len() as libc::socklen_t) }
}

fn handle_send_complete(
    conns: &mut Slab<Box<QuicConnection>>,
    key: usize,
    result: i32,
    ring: &mut WorkerRing,
    stats: &BenchStats,
    done_count: &mut usize,
) {
    let conn = &mut conns[key];
    conn.send_pending = false;
    if matches!(conn.state, QuicConnState::Done | QuicConnState::Failed) {
        return;
    }

    if result < 0 {
        let err_code = -result;
        eprintln!(
            "connection {}: UDP send failed: {}",
            conn.client_index,
            std::io::Error::from_raw_os_error(err_code)
        );
        mark_failed(conn, stats, done_count, ErrorKind::Send);
        return;
    }

    // Datagram sent successfully; pop it from the queue
    conn.pop_front_datagram();

    // If more datagrams in queue, submit next send
    if conn.has_pending_send() {
        submit_front_send(ring, key, conn);
    }
}

fn handle_recv_complete(
    conns: &mut Slab<Box<QuicConnection>>,
    key: usize,
    result: i32,
    ring: &mut WorkerRing,
    config: &BenchConfig,
    stats: &BenchStats,
    done_count: &mut usize,
) {
    let conn = &mut conns[key];
    conn.recv_pending = false;
    if matches!(conn.state, QuicConnState::Done | QuicConnState::Failed) {
        return;
    }

    if result <= 0 {
        if result < 0 {
            let err_code = -result;
            // ECONNREFUSED is common for UDP when the remote port is closed
            if err_code != libc::ECONNREFUSED {
                eprintln!(
                    "connection {}: UDP recv error: {}",
                    conn.client_index,
                    std::io::Error::from_raw_os_error(err_code)
                );
            }
        }
        if conn.state != QuicConnState::Done && conn.state != QuicConnState::Disconnecting {
            mark_failed(conn, stats, done_count, ErrorKind::Receive);
        }
        return;
    }

    let nbytes = result as usize;
    let now = Instant::now();

    // Feed datagram to QUIC engine
    let data: Vec<u8> = conn.recv_buf[..nbytes].to_vec();
    let events = conn.handle_datagram(&data, now);

    if !events.is_empty() {
        let outcome = conn.process_events(events, config);
        if outcome.connected {
            stats.clients_connected.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.acked > 0 {
            stats
                .messages_acked
                .fetch_add(outcome.acked, Ordering::Relaxed);
        }
        record_mqtt_outcome(stats, &outcome);
        if outcome.failed {
            finish_mqtt_failure(stats, done_count);
        }
    }

    conn.check_receive_complete(now);

    // Continue receiving (unless done/failed)
    if !matches!(
        conn.state,
        QuicConnState::Done | QuicConnState::Failed | QuicConnState::Disconnecting
    ) {
        submit_recv(ring, key, conn);
    }
}

fn drive_connection(
    conn: &mut QuicConnection,
    key: usize,
    ring: &mut WorkerRing,
    config: &BenchConfig,
    stats: &BenchStats,
    done_count: &mut usize,
) -> bool {
    if matches!(conn.state, QuicConnState::Done | QuicConnState::Failed) {
        return false;
    }
    let now = Instant::now();
    let events = conn.handle_tick(now);
    if !events.is_empty() {
        let outcome = conn.process_events(events, config);
        if outcome.connected {
            stats.clients_connected.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.acked > 0 {
            stats
                .messages_acked
                .fetch_add(outcome.acked, Ordering::Relaxed);
        }
        record_mqtt_outcome(stats, &outcome);
        if outcome.failed {
            finish_mqtt_failure(stats, done_count);
            return false;
        }
    }
    let published = conn.state == QuicConnState::Publishing
        && !conn.send_pending
        && !conn.has_pending_send()
        && conn.try_publish(config, now);
    if published {
        stats.messages_sent.fetch_add(1, Ordering::Relaxed);
    }
    conn.check_publish_complete();
    conn.check_receive_complete(now);
    conn.check_drain_complete(now);
    if conn.has_pending_send() && !conn.send_pending {
        submit_front_send(ring, key, conn);
    }
    if conn.state == QuicConnState::Disconnecting && !conn.has_pending_send() && !conn.send_pending
    {
        conn.state = QuicConnState::Done;
        stats.clients_done.fetch_add(1, Ordering::Relaxed);
        *done_count += 1;
    }
    published
        && conn.state == QuicConnState::Publishing
        && !conn.send_pending
        && !conn.has_pending_send()
        && config.interval_ms == 0
}

fn next_deadline(conn: &QuicConnection) -> Option<Instant> {
    if matches!(conn.state, QuicConnState::Done | QuicConnState::Failed) {
        return None;
    }
    let publish = if conn.state == QuicConnState::Publishing
        && !conn.send_pending
        && !conn.has_pending_send()
    {
        conn.next_publish_at
    } else {
        None
    };
    let drain = if conn.state == QuicConnState::Draining {
        conn.drain_deadline
    } else {
        None
    };
    Some(conn.next_transport_tick)
        .into_iter()
        .chain(conn.engine.engine().next_tick_at())
        .chain(publish)
        .chain(drain)
        .min()
}

fn mark_failed(
    conn: &mut QuicConnection,
    stats: &BenchStats,
    done_count: &mut usize,
    error_kind: ErrorKind,
) {
    if conn.state != QuicConnState::Failed && conn.state != QuicConnState::Done {
        conn.state = QuicConnState::Failed;
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

fn submit_front_send(ring: &mut WorkerRing, key: usize, conn: &mut QuicConnection) {
    if conn.send_pending {
        return;
    }
    let datagram = match conn.front_send_datagram() {
        Some(d) => d,
        None => return,
    };
    let ptr = datagram.as_ptr();
    let len = datagram.len() as u32;

    let send_e = opcode::Send::new(Fd(conn.fd), ptr, len)
        .build()
        .user_data(encode_user_data(key, OP_SEND));

    conn.send_pending = true;
    unsafe {
        ring.push(send_e);
    }
}

fn submit_recv(ring: &mut WorkerRing, key: usize, conn: &mut QuicConnection) {
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
    fn quic_transport_ticks_publish_intervals_and_drain_are_scheduled() {
        let config = BenchConfig {
            keep_alive: 0,
            ..BenchConfig::default()
        };
        let mut conn = QuicConnection::new(-1, 0, &config, config.addr).unwrap();
        let now = Instant::now();
        conn.handle_tick(now);
        assert_eq!(next_deadline(&conn), Some(now + Duration::from_millis(10)));
        conn.state = QuicConnState::Publishing;
        conn.next_publish_at = Some(now + Duration::from_millis(2));
        assert_eq!(next_deadline(&conn), conn.next_publish_at);
        conn.send_pending = true;
        assert_eq!(next_deadline(&conn), Some(conn.next_transport_tick));
        conn.state = QuicConnState::Draining;
        conn.drain_deadline = Some(now);
        assert_eq!(next_deadline(&conn), Some(now));
        conn.state = QuicConnState::Failed;
        assert_eq!(next_deadline(&conn), None);
    }
}
