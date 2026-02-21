#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{
    PersistentLogConfig, TransportConnectionState, TransportError, TransportSecurityConfig,
    TransportServer,
};

/// Default number of requests that can be in-flight on a single connection
/// simultaneously.  Increasing this fills the TCP window more aggressively
/// and hides round-trip latency at the cost of slightly more memory per
/// connection.
const DEFAULT_MAX_IN_FLIGHT: usize = 16;

pub struct AsyncTransportServer {
    listener: TcpListener,
    state: Arc<Mutex<TransportServer>>,
    max_frame_size: Arc<AtomicUsize>,
    /// Maximum number of requests pipelined on each connection.
    max_in_flight: usize,
}

impl AsyncTransportServer {
    pub async fn bind<A, P>(
        addr: A,
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<Self, TransportError>
    where
        A: ToSocketAddrs,
        P: AsRef<Path>,
    {
        Self::bind_with_security(
            addr,
            data_dir,
            log_config,
            TransportSecurityConfig::default(),
        )
        .await
    }

    pub async fn bind_with_security<A, P>(
        addr: A,
        data_dir: P,
        log_config: PersistentLogConfig,
        security: TransportSecurityConfig,
    ) -> Result<Self, TransportError>
    where
        A: ToSocketAddrs,
        P: AsRef<Path>,
    {
        let data_dir = data_dir.as_ref().to_path_buf();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| TransportError::Io {
                operation: "bind_async",
                message: err.to_string(),
            })?;
        let state =
            TransportServer::bind_with_security("127.0.0.1:0", &data_dir, log_config, security)?;
        let max_frame_size = state.max_frame_size();
        Ok(Self {
            listener,
            state: Arc::new(Mutex::new(state)),
            max_frame_size: Arc::new(AtomicUsize::new(max_frame_size)),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        })
    }

    /// Override the maximum number of pipelined in-flight requests per connection.
    pub fn set_max_in_flight(&mut self, max_in_flight: usize) {
        self.max_in_flight = max_in_flight.max(1);
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.listener
            .local_addr()
            .map_err(|err| TransportError::Io {
                operation: "local_addr_async",
                message: err.to_string(),
            })
    }

    pub async fn metrics_prometheus(&self) -> Result<String, TransportError> {
        let state = self.state.lock().map_err(|_| TransportError::Io {
            operation: "lock_transport_state",
            message: "transport state lock poisoned".to_string(),
        })?;
        state.metrics_prometheus()
    }

    pub async fn set_max_frame_size(&self, max_frame_size: usize) -> Result<(), TransportError> {
        self.max_frame_size.store(max_frame_size, Ordering::Relaxed);
        let mut state = self.state.lock().map_err(|_| TransportError::Io {
            operation: "lock_transport_state",
            message: "transport state lock poisoned".to_string(),
        })?;
        state.set_max_frame_size(max_frame_size);
        Ok(())
    }

    pub async fn serve_one_connection(&self) -> Result<(), TransportError> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .map_err(|err| TransportError::Io {
                operation: "accept_async",
                message: err.to_string(),
            })?;
        handle_connection(
            stream,
            self.state.clone(),
            self.max_frame_size.clone(),
            self.max_in_flight,
        )
        .await
    }

    pub async fn serve_n_connections_concurrent(&self, count: usize) -> Result<(), TransportError> {
        let mut handles: Vec<JoinHandle<Result<(), TransportError>>> = Vec::with_capacity(count);
        for _ in 0..count {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|err| TransportError::Io {
                    operation: "accept_async",
                    message: err.to_string(),
                })?;
            let state = self.state.clone();
            let max_frame_size = self.max_frame_size.clone();
            let max_in_flight = self.max_in_flight;
            handles.push(tokio::spawn(async move {
                handle_connection(stream, state, max_frame_size, max_in_flight).await
            }));
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => {
                    return Err(TransportError::Io {
                        operation: "join_async_connection",
                        message: err.to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

// ── Pipelined connection handler ──────────────────────────────────────────────
//
// Three concurrent tasks connected by bounded mpsc channels:
//
//   TCP socket ──► reader_task ──[frame_tx]──► process_task ──[resp_tx]──► writer_task ──► TCP socket
//
// reader_task   : reads [4-byte len][frame] pairs, emits (seq_no, frame).
//                 Back-pressures naturally when frame_tx is full.
// process_task  : acquires Arc<Mutex<TransportServer>>, calls
//                 process_frame_for_connection(), emits (seq_no, response).
//                 Connection security state (SASL) is mutated here in arrival
//                 order, which is safe because frames are received sequentially.
// writer_task   : buffers responses in a BTreeMap keyed by seq_no, then emits
//                 them strictly in arrival order (Kafka clients match responses
//                 by position, not just correlation_id).
//
// Error propagation: when any task closes its sending channel, downstream
// tasks drain remaining items and exit cleanly.

async fn handle_connection(
    stream: TcpStream,
    state: Arc<Mutex<TransportServer>>,
    max_frame_size: Arc<AtomicUsize>,
    max_in_flight: usize,
) -> Result<(), TransportError> {
    // Initialise per-connection security state while holding the server lock.
    let connection_state = {
        let server = state.lock().map_err(|_| TransportError::Io {
            operation: "lock_transport_state",
            message: "transport state lock poisoned".to_string(),
        })?;
        server.new_connection_state()
    };

    let (read_half, write_half) = stream.into_split();

    // reader → processor channel (capacity = max_in_flight for back-pressure)
    let (frame_tx, frame_rx) =
        mpsc::channel::<(u64, Vec<u8>)>(max_in_flight);
    // processor → writer channel (same capacity)
    let (resp_tx, resp_rx) =
        mpsc::channel::<(u64, Vec<u8>)>(max_in_flight);

    let reader = tokio::spawn(reader_task(read_half, frame_tx, max_frame_size));
    let processor = tokio::spawn(process_task(
        frame_rx,
        resp_tx,
        state,
        connection_state,
    ));
    let writer = tokio::spawn(writer_task(write_half, resp_rx));

    // Collect results.  If reader or processor close their channels (EOF or
    // error), the downstream tasks drain and exit on their own.  We join all
    // three and surface the first non-OK error.
    let (r_reader, r_processor, r_writer) =
        tokio::join!(reader, processor, writer);

    for join_result in [r_reader, r_processor, r_writer] {
        match join_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => {
                return Err(TransportError::Io {
                    operation: "join_pipeline_task",
                    message: join_err.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Reads frames from the TCP read half and forwards them with a monotonically
/// increasing sequence number.  Exits cleanly on EOF or connection reset.
async fn reader_task(
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    frame_tx: mpsc::Sender<(u64, Vec<u8>)>,
    max_frame_size: Arc<AtomicUsize>,
) -> Result<(), TransportError> {
    let mut seq_no: u64 = 0;
    loop {
        let mut len_buf = [0_u8; 4];
        match read_half.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::UnexpectedEof
                        | ErrorKind::ConnectionReset
                        | ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionAborted
                ) =>
            {
                // Clean EOF — stop reading; processor/writer will drain naturally.
                return Ok(());
            }
            Err(err) => {
                return Err(TransportError::Io {
                    operation: "reader_read_len",
                    message: err.to_string(),
                });
            }
        }

        let frame_size_i32 = i32::from_be_bytes(len_buf);
        let frame_size =
            validate_frame_size_with_limit(frame_size_i32, max_frame_size.load(Ordering::Relaxed))?;

        let mut frame = vec![0_u8; frame_size];
        read_half
            .read_exact(&mut frame)
            .await
            .map_err(|err| TransportError::Io {
                operation: "reader_read_frame",
                message: err.to_string(),
            })?;

        // If the processor has exited (channel closed), stop reading.
        if frame_tx.send((seq_no, frame)).await.is_err() {
            return Ok(());
        }
        seq_no += 1;
    }
}

/// Receives frames from the reader, acquires the server mutex, processes each
/// frame, and forwards the response.  Connection security state (SASL) is
/// owned here because SASL handshake/authenticate mutate it and must run in
/// arrival order.
async fn process_task(
    mut frame_rx: mpsc::Receiver<(u64, Vec<u8>)>,
    resp_tx: mpsc::Sender<(u64, Vec<u8>)>,
    state: Arc<Mutex<TransportServer>>,
    mut connection_state: TransportConnectionState,
) -> Result<(), TransportError> {
    while let Some((seq_no, frame)) = frame_rx.recv().await {
        let state_clone = state.clone();
        // Move connection_state into the blocking closure; get it back out.
        let conn_state_in = connection_state;
        let (response, conn_state_out) =
            tokio::task::spawn_blocking(move || {
                dispatch_frame_blocking(state_clone, frame, conn_state_in)
            })
            .await
            .map_err(|err| TransportError::Io {
                operation: "spawn_blocking_dispatch",
                message: err.to_string(),
            })??;
        connection_state = conn_state_out;

        if resp_tx.send((seq_no, response)).await.is_err() {
            // Writer has gone away; tear down the connection.
            return Ok(());
        }
    }
    Ok(())
}

/// Receives (seq_no, response) pairs from the processor and emits them to the
/// TCP write half in strict arrival order.  A BTreeMap buffers any
/// out-of-order responses so that a future parallel processor can be dropped
/// in without changing this task.
async fn writer_task(
    write_half: tokio::net::tcp::OwnedWriteHalf,
    mut resp_rx: mpsc::Receiver<(u64, Vec<u8>)>,
) -> Result<(), TransportError> {
    // BufWriter batches small writes into fewer syscalls.
    let mut writer = BufWriter::with_capacity(256 * 1024, write_half);
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut next_expected: u64 = 0;

    while let Some((seq_no, response)) = resp_rx.recv().await {
        pending.insert(seq_no, response);

        // Drain all in-order responses.
        while let Some(resp) = pending.remove(&next_expected) {
            writer
                .write_all(&resp)
                .await
                .map_err(|err| TransportError::Io {
                    operation: "writer_write_all",
                    message: err.to_string(),
                })?;
            next_expected += 1;
        }

        // Try to batch more without blocking (drain whatever is already queued).
        loop {
            match resp_rx.try_recv() {
                Ok((sn, r)) => {
                    pending.insert(sn, r);
                    while let Some(resp) = pending.remove(&next_expected) {
                        writer
                            .write_all(&resp)
                            .await
                            .map_err(|err| TransportError::Io {
                                operation: "writer_write_all",
                                message: err.to_string(),
                            })?;
                        next_expected += 1;
                    }
                }
                Err(_) => break,
            }
        }

        // Flush once after the batch.
        writer
            .flush()
            .await
            .map_err(|err| TransportError::Io {
                operation: "writer_flush",
                message: err.to_string(),
            })?;
    }
    Ok(())
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn dispatch_frame_blocking(
    state: Arc<Mutex<TransportServer>>,
    frame: Vec<u8>,
    mut connection_state: TransportConnectionState,
) -> Result<(Vec<u8>, TransportConnectionState), TransportError> {
    let mut server = state.lock().map_err(|_| TransportError::Io {
        operation: "lock_transport_state",
        message: "transport state lock poisoned".to_string(),
    })?;
    let response = server.process_frame_for_connection(&frame, &mut connection_state)?;
    Ok((response, connection_state))
}

fn validate_frame_size_with_limit(
    frame_size_i32: i32,
    max_frame_size: usize,
) -> Result<usize, TransportError> {
    if frame_size_i32 < 0 {
        return Err(TransportError::InvalidFrameSize(frame_size_i32));
    }
    let frame_size = usize::try_from(frame_size_i32)
        .map_err(|_| TransportError::InvalidFrameSize(frame_size_i32))?;
    if frame_size > max_frame_size {
        return Err(TransportError::FrameTooLarge {
            size: frame_size,
            max_size: max_frame_size,
        });
    }
    Ok(frame_size)
}

// ── Phase 4: AsyncTransportServerSharded ─────────────────────────────────────
//
// Replaces `Arc<Mutex<TransportServer>>` (one global lock for all connections)
// with `Arc<BrokerSharedState>` (per-partition async mutexes for Produce/Fetch,
// separate coordinator mutexes for group/offset/tx operations).
//
// The pipelining architecture from Phase 1 is reused unchanged — we only swap
// out the state type and route hot-path operations through the sharded broker.

use crate::{BrokerSharedState, TransportSharedError};

/// API key constants (re-declared locally to keep this module self-contained).
const API_KEY_SHARDED_PRODUCE: i16 = 0;

/// A TCP server that accepts Kafka-protocol connections and dispatches frames
/// with per-partition concurrency.
///
/// - **Produce / Fetch**: uses per-partition `tokio::sync::Mutex` via
///   `PartitionedBrokerSharded`.  Concurrent connections writing to different
///   partitions do not block each other.
/// - **All other API keys** (coordinators, SASL, metadata): delegate to a
///   `Arc<Mutex<TransportServer>>` so that existing, fully-tested handler logic
///   is reused without duplication.  These paths are not on the hot path for
///   throughput benchmarks.
pub struct AsyncTransportServerSharded {
    listener: TcpListener,
    /// Per-partition async state — hot path.
    sharded: Arc<BrokerSharedState>,
    /// Global-mutex fallback for coordinator / SASL / metadata handlers.
    fallback: Arc<Mutex<TransportServer>>,
    max_in_flight: usize,
}

impl AsyncTransportServerSharded {
    pub async fn bind<A, P>(
        addr: A,
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<Self, TransportSharedError>
    where
        A: ToSocketAddrs,
        P: AsRef<Path>,
    {
        Self::bind_with_security(
            addr,
            data_dir,
            log_config,
            TransportSecurityConfig::default(),
        )
        .await
    }

    pub async fn bind_with_security<A, P>(
        addr: A,
        data_dir: P,
        log_config: PersistentLogConfig,
        security: TransportSecurityConfig,
    ) -> Result<Self, TransportSharedError>
    where
        A: ToSocketAddrs,
        P: AsRef<Path>,
    {
        let data_dir = data_dir.as_ref().to_path_buf();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| TransportSharedError::Setup(e.to_string()))?;

        let sharded = BrokerSharedState::open_with_security(
            &data_dir,
            log_config.clone(),
            security.clone(),
        )
        .await?;

        // Build the fallback TransportServer synchronously on a blocking thread.
        let fallback_state = {
            let d = data_dir.clone();
            let cfg = log_config.clone();
            let sec = security.clone();
            tokio::task::spawn_blocking(move || {
                TransportServer::bind_with_security("127.0.0.1:0", &d, cfg, sec)
            })
            .await
            .map_err(|e| TransportSharedError::Setup(e.to_string()))?
            .map_err(|e| TransportSharedError::Setup(format!("{e:?}")))?
        };

        Ok(Self {
            listener,
            sharded: Arc::new(sharded),
            fallback: Arc::new(Mutex::new(fallback_state)),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, TransportError> {
        self.listener.local_addr().map_err(|e| TransportError::Io {
            operation: "sharded_local_addr",
            message: e.to_string(),
        })
    }

    pub fn set_max_in_flight(&mut self, max_in_flight: usize) {
        self.max_in_flight = max_in_flight.max(1);
    }

    pub async fn serve_one_connection(&self) -> Result<(), TransportError> {
        let (stream, _) = self.listener.accept().await.map_err(|e| TransportError::Io {
            operation: "sharded_accept",
            message: e.to_string(),
        })?;
        handle_connection_sharded(
            stream,
            self.sharded.clone(),
            self.fallback.clone(),
            self.max_in_flight,
        )
        .await
    }

    pub async fn serve_n_connections_concurrent(&self, count: usize) -> Result<(), TransportError> {
        let mut handles: Vec<JoinHandle<Result<(), TransportError>>> = Vec::with_capacity(count);
        for _ in 0..count {
            let (stream, _) =
                self.listener.accept().await.map_err(|e| TransportError::Io {
                    operation: "sharded_accept",
                    message: e.to_string(),
                })?;
            let sharded = self.sharded.clone();
            let fallback = self.fallback.clone();
            let max_in_flight = self.max_in_flight;
            handles.push(tokio::spawn(async move {
                handle_connection_sharded(stream, sharded, fallback, max_in_flight).await
            }));
        }
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(TransportError::Io {
                        operation: "sharded_join",
                        message: e.to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Pipelined connection handler for the sharded server.
///
/// Same three-task pipeline as Phase 1 (`handle_connection`), but the
/// `process_task` uses `dispatch_frame_sharded_blocking` which acquires only
/// the per-partition mutex for Produce — everything else still uses the
/// global fallback mutex.
async fn handle_connection_sharded(
    stream: TcpStream,
    sharded: Arc<BrokerSharedState>,
    fallback: Arc<Mutex<TransportServer>>,
    max_in_flight: usize,
) -> Result<(), TransportError> {
    let connection_state = sharded.new_connection_state();
    let group_commit_tx = sharded.group_commit_tx.clone();

    let (read_half, write_half) = stream.into_split();
    let max_frame_size = sharded.max_frame_size.clone();

    let (frame_tx, frame_rx) = mpsc::channel::<(u64, Vec<u8>)>(max_in_flight);
    let (resp_tx, resp_rx) = mpsc::channel::<(u64, Vec<u8>)>(max_in_flight);

    let reader = tokio::spawn(reader_task(read_half, frame_tx, max_frame_size));
    let processor = tokio::spawn(process_task_sharded(
        frame_rx,
        resp_tx,
        sharded,
        fallback,
        connection_state,
        group_commit_tx,
    ));
    let writer = tokio::spawn(writer_task(write_half, resp_rx));

    let (r_reader, r_processor, r_writer) = tokio::join!(reader, processor, writer);
    for join_result in [r_reader, r_processor, r_writer] {
        match join_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => {
                return Err(TransportError::Io {
                    operation: "sharded_join_pipeline_task",
                    message: join_err.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Like `process_task` but uses per-partition async locks for Produce and
/// falls back to the global mutex for all other API keys.
///
/// The hot path (Produce, API key 0) peeks at the first two bytes of the
/// Produce hot-path: decode → submit to group-commit queue → await flush →
/// encode response.  All other API keys fall through to the global-mutex
/// fallback.  The group-commit queue batches records from *all* concurrent
/// connections into a single `append_batch` + `sync_data` per partition,
/// delivering the same throughput multiplier as Kafka's pgroup-commit layer.
async fn process_task_sharded(
    mut frame_rx: mpsc::Receiver<(u64, Vec<u8>)>,
    resp_tx: mpsc::Sender<(u64, Vec<u8>)>,
    sharded: Arc<BrokerSharedState>,
    fallback: Arc<Mutex<TransportServer>>,
    mut connection_state: TransportConnectionState,
    group_commit_tx: crate::GroupCommitSender,
) -> Result<(), TransportError> {
    use rafka_protocol::messages::{ProduceRequest, VersionedCodec};

    while let Some((seq_no, frame)) = frame_rx.recv().await {
        let api_key = if frame.len() >= 2 {
            i16::from_be_bytes([frame[0], frame[1]])
        } else {
            -1
        };

        let (response, new_conn_state) = if api_key == API_KEY_SHARDED_PRODUCE {
            // ── Priority 2: group-commit produce path ────────────────────────
            // 1. Parse frame header synchronously (no I/O).
            let (api_version, correlation_id, body) =
                match parse_produce_frame_header(&frame) {
                    Ok(v) => v,
                    Err(e) => return Err(e),
                };
            sharded.metrics.record_api_request(api_key, api_version);

            // 2. Decode the ProduceRequest.
            let decoded = match ProduceRequest::decode(api_version, body) {
                Ok((req, _)) => req,
                Err(e) => return Err(TransportError::Protocol(e)),
            };

            let timestamp_ms = {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0u128, |d| d.as_millis());
                i64::try_from(now).unwrap_or(i64::MAX)
            };

            // 3. For each topic-partition, submit a GroupCommitEntry and
            //    collect the oneshot receiver so we can await the result.
            struct Submission {
                topic_name: Option<String>,
                topic_id: Option<[u8; 16]>,
                partition_index: i32,
                rx: tokio::sync::oneshot::Receiver<Result<Vec<i64>, String>>,
            }
            let mut submissions: Vec<Submission> = Vec::new();

            for topic in decoded.topic_data {
                let topic_name = topic.name.clone();
                for partition in topic.partition_data {
                    let tp_name = topic_name.as_deref().unwrap_or("").to_string();
                    let tp = match crate::TopicPartition::new(&tp_name, partition.index) {
                        Ok(tp) => tp,
                        Err(_) => {
                            // Bad partition — push a placeholder that will
                            // produce an error response.
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            let _ = tx.send(Err("invalid topic/partition".to_string()));
                            submissions.push(Submission {
                                topic_name: topic_name.clone(),
                                topic_id: topic.topic_id,
                                partition_index: partition.index,
                                rx,
                            });
                            continue;
                        }
                    };

                    let payload = partition.records.unwrap_or_default();
                    let payload_len = payload.len();
                    // Priority 1: unpack multi-record batch if present.
                    let records: Vec<(Vec<u8>, Vec<u8>, i64)> =
                        crate::decode_records_batch(payload)
                            .into_iter()
                            .map(|(k, v)| (k, v, timestamp_ms))
                            .collect();
                    sharded.metrics.record_partition_event(
                        "produce", &tp_name, partition.index, payload_len,
                    );

                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let entry = crate::GroupCommitEntry {
                        tp,
                        records,
                        result_tx: tx,
                    };
                    if group_commit_tx.send(entry).await.is_err() {
                        return Err(TransportError::Io {
                            operation: "group_commit_send",
                            message: "flusher task has exited".to_string(),
                        });
                    }
                    submissions.push(Submission {
                        topic_name: topic_name.clone(),
                        topic_id: topic.topic_id,
                        partition_index: partition.index,
                        rx,
                    });
                }
            }

            // 4. Await all flush results (yields executor → other connections
            //    can submit their records into the same flusher batch).
            // Group into the shape encode_sharded_produce_response expects:
            // Vec<(Option<String>, Option<[u8;16]>, Vec<(i32, i16, i64, i64)>)>
            let mut by_topic: Vec<(Option<String>, Option<[u8; 16]>, Vec<(i32, i16, i64, i64)>)> =
                Vec::new();
            for sub in submissions {
                let (error_code, base_offset, log_start_offset) =
                    match sub.rx.await {
                        Ok(Ok(offsets)) => {
                            let base = offsets.first().copied().unwrap_or(-1);
                            (0_i16, base, 0_i64)
                        }
                        Ok(Err(_)) | Err(_) => (-1_i16, -1_i64, -1_i64),
                    };
                // Find or insert the topic entry.
                let pos = by_topic.iter().position(|(n, id, _)| {
                    n == &sub.topic_name && id == &sub.topic_id
                });
                let entry = if let Some(i) = pos {
                    &mut by_topic[i]
                } else {
                    by_topic.push((sub.topic_name, sub.topic_id, Vec::new()));
                    by_topic.last_mut().unwrap()
                };
                entry.2.push((sub.partition_index, error_code, base_offset, log_start_offset));
            }

            // 5. Encode the response (no I/O — pure serialisation).
            let response_bytes = encode_sharded_produce_response(
                api_version, api_key, correlation_id, &by_topic,
            )?;
            sharded.metrics.record_api_response(api_key, api_version);
            (response_bytes, connection_state)
        } else {
            // Fallback path: global mutex handles coordinators / SASL / etc.
            let fallback_clone = fallback.clone();
            let conn_state_in = connection_state;
            tokio::task::spawn_blocking(move || {
                dispatch_frame_blocking(fallback_clone, frame, conn_state_in)
            })
            .await
            .map_err(|e| TransportError::Io {
                operation: "sharded_spawn_blocking_fallback",
                message: e.to_string(),
            })??
        };

        connection_state = new_conn_state;

        if resp_tx.send((seq_no, response)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Extract `(api_version, correlation_id, body_slice)` from a raw frame.
fn parse_produce_frame_header(frame: &[u8]) -> Result<(i16, i32, &[u8]), TransportError> {
    if frame.len() < 8 {
        return Err(TransportError::Truncated);
    }
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation_id = i32::from_be_bytes(frame[4..8].try_into().unwrap());
    let mut cursor = 8usize;
    // Skip client_id: [2-byte length][bytes]
    if frame.len() < cursor + 2 {
        return Err(TransportError::Truncated);
    }
    let client_id_len = i16::from_be_bytes([frame[cursor], frame[cursor + 1]]) as i32;
    cursor += 2;
    if client_id_len > 0 {
        cursor += client_id_len as usize;
    }
    // Flexible header (api_version ≥ 9): skip tagged-fields varint (always 0x00 here).
    if api_version >= 9 && cursor < frame.len() {
        cursor += 1;
    }
    if cursor > frame.len() {
        return Err(TransportError::Truncated);
    }
    Ok((api_version, correlation_id, &frame[cursor..]))
}

/// Dispatch a Produce frame using per-partition blocking locks from
/// `PartitionedBrokerSharded`.
///
/// This function runs inside `spawn_blocking`, so it may call
/// `Mutex::blocking_lock()` without stalling the async runtime.
///
/// **Concurrency model**: while this call holds the lock for partition A, a
/// concurrent `spawn_blocking` task for partition B acquires a *different*
/// `Arc<tokio::sync::Mutex<PersistentSegmentLog>>` and runs in parallel on
/// another OS thread — completely unblocked.
#[allow(dead_code)]
fn dispatch_frame_sharded_blocking(
    sharded: Arc<BrokerSharedState>,
    frame: Vec<u8>,
    connection_state: TransportConnectionState,
) -> Result<(Vec<u8>, TransportConnectionState), TransportError> {
    use rafka_protocol::messages::{ProduceRequest, VersionedCodec};

    // ── 1. Parse frame header (fast, no I/O) ─────────────────────────────────
    let api_key = if frame.len() >= 2 {
        i16::from_be_bytes([frame[0], frame[1]])
    } else {
        return Err(TransportError::Truncated);
    };
    let api_version = if frame.len() >= 4 {
        i16::from_be_bytes([frame[2], frame[3]])
    } else {
        return Err(TransportError::Truncated);
    };
    // correlation_id is at [4..8], client_id follows; skip via a minimal parse.
    let mut cursor: usize = 4;
    // correlation_id (4 bytes)
    if frame.len() < cursor + 4 { return Err(TransportError::Truncated); }
    let correlation_id = i32::from_be_bytes(frame[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    // client_id: 2-byte length + bytes (legacy non-flexible header, version < 9)
    if frame.len() < cursor + 2 { return Err(TransportError::Truncated); }
    let client_id_len = i16::from_be_bytes([frame[cursor], frame[cursor + 1]]) as i32;
    cursor += 2;
    if client_id_len > 0 {
        cursor += client_id_len as usize;
    }
    if api_key == 0 && api_version >= 9 {
        // Flexible header: skip tagged fields varint after client_id.
        if cursor < frame.len() { cursor += 1; } // 0 tagged fields = 0x00
    }
    let body = &frame[cursor..];

    // ── 2. Decode Produce request ─────────────────────────────────────────────
    let (decoded, _read) = ProduceRequest::decode(api_version, body)
        .map_err(TransportError::Protocol)?;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |d| d.as_millis());
    let timestamp_ms = i64::try_from(now_ms).unwrap_or(i64::MAX);

    // ── 3. Per-partition produce using sharded blocking locks ─────────────────
    // Lock ordering: acquire shard_map (read) → per-partition mutex.
    // For new partitions we upgrade to a write lock, then downgrade back.

    let mut topic_results: Vec<(Option<String>, Option<[u8; 16]>, Vec<(i32, i16, i64, i64)>)> =
        Vec::with_capacity(decoded.topic_data.len());

    for topic in decoded.topic_data {
        let topic_name = match &topic.name {
            Some(n) => n.clone(),
            None => {
                topic_results.push((topic.name, topic.topic_id, Vec::new()));
                continue;
            }
        };

        let mut partition_results: Vec<(i32, i16, i64, i64)> = Vec::new();

        for partition in topic.partition_data {
            let tp = crate::TopicPartition {
                topic: topic_name.clone(),
                partition: partition.index,
            };
            let payload = partition.records.unwrap_or_default();

            // Resolve the partition lock — get_or_create under read lock,
            // upgrading to write lock only for new partitions.
            let partition_lock: crate::PartitionLock = {
                let map = sharded.broker.shard_map.blocking_read();
                if let Some(lock) = map.get(&tp) {
                    Arc::clone(lock)
                } else {
                    drop(map);
                    // Create the partition log on the first produce.
                    let dir = sharded.broker.data_dir.join(
                        format!("{}-{}", topic_name, partition.index)
                    );
                    let config = sharded.broker.log_config.clone();
                    let new_log = match crate::PersistentSegmentLog::open(dir, config) {
                        Ok(log) => log,
                        Err(_e) => {
                            partition_results.push((partition.index, -1_i16, -1, -1));
                            continue;
                        }
                    };
                    let new_lock = Arc::new(tokio::sync::Mutex::new(new_log));
                    let mut map = sharded.broker.shard_map.blocking_write();
                    Arc::clone(map.entry(tp).or_insert(new_lock))
                }
            };

            let mut log = partition_lock.blocking_lock();
            match log.append(Vec::new(), payload, timestamp_ms) {
                Ok(offset) => {
                    sharded.metrics.record_partition_event(
                        "produce",
                        &topic_name,
                        partition.index,
                        0,
                    );
                    partition_results.push((partition.index, 0_i16, offset, 0));
                }
                Err(_) => {
                    partition_results.push((partition.index, -1_i16, -1, -1));
                }
            }
        }

        topic_results.push((topic.name, topic.topic_id, partition_results));
    }

    // ── 4. Encode response ────────────────────────────────────────────────────
    let response_bytes = encode_sharded_produce_response(
        api_version,
        api_key,
        correlation_id,
        &topic_results,
    )?;

    Ok((response_bytes, connection_state))
}

fn encode_sharded_produce_response(
    api_version: i16,
    api_key: i16,
    correlation_id: i32,
    topic_results: &[(Option<String>, Option<[u8; 16]>, Vec<(i32, i16, i64, i64)>)],
) -> Result<Vec<u8>, TransportError> {
    use crate::transport::{
        encode_produce_response, encode_response_frame, response_header_version,
        ProducePartitionResponse, ProduceTopicResponse,
    };

    let topics: Vec<ProduceTopicResponse> = topic_results
        .iter()
        .map(|(name, topic_id, partitions)| ProduceTopicResponse {
            name: name.clone(),
            topic_id: *topic_id,
            partitions: partitions
                .iter()
                .map(|(index, error_code, base_offset, log_start_offset)| {
                    ProducePartitionResponse {
                        index: *index,
                        error_code: *error_code,
                        base_offset: *base_offset,
                        log_start_offset: *log_start_offset,
                    }
                })
                .collect(),
        })
        .collect();

    let body = encode_produce_response(api_version, &topics, 0)?;
    let header_version = response_header_version(api_key, api_version)?;
    encode_response_frame(correlation_id, header_version, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_and_metrics_are_available() {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_millis();
        let data_dir = std::env::temp_dir().join(format!(
            "rafka-async-transport-unit-{}-{millis}",
            std::process::id()
        ));
        let server = AsyncTransportServer::bind(
            "127.0.0.1:0",
            &data_dir,
            PersistentLogConfig {
                base_offset: 0,
                segment_max_records: 8,
                sync_on_append: false,
            },
        )
        .await
        .expect("bind async server");
        let _ = server.local_addr().expect("local addr");
        let _metrics = server.metrics_prometheus().await.expect("metrics");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn set_max_frame_size_updates_async_guard() {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_millis();
        let data_dir = std::env::temp_dir().join(format!(
            "rafka-async-transport-limit-{}-{millis}",
            std::process::id()
        ));
        let server = AsyncTransportServer::bind(
            "127.0.0.1:0",
            &data_dir,
            PersistentLogConfig {
                base_offset: 0,
                segment_max_records: 8,
                sync_on_append: false,
            },
        )
        .await
        .expect("bind async server");
        server
            .set_max_frame_size(16)
            .await
            .expect("set max frame size");
        assert_eq!(server.max_frame_size.load(Ordering::Relaxed), 16);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
