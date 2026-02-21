#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, ApiVersionsResponseApiVersion, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    JoinGroupResponseJoinGroupResponseMember, LeaveGroupRequest, LeaveGroupResponse,
    LeaveGroupResponseMemberResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetCommitResponseOffsetCommitResponsePartition,
    OffsetCommitResponseOffsetCommitResponseTopic, OffsetFetchRequest, OffsetFetchResponse,
    OffsetFetchResponseOffsetFetchResponseGroup, OffsetFetchResponseOffsetFetchResponsePartition,
    OffsetFetchResponseOffsetFetchResponsePartitions, OffsetFetchResponseOffsetFetchResponseTopic,
    OffsetFetchResponseOffsetFetchResponseTopics, ProduceRequest, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, SyncGroupRequest,
    SyncGroupResponse, VersionedCodec, API_VERSIONS_MAX_VERSION, API_VERSIONS_MIN_VERSION,
    OFFSET_COMMIT_MAX_VERSION, OFFSET_COMMIT_MIN_VERSION, OFFSET_FETCH_MAX_VERSION,
    OFFSET_FETCH_MIN_VERSION, PRODUCE_MAX_VERSION, PRODUCE_MIN_VERSION,
    SASL_AUTHENTICATE_MAX_VERSION, SASL_AUTHENTICATE_MIN_VERSION, SASL_HANDSHAKE_MAX_VERSION,
    SASL_HANDSHAKE_MIN_VERSION,
};
use rafka_protocol::ProtocolError;
use rustls::{ServerConfig as RustlsServerConfig, ServerConnection, StreamOwned};

use crate::{
    authorization::{
        AclAuthorizer, AclOperation, AclResourceType, ANONYMOUS_PRINCIPAL, CLUSTER_RESOURCE_NAME,
    },
    metrics::TransportMetrics,
    security::{authenticate_sasl, begin_sasl_handshake, SaslConfig, SaslConnectionState},
    ClassicGroupCoordinator, ClassicGroupCoordinatorError, ClassicGroupStoreConfig, EndTxnInput,
    HeartbeatInput, InitProducerInput, JoinGroupInput, JoinProtocol, LeaveGroupMemberInput,
    OffsetCommitInput, OffsetCoordinator, OffsetCoordinatorError, OffsetStoreConfig, OffsetTopic,
    PartitionedBroker, PartitionedBrokerError, PendingWriteBatch, PersistentLogConfig,
    StorageError, SyncAssignment, SyncGroupInput, TransactionCoordinator,
    TransactionCoordinatorError, TransactionStoreConfig, WriteTxnMarkerInput,
    WriteTxnMarkerTopicInput,
};

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_FETCH: i16 = 1;
const API_KEY_OFFSET_COMMIT: i16 = 8;
const API_KEY_OFFSET_FETCH: i16 = 9;
const API_KEY_JOIN_GROUP: i16 = 11;
const API_KEY_HEARTBEAT: i16 = 12;
const API_KEY_LEAVE_GROUP: i16 = 13;
const API_KEY_SYNC_GROUP: i16 = 14;
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_INIT_PRODUCER_ID: i16 = 22;
const API_KEY_END_TXN: i16 = 26;
const API_KEY_WRITE_TXN_MARKERS: i16 = 27;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

const API_VERSIONS_RESPONSE_HEADER_VERSION: i16 = 0;
const DEFAULT_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

pub(crate) const ERROR_NONE: i16 = 0;
const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
pub(crate) const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
pub(crate) const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERROR_GROUP_AUTHORIZATION_FAILED: i16 = 30;
const ERROR_CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;
const ERROR_INVALID_REQUEST: i16 = 42;
const ERROR_ILLEGAL_GENERATION: i16 = 22;
const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const ERROR_INVALID_GROUP_ID: i16 = 24;
const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;
const ERROR_MEMBER_ID_REQUIRED: i16 = 79;
const ERROR_FENCED_INSTANCE_ID: i16 = 82;
const ERROR_INVALID_PRODUCER_EPOCH: i16 = 47;
const ERROR_INVALID_TXN_STATE: i16 = 48;
const ERROR_CONCURRENT_TRANSACTIONS: i16 = 51;
const ERROR_UNKNOWN_PRODUCER_ID: i16 = 59;
const ERROR_PRODUCER_FENCED: i16 = 90;
const ERROR_STALE_MEMBER_EPOCH: i16 = 113;
const ERROR_UNKNOWN_TOPIC_ID: i16 = 100;
const ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

const PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION: i16 = 0;
pub(crate) const FETCH_MIN_VERSION: i16 = 4;
pub(crate) const FETCH_MAX_VERSION: i16 = 18;
const JOIN_GROUP_MIN_VERSION: i16 = 0;
const JOIN_GROUP_MAX_VERSION: i16 = 9;
const HEARTBEAT_MIN_VERSION: i16 = 0;
const HEARTBEAT_MAX_VERSION: i16 = 4;
const LEAVE_GROUP_MIN_VERSION: i16 = 0;
const LEAVE_GROUP_MAX_VERSION: i16 = 5;
const SYNC_GROUP_MIN_VERSION: i16 = 0;
const SYNC_GROUP_MAX_VERSION: i16 = 5;
const INIT_PRODUCER_ID_MIN_VERSION: i16 = 0;
const INIT_PRODUCER_ID_MAX_VERSION: i16 = 6;
const END_TXN_MIN_VERSION: i16 = 0;
const END_TXN_MAX_VERSION: i16 = 5;
const WRITE_TXN_MARKERS_MIN_VERSION: i16 = 1;
const WRITE_TXN_MARKERS_MAX_VERSION: i16 = 2;

const SASL_HANDSHAKE_RESPONSE_HEADER_VERSION: i16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Io {
        operation: &'static str,
        message: String,
    },
    FrameTooLarge {
        size: usize,
        max_size: usize,
    },
    InvalidFrameSize(i32),
    Truncated,
    InvalidHeader(&'static str),
    InvalidUtf8,
    UnsupportedApiKey(i16),
    UnsupportedApiVersion {
        api_key: i16,
        api_version: i16,
    },
    Protocol(ProtocolError),
    Broker(PartitionedBrokerError),
    Security(String),
    Metrics(String),
    Tls(String),
}

impl TransportError {
    fn io(operation: &'static str, err: std::io::Error) -> Self {
        Self::Io {
            operation,
            message: err.to_string(),
        }
    }
}

impl From<ProtocolError> for TransportError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<PartitionedBrokerError> for TransportError {
    fn from(value: PartitionedBrokerError) -> Self {
        Self::Broker(value)
    }
}

#[derive(Clone, Default)]
pub struct TransportSecurityConfig {
    pub tls_server_config: Option<Arc<RustlsServerConfig>>,
    pub sasl: Option<SaslConfig>,
    pub acl: Option<AclAuthorizer>,
}

pub struct TransportServer {
    listener: TcpListener,
    broker: PartitionedBroker,
    offsets: OffsetCoordinator,
    groups: ClassicGroupCoordinator,
    transactions: TransactionCoordinator,
    max_frame_size: usize,
    security: TransportSecurityConfig,
    metrics: TransportMetrics,
}

#[derive(Debug, Clone)]
enum ConnectionSecurityState {
    Disabled,
    Sasl(SaslConnectionState),
}

#[derive(Debug, Clone)]
pub struct TransportConnectionState {
    security: ConnectionSecurityState,
}

impl TransportConnectionState {
    /// Create a new connection state appropriate for the given security config.
    pub fn new_for_security(has_sasl: bool) -> Self {
        let security = if has_sasl {
            ConnectionSecurityState::Sasl(SaslConnectionState::new())
        } else {
            ConnectionSecurityState::Disabled
        };
        Self { security }
    }
}

impl TransportServer {
    pub fn bind<A, P>(
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
    }

    pub fn bind_with_security<A, P>(
        addr: A,
        data_dir: P,
        log_config: PersistentLogConfig,
        security: TransportSecurityConfig,
    ) -> Result<Self, TransportError>
    where
        A: ToSocketAddrs,
        P: AsRef<Path>,
    {
        let listener = TcpListener::bind(addr).map_err(|err| TransportError::io("bind", err))?;
        let data_dir = data_dir.as_ref().to_path_buf();
        let broker = PartitionedBroker::open(&data_dir, log_config)?;
        let offsets = OffsetCoordinator::open(
            data_dir.join("__consumer_offsets"),
            OffsetStoreConfig::default(),
        )
        .map_err(map_offset_coordinator_error)?;
        let groups = ClassicGroupCoordinator::open(
            data_dir.join("__consumer_groups"),
            ClassicGroupStoreConfig::default(),
        )
        .map_err(map_classic_group_error)?;
        let transactions = TransactionCoordinator::open(
            data_dir.join("__transaction_state"),
            TransactionStoreConfig::default(),
        )
        .map_err(map_transaction_coordinator_error)?;
        let metrics = TransportMetrics::new().map_err(TransportError::Metrics)?;
        Ok(Self {
            listener,
            broker,
            offsets,
            groups,
            transactions,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            security,
            metrics,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.listener
            .local_addr()
            .map_err(|err| TransportError::io("local_addr", err))
    }

    pub fn set_max_frame_size(&mut self, max_frame_size: usize) {
        self.max_frame_size = max_frame_size;
    }

    pub fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    pub fn metrics_prometheus(&self) -> Result<String, TransportError> {
        self.metrics
            .render_prometheus()
            .map_err(TransportError::Metrics)
    }

    pub fn new_connection_state(&self) -> TransportConnectionState {
        let security = if self.security.sasl.is_some() {
            ConnectionSecurityState::Sasl(SaslConnectionState::new())
        } else {
            ConnectionSecurityState::Disabled
        };
        TransportConnectionState { security }
    }

    pub fn validate_frame_size(&self, frame_size_i32: i32) -> Result<usize, TransportError> {
        if frame_size_i32 < 0 {
            return Err(TransportError::InvalidFrameSize(frame_size_i32));
        }
        let frame_size = usize::try_from(frame_size_i32)
            .map_err(|_| TransportError::InvalidFrameSize(frame_size_i32))?;
        if frame_size > self.max_frame_size {
            return Err(TransportError::FrameTooLarge {
                size: frame_size,
                max_size: self.max_frame_size,
            });
        }
        Ok(frame_size)
    }

    fn authorize(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> bool {
        if let Some(acl) = &self.security.acl {
            acl.authorize(principal, operation, resource_type, resource_name)
        } else {
            true
        }
    }

    fn authorize_any(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
    ) -> bool {
        if let Some(acl) = &self.security.acl {
            acl.authorize_any(principal, operation, resource_type)
        } else {
            true
        }
    }

    pub fn process_frame_for_connection(
        &mut self,
        frame: &[u8],
        connection_state: &mut TransportConnectionState,
    ) -> Result<Vec<u8>, TransportError> {
        let request = parse_request_envelope(frame)?;
        let api_key = request.api_key;
        let api_version = request.api_version;
        self.metrics.record_api_request(api_key, api_version);

        let response = self.dispatch_request(request, &mut connection_state.security);
        match response {
            Ok(payload) => {
                self.metrics.record_api_response(api_key, api_version);
                Ok(payload)
            }
            Err(err) => {
                self.metrics
                    .record_api_error(api_key, api_version, transport_error_class(&err));
                Err(err)
            }
        }
    }

    pub fn serve_one_connection(&mut self) -> Result<(), TransportError> {
        let (stream, _) = self
            .listener
            .accept()
            .map_err(|err| TransportError::io("accept", err))?;
        // Disable Nagle's algorithm so that small responses (< MSS) are sent
        // immediately instead of being delayed waiting for ACKs.  This prevents
        // the classic Nagle + delayed-ACK interaction that adds ~40 ms of latency
        // to every request/response pair.
        stream
            .set_nodelay(true)
            .map_err(|err| TransportError::io("set_nodelay", err))?;
        if let Some(server_config) = self.security.tls_server_config.clone() {
            let connection = ServerConnection::new(server_config)
                .map_err(|err| TransportError::Tls(err.to_string()))?;
            let mut tls_stream = StreamOwned::new(connection, stream);
            while tls_stream.conn.is_handshaking() {
                if let Err(err) = tls_stream.conn.complete_io(&mut tls_stream.sock) {
                    self.metrics.record_tls_handshake("failure");
                    return Err(TransportError::Tls(err.to_string()));
                }
            }
            self.metrics.record_tls_handshake("success");
            self.handle_connection_io(&mut tls_stream)
        } else {
            let mut stream = stream;
            self.handle_connection_io(&mut stream)
        }
    }

    fn handle_connection_io<S: Read + Write>(
        &mut self,
        stream: &mut S,
    ) -> Result<(), TransportError> {
        let mut connection_state = self.new_connection_state();
        loop {
            let mut len_buf = [0_u8; 4];
            match stream.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err)
                    if matches!(
                        err.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::ConnectionReset
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(());
                }
                Err(err) => return Err(TransportError::io("read_exact", err)),
            }

            let frame_size_i32 = i32::from_be_bytes(len_buf);
            let frame_size = self.validate_frame_size(frame_size_i32)?;

            let mut frame = vec![0_u8; frame_size];
            stream
                .read_exact(&mut frame)
                .map_err(|err| TransportError::io("read_exact", err))?;
            let response = self.process_frame_for_connection(&frame, &mut connection_state)?;
            stream
                .write_all(&response)
                .map_err(|err| TransportError::io("write_all", err))?;
            stream
                .flush()
                .map_err(|err| TransportError::io("flush", err))?;
        }
    }

    fn dispatch_request(
        &mut self,
        request: RequestEnvelope<'_>,
        security_state: &mut ConnectionSecurityState,
    ) -> Result<Vec<u8>, TransportError> {
        if let ConnectionSecurityState::Sasl(state) = security_state {
            if !state.is_authenticated()
                && request.api_key != API_KEY_API_VERSIONS
                && request.api_key != API_KEY_SASL_HANDSHAKE
                && request.api_key != API_KEY_SASL_AUTHENTICATE
            {
                return Err(TransportError::Security(
                    "SASL authentication required before Kafka API dispatch".to_string(),
                ));
            }
        }
        let principal = connection_principal(security_state);
        match request.api_key {
            API_KEY_API_VERSIONS => self.handle_api_versions(request),
            API_KEY_PRODUCE => self.handle_produce(request, principal),
            API_KEY_FETCH => self.handle_fetch(request, principal),
            API_KEY_OFFSET_COMMIT => self.handle_offset_commit(request, principal),
            API_KEY_OFFSET_FETCH => self.handle_offset_fetch(request, principal),
            API_KEY_JOIN_GROUP => self.handle_join_group(request, principal),
            API_KEY_HEARTBEAT => self.handle_heartbeat(request, principal),
            API_KEY_LEAVE_GROUP => self.handle_leave_group(request, principal),
            API_KEY_SYNC_GROUP => self.handle_sync_group(request, principal),
            API_KEY_INIT_PRODUCER_ID => self.handle_init_producer_id(request, principal),
            API_KEY_END_TXN => self.handle_end_txn(request, principal),
            API_KEY_WRITE_TXN_MARKERS => self.handle_write_txn_markers(request, principal),
            API_KEY_SASL_HANDSHAKE => self.handle_sasl_handshake(request, security_state),
            API_KEY_SASL_AUTHENTICATE => self.handle_sasl_authenticate(request, security_state),
            _ => Err(TransportError::UnsupportedApiKey(request.api_key)),
        }
    }

    fn handle_api_versions(
        &mut self,
        request: RequestEnvelope<'_>,
    ) -> Result<Vec<u8>, TransportError> {
        let response_version = if (API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION)
            .contains(&request.api_version)
        {
            request.api_version
        } else {
            0
        };

        let mut error_code = ERROR_NONE;

        if !(API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION).contains(&request.api_version) {
            error_code = ERROR_UNSUPPORTED_VERSION;
        } else {
            match ApiVersionsRequest::decode(request.api_version, request.body) {
                Ok((_decoded, read)) if read == request.body.len() => {}
                Ok(_) => {
                    error_code = ERROR_INVALID_REQUEST;
                }
                Err(_err) => {
                    error_code = ERROR_INVALID_REQUEST;
                }
            }
        }

        let api_keys = if error_code == ERROR_INVALID_REQUEST {
            Vec::new()
        } else {
            supported_api_versions(self.security.sasl.is_some())
        };

        let response = ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms: (response_version >= 1).then_some(0),
            supported_features: None,
            finalized_features_epoch: None,
            finalized_features: None,
            zk_migration_ready: None,
        };

        let body = response.encode(response_version)?;
        encode_response_frame(
            request.correlation_id,
            API_VERSIONS_RESPONSE_HEADER_VERSION,
            &body,
        )
    }

    fn handle_sasl_handshake(
        &mut self,
        request: RequestEnvelope<'_>,
        security_state: &mut ConnectionSecurityState,
    ) -> Result<Vec<u8>, TransportError> {
        if !(SASL_HANDSHAKE_MIN_VERSION..=SASL_HANDSHAKE_MAX_VERSION).contains(&request.api_version)
        {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = SaslHandshakeRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "sasl handshake request has trailing bytes",
            ));
        }
        let Some(sasl_config) = self.security.sasl.as_ref() else {
            return Err(TransportError::UnsupportedApiKey(request.api_key));
        };
        let ConnectionSecurityState::Sasl(sasl_state) = security_state else {
            return Err(TransportError::Security(
                "SASL handshake received on unsecured listener".to_string(),
            ));
        };

        let handshake = begin_sasl_handshake(sasl_config, &decoded.mechanism, sasl_state);
        let response = SaslHandshakeResponse {
            error_code: handshake.error_code,
            mechanisms: handshake.mechanisms,
        };
        let body = response.encode(request.api_version)?;
        encode_response_frame(
            request.correlation_id,
            SASL_HANDSHAKE_RESPONSE_HEADER_VERSION,
            &body,
        )
    }

    fn handle_sasl_authenticate(
        &mut self,
        request: RequestEnvelope<'_>,
        security_state: &mut ConnectionSecurityState,
    ) -> Result<Vec<u8>, TransportError> {
        if !(SASL_AUTHENTICATE_MIN_VERSION..=SASL_AUTHENTICATE_MAX_VERSION)
            .contains(&request.api_version)
        {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = SaslAuthenticateRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "sasl authenticate request has trailing bytes",
            ));
        }
        let Some(sasl_config) = self.security.sasl.as_ref() else {
            return Err(TransportError::UnsupportedApiKey(request.api_key));
        };
        let ConnectionSecurityState::Sasl(sasl_state) = security_state else {
            return Err(TransportError::Security(
                "SASL authenticate received on unsecured listener".to_string(),
            ));
        };

        let current_mechanism = sasl_state.mechanism_name().unwrap_or("UNKNOWN");
        let auth = authenticate_sasl(sasl_config, &decoded.auth_bytes, sasl_state);
        let result = if auth.error_code == ERROR_NONE {
            if auth.complete {
                "success"
            } else {
                "continue"
            }
        } else {
            "failure"
        };
        self.metrics.record_sasl_auth(current_mechanism, result);

        let response = SaslAuthenticateResponse {
            error_code: auth.error_code,
            error_message: auth.error_message,
            auth_bytes: auth.auth_bytes,
            session_lifetime_ms: (request.api_version >= 1).then_some(0),
        };

        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_produce(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(PRODUCE_MIN_VERSION..=PRODUCE_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = ProduceRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "produce request has trailing bytes",
            ));
        }

        let transactional_id = decoded.transactional_id.clone();
        let transactional_authorized = transactional_id.as_deref().is_none_or(|tx_id| {
            self.authorize(
                principal,
                AclOperation::Write,
                AclResourceType::TransactionalId,
                tx_id,
            )
        });
        let transactional_precheck = if transactional_authorized {
            transactional_id
                .as_deref()
                .map(|tx_id| self.transactions.ensure_transactional_id(tx_id))
        } else {
            None
        };

        // ── Phase 2: batch accumulation ───────────────────────────────────────
        // We accumulate all authorised partition payloads into `batch` and
        // flush them with a single write_all + (optional) sync_data per
        // partition at the end of the produce call.  Pre-authorisation
        // errors (ACL failures, transaction precheck failures) are recorded
        // directly without going through the batch.

        struct PartitionSlot {
            index: i32,
            route_key: String,
            payload_len: usize,
            /// `Some` when record was successfully enqueued in the batch.
            enqueued: Option<(String, i32)>, // (route_key, partition_index) for tx recording
            error_code: i16,
        }

        let ts = now_ms();
        let mut batch = PendingWriteBatch::new();
        // Collect per-topic, per-partition intent so we can build the response
        // after the single flush.
        struct TopicIntent {
            name: Option<String>,
            topic_id: Option<[u8; 16]>,
            slots: Vec<PartitionSlot>,
        }
        let mut intents: Vec<TopicIntent> = Vec::with_capacity(decoded.topic_data.len());

        for topic in decoded.topic_data {
            let route_key = produce_route_key(request.api_version, &topic)?;
            let topic_write_authorized = self.authorize(
                principal,
                AclOperation::Write,
                AclResourceType::Topic,
                &route_key,
            );
            let mut slots: Vec<PartitionSlot> = Vec::with_capacity(topic.partition_data.len());

            for partition in topic.partition_data {
                let partition_index = partition.index;
                let payload = partition.records.unwrap_or_default();
                let payload_len = payload.len();

                let (error_code, enqueued) = if !transactional_authorized {
                    (ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, None)
                } else if !topic_write_authorized {
                    (ERROR_TOPIC_AUTHORIZATION_FAILED, None)
                } else {
                    match transactional_precheck.as_ref() {
                        Some(Err(err)) => (map_transaction_error(err), None),
                        _ => {
                            // Priority 1: decode multi-record batch, enqueue each record.
                            let records = crate::decode_records_batch(payload);
                            let mut enqueue_err: Option<i16> = None;
                            for (key, value) in records {
                                if let Err(err) =
                                    batch.enqueue(&route_key, partition_index, key, value, ts)
                                {
                                    enqueue_err = Some(map_partition_error(&err));
                                    break;
                                }
                            }
                            match enqueue_err {
                                Some(code) => (code, None),
                                None => (ERROR_NONE, Some((route_key.clone(), partition_index))),
                            }
                        }
                    }
                };

                slots.push(PartitionSlot {
                    index: partition_index,
                    route_key: route_key.clone(),
                    payload_len,
                    enqueued,
                    error_code,
                });
            }

            intents.push(TopicIntent {
                name: topic.name,
                topic_id: topic.topic_id,
                slots,
            });
        }

        // ── Single flush: one write_all + one sync_data per partition ─────────
        let (mut partition_offsets, flush_errors) = self.broker.flush_pending_batch(batch);

        // Merge flush errors into the slot error codes.
        for (tp, _err) in &flush_errors {
            // Mark any slot that was enqueued for this partition as failed.
            for intent in &mut intents {
                for slot in &mut intent.slots {
                    if let Some((ref rk, pi)) = slot.enqueued {
                        if rk == &tp.topic && pi == tp.partition {
                            slot.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            slot.enqueued = None;
                        }
                    }
                }
            }
        }

        // ── Build response ────────────────────────────────────────────────────
        let mut topic_responses: Vec<ProduceTopicResponse> = Vec::with_capacity(intents.len());
        for intent in intents {
            let mut partition_responses: Vec<ProducePartitionResponse> =
                Vec::with_capacity(intent.slots.len());

            for slot in intent.slots {
                let (error_code, base_offset, log_start_offset) = if slot.error_code != ERROR_NONE {
                    (slot.error_code, -1_i64, -1_i64)
                } else if let Some((ref rk, pi)) = slot.enqueued {
                    // Pop the first offset assigned to this partition.
                    let tp = crate::TopicPartition {
                        topic: rk.clone(),
                        partition: pi,
                    };
                    let assigned_offset = partition_offsets
                        .get_mut(&tp)
                        .and_then(|v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.remove(0))
                            }
                        })
                        .unwrap_or(-1);

                    if assigned_offset < 0 {
                        (ERROR_UNKNOWN_SERVER_ERROR, -1, -1)
                    } else {
                        let mut final_error = ERROR_NONE;
                        if let Some(tx_id) = transactional_id.as_deref() {
                            if let Err(err) =
                                self.transactions
                                    .record_produce(tx_id, rk, pi, assigned_offset)
                            {
                                final_error = map_transaction_error(&err);
                            }
                        }
                        if final_error == ERROR_NONE {
                            (ERROR_NONE, assigned_offset, 0_i64)
                        } else {
                            (final_error, -1, -1)
                        }
                    }
                } else {
                    (ERROR_UNKNOWN_SERVER_ERROR, -1, -1)
                };

                self.metrics.record_partition_event(
                    "produce",
                    &slot.route_key,
                    slot.index,
                    slot.payload_len,
                );

                partition_responses.push(ProducePartitionResponse {
                    index: slot.index,
                    error_code,
                    base_offset,
                    log_start_offset,
                });
            }

            topic_responses.push(ProduceTopicResponse {
                name: intent.name,
                topic_id: intent.topic_id,
                partitions: partition_responses,
            });
        }

        let body = encode_produce_response(request.api_version, &topic_responses, 0)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_fetch(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(FETCH_MIN_VERSION..=FETCH_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = decode_fetch_request(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "fetch request has trailing bytes",
            ));
        }
        let _ = (decoded.replica_epoch, decoded.cluster_id.as_deref());
        let follower_fetch = decoded.replica_id >= 0;
        let follower_authorized = !follower_fetch
            || self.authorize(
                principal,
                AclOperation::ClusterAction,
                AclResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
            );
        let read_committed = decoded.isolation_level == 1;

        let mut topic_responses = Vec::with_capacity(decoded.topics.len());
        for topic in decoded.topics {
            let route_key = fetch_route_key(request.api_version, &topic)?;
            let topic_read_authorized = self.authorize(
                principal,
                AclOperation::Read,
                AclResourceType::Topic,
                &route_key,
            );
            let mut partition_responses = Vec::with_capacity(topic.partitions.len());

            for partition in topic.partitions {
                let _ = (partition.replica_directory_id, partition.high_watermark);
                let max_bytes = usize::try_from(partition.partition_max_bytes.max(0)).unwrap_or(0);
                let unauthorized_topic = (follower_fetch && !follower_authorized)
                    || (!follower_fetch && !topic_read_authorized);
                let partition_response = if unauthorized_topic {
                    FetchPartitionResponse {
                        partition_index: partition.partition,
                        error_code: ERROR_TOPIC_AUTHORIZATION_FAILED,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        aborted_transactions: None,
                        preferred_read_replica: -1,
                        records: None,
                    }
                } else {
                    // ── Phase 3: zero-copy-style fetch path ───────────────────
                    // For non-read_committed fetches we use fetch_file_ranges
                    // to locate the raw value bytes directly in the on-disk
                    // log file, bypassing Record struct allocation and frame
                    // re-decoding.  The high-watermark lookup and the
                    // read_committed transaction-filter path still use the
                    // conventional fetch when needed.
                    let high_watermark =
                        partition_high_watermark(&mut self.broker, &route_key, partition.partition)
                            .unwrap_or(-1);

                    if !read_committed {
                        // Fast path: read value bytes directly from the file.
                        match self.broker.fetch_file_ranges_for_partition(
                            &route_key,
                            partition.partition,
                            partition.fetch_offset,
                            max_bytes,
                        ) {
                            Ok(ranges) => {
                                // Collect value bytes via pread (sendfile fallback).
                                let records_bytes = collect_file_ranges(&ranges);
                                FetchPartitionResponse {
                                    partition_index: partition.partition,
                                    error_code: ERROR_NONE,
                                    high_watermark,
                                    last_stable_offset: high_watermark,
                                    log_start_offset: 0,
                                    aborted_transactions: None,
                                    preferred_read_replica: -1,
                                    records: if records_bytes.is_empty() {
                                        None
                                    } else {
                                        Some(records_bytes)
                                    },
                                }
                            }
                            Err(err) => FetchPartitionResponse {
                                partition_index: partition.partition,
                                error_code: map_fetch_partition_error(&err),
                                high_watermark: -1,
                                last_stable_offset: -1,
                                log_start_offset: -1,
                                aborted_transactions: None,
                                preferred_read_replica: -1,
                                records: None,
                            },
                        }
                    } else {
                        // read_committed path: need Record structs for tx filter.
                        let fetched = self.broker.fetch_from_partition_bounded(
                            &route_key,
                            partition.partition,
                            partition.fetch_offset,
                            usize::MAX,
                            max_bytes,
                        );
                        match fetched {
                            Ok(records) => {
                                let committed = self.transactions.read_committed(
                                    &route_key,
                                    partition.partition,
                                    partition.fetch_offset,
                                    high_watermark,
                                    &records,
                                );
                                FetchPartitionResponse {
                                    partition_index: partition.partition,
                                    error_code: ERROR_NONE,
                                    high_watermark,
                                    last_stable_offset: committed.last_stable_offset,
                                    log_start_offset: 0,
                                    aborted_transactions: Some(
                                        committed
                                            .aborted_transactions
                                            .into_iter()
                                            .map(|entry| FetchAbortedTransaction {
                                                producer_id: entry.producer_id,
                                                first_offset: entry.first_offset,
                                            })
                                            .collect(),
                                    ),
                                    preferred_read_replica: -1,
                                    records: Some(limit_fetch_payload_by_bytes(
                                        committed.visible_records,
                                        max_bytes,
                                    )),
                                }
                            }
                            Err(err) => FetchPartitionResponse {
                                partition_index: partition.partition,
                                error_code: map_fetch_partition_error(&err),
                                high_watermark: -1,
                                last_stable_offset: -1,
                                log_start_offset: -1,
                                aborted_transactions: None,
                                preferred_read_replica: -1,
                                records: None,
                            },
                        }
                    }
                };
                self.metrics.record_partition_event(
                    "fetch",
                    &route_key,
                    partition.partition,
                    partition_response
                        .records
                        .as_ref()
                        .map_or(0, |records| records.len()),
                );

                partition_responses.push(partition_response);
            }

            topic_responses.push(FetchTopicResponse {
                name: topic.name,
                topic_id: topic.topic_id,
                partitions: partition_responses,
            });
        }

        let body = encode_fetch_response(request.api_version, 0, 0, 0, &topic_responses, &[])?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_offset_commit(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(OFFSET_COMMIT_MIN_VERSION..=OFFSET_COMMIT_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = OffsetCommitRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "offset commit request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Read,
            AclResourceType::Group,
            &decoded.group_id,
        ) {
            let topics = decoded
                .topics
                .into_iter()
                .map(|topic| OffsetCommitResponseOffsetCommitResponseTopic {
                    name: topic.name,
                    topic_id: topic.topic_id,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(
                            |partition| OffsetCommitResponseOffsetCommitResponsePartition {
                                partition_index: partition.partition_index,
                                error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
                            },
                        )
                        .collect(),
                })
                .collect();

            let response = OffsetCommitResponse {
                throttle_time_ms: (request.api_version >= 3).then_some(0),
                topics,
            };
            let body = response.encode(request.api_version)?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let mut topic_responses = Vec::with_capacity(decoded.topics.len());
        for topic in decoded.topics {
            let mut partition_responses = Vec::with_capacity(topic.partitions.len());
            let commit_topic = offset_topic_from_commit_request(request.api_version, &topic);

            for partition in topic.partitions {
                let error_code = match &commit_topic {
                    Ok(topic) => {
                        let topic_resource = offset_topic_resource_name(topic);
                        if !self.authorize(
                            principal,
                            AclOperation::Read,
                            AclResourceType::Topic,
                            &topic_resource,
                        ) {
                            ERROR_TOPIC_AUTHORIZATION_FAILED
                        } else {
                            match self.offsets.commit_offset(OffsetCommitInput {
                                api_version: request.api_version,
                                group_id: &decoded.group_id,
                                member_id: &decoded.member_id,
                                member_epoch: decoded.generation_id_or_member_epoch,
                                topic: topic.clone(),
                                partition: partition.partition_index,
                                committed_offset: partition.committed_offset,
                                committed_leader_epoch: partition
                                    .committed_leader_epoch
                                    .unwrap_or(-1),
                                metadata: partition.committed_metadata,
                                commit_timestamp_ms: now_ms(),
                            }) {
                                Ok(()) => ERROR_NONE,
                                Err(err) => map_offset_commit_error(request.api_version, &err),
                            }
                        }
                    }
                    Err(error_code) => *error_code,
                };

                partition_responses.push(OffsetCommitResponseOffsetCommitResponsePartition {
                    partition_index: partition.partition_index,
                    error_code,
                });
            }

            topic_responses.push(OffsetCommitResponseOffsetCommitResponseTopic {
                name: topic.name,
                topic_id: topic.topic_id,
                partitions: partition_responses,
            });
        }

        let response = OffsetCommitResponse {
            throttle_time_ms: (request.api_version >= 3).then_some(0),
            topics: topic_responses,
        };
        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_offset_fetch(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(OFFSET_FETCH_MIN_VERSION..=OFFSET_FETCH_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = OffsetFetchRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "offset fetch request has trailing bytes",
            ));
        }

        let response = if request.api_version <= 7 {
            let group_id = decoded.group_id.ok_or(TransportError::InvalidHeader(
                "offset fetch missing group_id",
            ))?;

            if !self.authorize(
                principal,
                AclOperation::Describe,
                AclResourceType::Group,
                &group_id,
            ) {
                let response = OffsetFetchResponse {
                    throttle_time_ms: (request.api_version >= 3).then_some(0),
                    topics: Some(Vec::new()),
                    error_code: (request.api_version >= 2)
                        .then_some(ERROR_GROUP_AUTHORIZATION_FAILED),
                    groups: None,
                };
                let body = response.encode(request.api_version)?;
                let header_version = response_header_version(request.api_key, request.api_version)?;
                return encode_response_frame(request.correlation_id, header_version, &body);
            }

            let response_topics = if let Some(topics) = decoded.topics {
                encode_offset_fetch_topics_legacy(
                    request.api_version,
                    &self.offsets,
                    &group_id,
                    None,
                    None,
                    &topics,
                    |topic| {
                        let resource = offset_topic_resource_name(topic);
                        self.authorize(
                            principal,
                            AclOperation::Describe,
                            AclResourceType::Topic,
                            &resource,
                        )
                    },
                )
            } else {
                encode_offset_fetch_topics_legacy_all(
                    request.api_version,
                    &self.offsets,
                    &group_id,
                    None,
                    None,
                    |topic| {
                        let resource = offset_topic_resource_name(topic);
                        self.authorize(
                            principal,
                            AclOperation::Describe,
                            AclResourceType::Topic,
                            &resource,
                        )
                    },
                )
            };

            match response_topics {
                Ok(topics) => OffsetFetchResponse {
                    throttle_time_ms: (request.api_version >= 3).then_some(0),
                    topics: Some(topics),
                    error_code: (request.api_version >= 2).then_some(ERROR_NONE),
                    groups: None,
                },
                Err(err) => OffsetFetchResponse {
                    throttle_time_ms: (request.api_version >= 3).then_some(0),
                    topics: Some(Vec::new()),
                    error_code: (request.api_version >= 2)
                        .then_some(map_offset_fetch_group_error(&err)),
                    groups: None,
                },
            }
        } else {
            let groups = decoded
                .groups
                .ok_or(TransportError::InvalidHeader("offset fetch missing groups"))?;
            let mut response_groups = Vec::with_capacity(groups.len());
            for group in groups {
                let member_id = group.member_id.as_deref();
                let member_epoch = group.member_epoch;

                if !self.authorize(
                    principal,
                    AclOperation::Describe,
                    AclResourceType::Group,
                    &group.group_id,
                ) {
                    response_groups.push(OffsetFetchResponseOffsetFetchResponseGroup {
                        group_id: group.group_id,
                        topics: Vec::new(),
                        error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
                    });
                    continue;
                }

                let group_topics = if let Some(topics) = group.topics.as_ref() {
                    encode_offset_fetch_topics_grouped(
                        request.api_version,
                        &self.offsets,
                        &group.group_id,
                        member_id,
                        member_epoch,
                        topics,
                        |topic| {
                            let resource = offset_topic_resource_name(topic);
                            self.authorize(
                                principal,
                                AclOperation::Describe,
                                AclResourceType::Topic,
                                &resource,
                            )
                        },
                    )
                } else {
                    encode_offset_fetch_topics_grouped_all(
                        request.api_version,
                        &self.offsets,
                        &group.group_id,
                        member_id,
                        member_epoch,
                        |topic| {
                            let resource = offset_topic_resource_name(topic);
                            self.authorize(
                                principal,
                                AclOperation::Describe,
                                AclResourceType::Topic,
                                &resource,
                            )
                        },
                    )
                };

                match group_topics {
                    Ok(topics) => {
                        response_groups.push(OffsetFetchResponseOffsetFetchResponseGroup {
                            group_id: group.group_id,
                            topics,
                            error_code: ERROR_NONE,
                        });
                    }
                    Err(err) => {
                        response_groups.push(OffsetFetchResponseOffsetFetchResponseGroup {
                            group_id: group.group_id,
                            topics: Vec::new(),
                            error_code: map_offset_fetch_group_error(&err),
                        });
                    }
                }
            }

            OffsetFetchResponse {
                throttle_time_ms: (request.api_version >= 3).then_some(0),
                topics: None,
                error_code: None,
                groups: Some(response_groups),
            }
        };

        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_join_group(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(JOIN_GROUP_MIN_VERSION..=JOIN_GROUP_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = JoinGroupRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "join group request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Read,
            AclResourceType::Group,
            &decoded.group_id,
        ) {
            let response = JoinGroupResponse {
                throttle_time_ms: (request.api_version >= 2).then_some(0),
                error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
                generation_id: -1,
                protocol_type: None,
                protocol_name: if request.api_version >= 7 {
                    None
                } else {
                    Some(String::new())
                },
                leader: String::new(),
                skip_assignment: (request.api_version >= 9).then_some(false),
                member_id: decoded.member_id,
                members: Vec::new(),
            };
            let body = response.encode(request.api_version)?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let join_result = self.groups.join_group(
            JoinGroupInput {
                api_version: request.api_version,
                group_id: decoded.group_id.clone(),
                session_timeout_ms: decoded.session_timeout_ms,
                rebalance_timeout_ms: decoded
                    .rebalance_timeout_ms
                    .unwrap_or(decoded.session_timeout_ms),
                member_id: decoded.member_id.clone(),
                group_instance_id: decoded.group_instance_id.clone(),
                protocol_type: decoded.protocol_type.clone(),
                protocols: decoded
                    .protocols
                    .iter()
                    .map(|protocol| JoinProtocol {
                        name: protocol.name.clone(),
                        metadata: protocol.metadata.clone(),
                    })
                    .collect(),
            },
            u64::try_from(now_ms()).unwrap_or(0),
        );
        let _ = decoded.reason.as_deref();

        let response = match join_result {
            Ok(joined) => JoinGroupResponse {
                throttle_time_ms: (request.api_version >= 2).then_some(0),
                error_code: ERROR_NONE,
                generation_id: joined.generation_id,
                protocol_type: joined.protocol_type,
                protocol_name: if request.api_version >= 7 {
                    joined.protocol_name
                } else {
                    Some(joined.protocol_name.unwrap_or_default())
                },
                leader: joined.leader,
                skip_assignment: (request.api_version >= 9).then_some(joined.skip_assignment),
                member_id: joined.member_id,
                members: joined
                    .members
                    .into_iter()
                    .map(|member| JoinGroupResponseJoinGroupResponseMember {
                        member_id: member.member_id,
                        group_instance_id: member.group_instance_id,
                        metadata: member.metadata,
                    })
                    .collect(),
            },
            Err(ClassicGroupCoordinatorError::MemberIdRequired(member_id)) => JoinGroupResponse {
                throttle_time_ms: (request.api_version >= 2).then_some(0),
                error_code: ERROR_MEMBER_ID_REQUIRED,
                generation_id: -1,
                protocol_type: None,
                protocol_name: if request.api_version >= 7 {
                    None
                } else {
                    Some(String::new())
                },
                leader: String::new(),
                skip_assignment: (request.api_version >= 9).then_some(false),
                member_id,
                members: Vec::new(),
            },
            Err(err) => JoinGroupResponse {
                throttle_time_ms: (request.api_version >= 2).then_some(0),
                error_code: map_join_group_error(&err),
                generation_id: -1,
                protocol_type: None,
                protocol_name: if request.api_version >= 7 {
                    None
                } else {
                    Some(String::new())
                },
                leader: String::new(),
                skip_assignment: (request.api_version >= 9).then_some(false),
                member_id: decoded.member_id,
                members: Vec::new(),
            },
        };

        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_sync_group(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(SYNC_GROUP_MIN_VERSION..=SYNC_GROUP_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = SyncGroupRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "sync group request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Read,
            AclResourceType::Group,
            &decoded.group_id,
        ) {
            let response = SyncGroupResponse {
                throttle_time_ms: (request.api_version >= 1).then_some(0),
                error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
                protocol_type: None,
                protocol_name: None,
                assignment: Vec::new(),
            };
            let body = response.encode(request.api_version)?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let sync_result = self.groups.sync_group(SyncGroupInput {
            api_version: request.api_version,
            group_id: decoded.group_id.clone(),
            generation_id: decoded.generation_id,
            member_id: decoded.member_id.clone(),
            group_instance_id: decoded.group_instance_id.clone(),
            protocol_type: decoded.protocol_type.clone(),
            protocol_name: decoded.protocol_name.clone(),
            assignments: decoded
                .assignments
                .iter()
                .map(|assignment| SyncAssignment {
                    member_id: assignment.member_id.clone(),
                    assignment: assignment.assignment.clone(),
                })
                .collect(),
        });

        let response = match sync_result {
            Ok(synced) => SyncGroupResponse {
                throttle_time_ms: (request.api_version >= 1).then_some(0),
                error_code: ERROR_NONE,
                protocol_type: synced.protocol_type,
                protocol_name: synced.protocol_name,
                assignment: synced.assignment,
            },
            Err(err) => SyncGroupResponse {
                throttle_time_ms: (request.api_version >= 1).then_some(0),
                error_code: map_sync_group_error(&err),
                protocol_type: None,
                protocol_name: None,
                assignment: Vec::new(),
            },
        };

        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_heartbeat(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(HEARTBEAT_MIN_VERSION..=HEARTBEAT_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = HeartbeatRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "heartbeat request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Read,
            AclResourceType::Group,
            &decoded.group_id,
        ) {
            let response = HeartbeatResponse {
                throttle_time_ms: (request.api_version >= 1).then_some(0),
                error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
            };
            let body = response.encode(request.api_version)?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let error_code = match self.groups.heartbeat(
            HeartbeatInput {
                group_id: decoded.group_id,
                generation_id: decoded.generation_id,
                member_id: decoded.member_id,
                group_instance_id: decoded.group_instance_id,
            },
            u64::try_from(now_ms()).unwrap_or(0),
        ) {
            Ok(()) => ERROR_NONE,
            Err(err) => map_heartbeat_error(&err),
        };

        let response = HeartbeatResponse {
            throttle_time_ms: (request.api_version >= 1).then_some(0),
            error_code,
        };
        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_leave_group(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(LEAVE_GROUP_MIN_VERSION..=LEAVE_GROUP_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = LeaveGroupRequest::decode(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "leave group request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Read,
            AclResourceType::Group,
            &decoded.group_id,
        ) {
            let response = LeaveGroupResponse {
                throttle_time_ms: (request.api_version >= 1).then_some(0),
                error_code: ERROR_GROUP_AUTHORIZATION_FAILED,
                members: (request.api_version >= 3).then_some(Vec::new()),
            };
            let body = response.encode(request.api_version)?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let mut response = LeaveGroupResponse {
            throttle_time_ms: (request.api_version >= 1).then_some(0),
            error_code: ERROR_NONE,
            members: (request.api_version >= 3).then_some(Vec::new()),
        };

        if request.api_version <= 2 {
            let member_id = decoded.member_id.ok_or(TransportError::InvalidHeader(
                "leave group missing member id",
            ))?;
            if let Err(err) = self
                .groups
                .leave_group_legacy(&decoded.group_id, &member_id)
            {
                response.error_code = map_leave_group_error(&err);
            }
        } else {
            let leave_members = decoded
                .members
                .as_ref()
                .ok_or(TransportError::InvalidHeader("leave group missing members"))?;
            let leave_members: Vec<LeaveGroupMemberInput> = leave_members
                .iter()
                .map(|member| LeaveGroupMemberInput {
                    member_id: member.member_id.clone(),
                    group_instance_id: member.group_instance_id.clone(),
                })
                .collect();
            match self
                .groups
                .leave_group_members(&decoded.group_id, &leave_members)
            {
                Ok(outcomes) => {
                    response.members = Some(
                        outcomes
                            .into_iter()
                            .map(|member| LeaveGroupResponseMemberResponse {
                                member_id: member.member_id,
                                group_instance_id: member.group_instance_id,
                                error_code: member
                                    .error
                                    .as_ref()
                                    .map_or(ERROR_NONE, map_leave_group_member_error),
                            })
                            .collect(),
                    );
                }
                Err(err) => {
                    response.error_code = map_leave_group_error(&err);
                }
            }
        }

        let body = response.encode(request.api_version)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_init_producer_id(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(INIT_PRODUCER_ID_MIN_VERSION..=INIT_PRODUCER_ID_MAX_VERSION)
            .contains(&request.api_version)
        {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = decode_init_producer_id_request(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "init producer id request has trailing bytes",
            ));
        }
        let _ = (decoded.enable_2pc, decoded.keep_prepared_txn);

        if let Some(transactional_id) = decoded.transactional_id.as_deref() {
            if !self.authorize(
                principal,
                AclOperation::Write,
                AclResourceType::TransactionalId,
                transactional_id,
            ) {
                let body = encode_init_producer_id_response(
                    request.api_version,
                    &InitProducerIdResponseBody {
                        throttle_time_ms: 0,
                        error_code: ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                        producer_id: -1,
                        producer_epoch: -1,
                        ongoing_txn_producer_id: -1,
                        ongoing_txn_producer_epoch: -1,
                    },
                )?;
                let header_version = response_header_version(request.api_key, request.api_version)?;
                return encode_response_frame(request.correlation_id, header_version, &body);
            }
        } else if !self.authorize(
            principal,
            AclOperation::IdempotentWrite,
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        ) && !self.authorize_any(principal, AclOperation::Write, AclResourceType::Topic)
        {
            let body = encode_init_producer_id_response(
                request.api_version,
                &InitProducerIdResponseBody {
                    throttle_time_ms: 0,
                    error_code: ERROR_CLUSTER_AUTHORIZATION_FAILED,
                    producer_id: -1,
                    producer_epoch: -1,
                    ongoing_txn_producer_id: -1,
                    ongoing_txn_producer_epoch: -1,
                },
            )?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let response = match self.transactions.init_producer(InitProducerInput {
            api_version: request.api_version,
            transactional_id: decoded.transactional_id,
            transaction_timeout_ms: decoded.transaction_timeout_ms,
            producer_id: decoded.producer_id,
            producer_epoch: decoded.producer_epoch,
        }) {
            Ok(init) => InitProducerIdResponseBody {
                throttle_time_ms: 0,
                error_code: ERROR_NONE,
                producer_id: init.producer_id,
                producer_epoch: init.producer_epoch,
                ongoing_txn_producer_id: init.ongoing_txn_producer_id,
                ongoing_txn_producer_epoch: init.ongoing_txn_producer_epoch,
            },
            Err(err) => InitProducerIdResponseBody {
                throttle_time_ms: 0,
                error_code: map_transaction_error(&err),
                producer_id: -1,
                producer_epoch: -1,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            },
        };

        let body = encode_init_producer_id_response(request.api_version, &response)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_end_txn(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(END_TXN_MIN_VERSION..=END_TXN_MAX_VERSION).contains(&request.api_version) {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = decode_end_txn_request(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "end txn request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::Write,
            AclResourceType::TransactionalId,
            &decoded.transactional_id,
        ) {
            let body = encode_end_txn_response(
                request.api_version,
                &EndTxnResponseBody {
                    throttle_time_ms: 0,
                    error_code: ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    producer_id: -1,
                    producer_epoch: -1,
                },
            )?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let response = match self.transactions.end_txn(EndTxnInput {
            api_version: request.api_version,
            transactional_id: decoded.transactional_id,
            producer_id: decoded.producer_id,
            producer_epoch: decoded.producer_epoch,
            committed: decoded.committed,
        }) {
            Ok(outcome) => EndTxnResponseBody {
                throttle_time_ms: 0,
                error_code: ERROR_NONE,
                producer_id: outcome.producer_id,
                producer_epoch: outcome.producer_epoch,
            },
            Err(err) => EndTxnResponseBody {
                throttle_time_ms: 0,
                error_code: map_transaction_error(&err),
                producer_id: -1,
                producer_epoch: -1,
            },
        };

        let body = encode_end_txn_response(request.api_version, &response)?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }

    fn handle_write_txn_markers(
        &mut self,
        request: RequestEnvelope<'_>,
        principal: &str,
    ) -> Result<Vec<u8>, TransportError> {
        if !(WRITE_TXN_MARKERS_MIN_VERSION..=WRITE_TXN_MARKERS_MAX_VERSION)
            .contains(&request.api_version)
        {
            return Err(TransportError::UnsupportedApiVersion {
                api_key: request.api_key,
                api_version: request.api_version,
            });
        }

        let (decoded, read) = decode_write_txn_markers_request(request.api_version, request.body)?;
        if read != request.body.len() {
            return Err(TransportError::InvalidHeader(
                "write txn markers request has trailing bytes",
            ));
        }

        if !self.authorize(
            principal,
            AclOperation::ClusterAction,
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        ) {
            let markers = decoded
                .markers
                .iter()
                .map(|marker| WriteTxnMarkersResponseMarkerBody {
                    producer_id: marker.producer_id,
                    topics: marker
                        .topics
                        .iter()
                        .map(|topic| WriteTxnMarkersResponseTopicBody {
                            name: topic.name.clone(),
                            partitions: topic
                                .partition_indexes
                                .iter()
                                .map(|partition_index| WriteTxnMarkersResponsePartitionBody {
                                    partition_index: *partition_index,
                                    error_code: ERROR_CLUSTER_AUTHORIZATION_FAILED,
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect();

            let body = encode_write_txn_markers_response(
                request.api_version,
                &WriteTxnMarkersResponseBody { markers },
            )?;
            let header_version = response_header_version(request.api_key, request.api_version)?;
            return encode_response_frame(request.correlation_id, header_version, &body);
        }

        let markers = decoded
            .markers
            .iter()
            .map(|marker| {
                let _ = (marker.coordinator_epoch, marker.transaction_version);
                WriteTxnMarkerInput {
                    producer_id: marker.producer_id,
                    producer_epoch: marker.producer_epoch,
                    committed: marker.transaction_result,
                    topics: marker
                        .topics
                        .iter()
                        .map(|topic| WriteTxnMarkerTopicInput {
                            name: topic.name.clone(),
                            partition_indexes: topic.partition_indexes.clone(),
                        })
                        .collect(),
                }
            })
            .collect();

        let response_markers = match self.transactions.write_txn_markers(markers) {
            Ok(results) => results
                .into_iter()
                .map(|marker| WriteTxnMarkersResponseMarkerBody {
                    producer_id: marker.producer_id,
                    topics: marker
                        .topics
                        .into_iter()
                        .map(|topic| WriteTxnMarkersResponseTopicBody {
                            name: topic.name,
                            partitions: topic
                                .partitions
                                .into_iter()
                                .map(|partition| WriteTxnMarkersResponsePartitionBody {
                                    partition_index: partition.partition_index,
                                    error_code: partition
                                        .error
                                        .as_ref()
                                        .map_or(ERROR_NONE, map_transaction_error),
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
            Err(err) => decoded
                .markers
                .into_iter()
                .map(|marker| WriteTxnMarkersResponseMarkerBody {
                    producer_id: marker.producer_id,
                    topics: marker
                        .topics
                        .into_iter()
                        .map(|topic| WriteTxnMarkersResponseTopicBody {
                            name: topic.name,
                            partitions: topic
                                .partition_indexes
                                .into_iter()
                                .map(|partition_index| WriteTxnMarkersResponsePartitionBody {
                                    partition_index,
                                    error_code: map_transaction_error(&err),
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
        };

        let body = encode_write_txn_markers_response(
            request.api_version,
            &WriteTxnMarkersResponseBody {
                markers: response_markers,
            },
        )?;
        let header_version = response_header_version(request.api_key, request.api_version)?;
        encode_response_frame(request.correlation_id, header_version, &body)
    }
}

fn connection_principal(security_state: &ConnectionSecurityState) -> &str {
    match security_state {
        ConnectionSecurityState::Disabled => ANONYMOUS_PRINCIPAL,
        ConnectionSecurityState::Sasl(state) => state.principal().unwrap_or(ANONYMOUS_PRINCIPAL),
    }
}

#[derive(Debug, Clone, Copy)]
struct RequestEnvelope<'a> {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &'a [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct ProduceTopicResponse {
    pub(crate) name: Option<String>,
    pub(crate) topic_id: Option<[u8; 16]>,
    pub(crate) partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProducePartitionResponse {
    pub(crate) index: i32,
    pub(crate) error_code: i16,
    pub(crate) base_offset: i64,
    pub(crate) log_start_offset: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchRequestBody {
    pub(crate) replica_id: i32,
    pub(crate) replica_epoch: Option<i64>,
    pub(crate) cluster_id: Option<String>,
    pub(crate) isolation_level: i8,
    pub(crate) topics: Vec<FetchTopicRequest>,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchTopicRequest {
    pub(crate) name: Option<String>,
    pub(crate) topic_id: Option<[u8; 16]>,
    pub(crate) partitions: Vec<FetchPartitionRequest>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchPartitionRequest {
    pub(crate) partition: i32,
    pub(crate) fetch_offset: i64,
    pub(crate) partition_max_bytes: i32,
    pub(crate) replica_directory_id: Option<[u8; 16]>,
    pub(crate) high_watermark: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchTopicResponse {
    pub(crate) name: Option<String>,
    pub(crate) topic_id: Option<[u8; 16]>,
    pub(crate) partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchPartitionResponse {
    pub(crate) partition_index: i32,
    pub(crate) error_code: i16,
    pub(crate) high_watermark: i64,
    pub(crate) last_stable_offset: i64,
    pub(crate) log_start_offset: i64,
    pub(crate) aborted_transactions: Option<Vec<FetchAbortedTransaction>>,
    pub(crate) preferred_read_replica: i32,
    pub(crate) records: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchAbortedTransaction {
    pub(crate) producer_id: i64,
    pub(crate) first_offset: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchNodeEndpointResponse {
    pub(crate) node_id: i32,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) rack: Option<String>,
}

#[derive(Debug, Clone)]
struct InitProducerIdRequestBody {
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
    enable_2pc: bool,
    keep_prepared_txn: bool,
}

#[derive(Debug, Clone)]
struct InitProducerIdResponseBody {
    throttle_time_ms: i32,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
    ongoing_txn_producer_id: i64,
    ongoing_txn_producer_epoch: i16,
}

#[derive(Debug, Clone)]
struct EndTxnRequestBody {
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
}

#[derive(Debug, Clone)]
struct EndTxnResponseBody {
    throttle_time_ms: i32,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersRequestBody {
    markers: Vec<WriteTxnMarkersRequestMarkerBody>,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersRequestMarkerBody {
    producer_id: i64,
    producer_epoch: i16,
    transaction_result: bool,
    topics: Vec<WriteTxnMarkersRequestTopicBody>,
    coordinator_epoch: i32,
    transaction_version: i8,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersRequestTopicBody {
    name: String,
    partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersResponseBody {
    markers: Vec<WriteTxnMarkersResponseMarkerBody>,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersResponseMarkerBody {
    producer_id: i64,
    topics: Vec<WriteTxnMarkersResponseTopicBody>,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersResponseTopicBody {
    name: String,
    partitions: Vec<WriteTxnMarkersResponsePartitionBody>,
}

#[derive(Debug, Clone)]
struct WriteTxnMarkersResponsePartitionBody {
    partition_index: i32,
    error_code: i16,
}

fn parse_request_envelope(frame: &[u8]) -> Result<RequestEnvelope<'_>, TransportError> {
    let mut cursor = 0;
    let api_key = read_i16(frame, &mut cursor)?;
    let api_version = read_i16(frame, &mut cursor)?;
    let header_version = request_header_version(api_key, api_version)?;

    let correlation_id = read_i32(frame, &mut cursor)?;
    let _client_id = read_nullable_legacy_string(frame, &mut cursor)?;

    if header_version == 2 {
        skip_tagged_fields(frame, &mut cursor)?;
    }

    Ok(RequestEnvelope {
        api_key,
        api_version,
        correlation_id,
        body: &frame[cursor..],
    })
}

fn request_header_version(api_key: i16, api_version: i16) -> Result<i16, TransportError> {
    match api_key {
        API_KEY_API_VERSIONS => {
            if api_version >= 3 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_PRODUCE => {
            if api_version >= 9 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_FETCH => {
            if api_version >= 12 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_OFFSET_COMMIT => {
            if api_version >= 8 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_OFFSET_FETCH => {
            if api_version >= 6 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_JOIN_GROUP => {
            if api_version >= 6 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_HEARTBEAT => {
            if api_version >= 4 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_LEAVE_GROUP => {
            if api_version >= 4 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_SYNC_GROUP => {
            if api_version >= 4 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_SASL_HANDSHAKE => Ok(1),
        API_KEY_INIT_PRODUCER_ID => {
            if api_version >= 2 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_END_TXN => {
            if api_version >= 3 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        API_KEY_WRITE_TXN_MARKERS => Ok(2),
        API_KEY_SASL_AUTHENTICATE => {
            if api_version >= 2 {
                Ok(2)
            } else {
                Ok(1)
            }
        }
        _ => Err(TransportError::UnsupportedApiKey(api_key)),
    }
}

pub(crate) fn response_header_version(
    api_key: i16,
    api_version: i16,
) -> Result<i16, TransportError> {
    match api_key {
        API_KEY_API_VERSIONS => Ok(API_VERSIONS_RESPONSE_HEADER_VERSION),
        API_KEY_PRODUCE => {
            if api_version >= 9 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_FETCH => {
            if api_version >= 12 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_OFFSET_COMMIT => {
            if api_version >= 8 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_OFFSET_FETCH => {
            if api_version >= 6 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_JOIN_GROUP => {
            if api_version >= 6 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_HEARTBEAT => {
            if api_version >= 4 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_LEAVE_GROUP => {
            if api_version >= 4 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_SYNC_GROUP => {
            if api_version >= 4 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_SASL_HANDSHAKE => Ok(0),
        API_KEY_INIT_PRODUCER_ID => {
            if api_version >= 2 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_END_TXN => {
            if api_version >= 3 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        API_KEY_WRITE_TXN_MARKERS => Ok(1),
        API_KEY_SASL_AUTHENTICATE => {
            if api_version >= 2 {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        _ => Err(TransportError::UnsupportedApiKey(api_key)),
    }
}

pub(crate) fn encode_response_frame(
    correlation_id: i32,
    header_version: i16,
    body: &[u8],
) -> Result<Vec<u8>, TransportError> {
    let mut header = Vec::with_capacity(8);
    header.extend_from_slice(&correlation_id.to_be_bytes());
    if header_version == 1 {
        write_unsigned_varint(&mut header, 0);
    } else if header_version != 0 {
        return Err(TransportError::InvalidHeader(
            "unsupported response header version",
        ));
    }

    let frame_len = header
        .len()
        .checked_add(body.len())
        .ok_or(TransportError::InvalidFrameSize(i32::MAX))?;
    let frame_len_i32 =
        i32::try_from(frame_len).map_err(|_| TransportError::InvalidFrameSize(i32::MAX))?;

    let mut out = Vec::with_capacity(4 + frame_len);
    out.extend_from_slice(&frame_len_i32.to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(body);
    Ok(out)
}

fn supported_api_versions(sasl_enabled: bool) -> Vec<ApiVersionsResponseApiVersion> {
    let mut versions = vec![
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_PRODUCE,
            min_version: PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION,
            max_version: PRODUCE_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_FETCH,
            min_version: FETCH_MIN_VERSION,
            max_version: FETCH_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_API_VERSIONS,
            min_version: API_VERSIONS_MIN_VERSION,
            max_version: API_VERSIONS_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_OFFSET_COMMIT,
            min_version: OFFSET_COMMIT_MIN_VERSION,
            max_version: OFFSET_COMMIT_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_OFFSET_FETCH,
            min_version: OFFSET_FETCH_MIN_VERSION,
            max_version: OFFSET_FETCH_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_JOIN_GROUP,
            min_version: JOIN_GROUP_MIN_VERSION,
            max_version: JOIN_GROUP_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_HEARTBEAT,
            min_version: HEARTBEAT_MIN_VERSION,
            max_version: HEARTBEAT_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_LEAVE_GROUP,
            min_version: LEAVE_GROUP_MIN_VERSION,
            max_version: LEAVE_GROUP_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_SYNC_GROUP,
            min_version: SYNC_GROUP_MIN_VERSION,
            max_version: SYNC_GROUP_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_INIT_PRODUCER_ID,
            min_version: INIT_PRODUCER_ID_MIN_VERSION,
            max_version: INIT_PRODUCER_ID_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_END_TXN,
            min_version: END_TXN_MIN_VERSION,
            max_version: END_TXN_MAX_VERSION,
        },
        ApiVersionsResponseApiVersion {
            api_key: API_KEY_WRITE_TXN_MARKERS,
            min_version: WRITE_TXN_MARKERS_MIN_VERSION,
            max_version: WRITE_TXN_MARKERS_MAX_VERSION,
        },
    ];
    if sasl_enabled {
        versions.push(ApiVersionsResponseApiVersion {
            api_key: API_KEY_SASL_HANDSHAKE,
            min_version: SASL_HANDSHAKE_MIN_VERSION,
            max_version: SASL_HANDSHAKE_MAX_VERSION,
        });
        versions.push(ApiVersionsResponseApiVersion {
            api_key: API_KEY_SASL_AUTHENTICATE,
            min_version: SASL_AUTHENTICATE_MIN_VERSION,
            max_version: SASL_AUTHENTICATE_MAX_VERSION,
        });
    }
    versions
}

fn produce_route_key(
    version: i16,
    topic: &rafka_protocol::messages::ProduceRequestTopicProduceData,
) -> Result<String, TransportError> {
    if version <= 12 {
        return topic
            .name
            .clone()
            .ok_or(TransportError::InvalidHeader("missing topic name"));
    }

    let topic_id = topic
        .topic_id
        .ok_or(TransportError::InvalidHeader("missing topic id"))?;
    Ok(topic_id_to_hex(topic_id))
}

pub(crate) fn fetch_route_key(
    version: i16,
    topic: &FetchTopicRequest,
) -> Result<String, TransportError> {
    if version <= 12 {
        return topic
            .name
            .clone()
            .ok_or(TransportError::InvalidHeader("missing topic name"));
    }

    let topic_id = topic
        .topic_id
        .ok_or(TransportError::InvalidHeader("missing topic id"))?;
    Ok(topic_id_to_hex(topic_id))
}

fn limit_fetch_payload_by_bytes(records: Vec<crate::Record>, max_bytes: usize) -> Vec<u8> {
    if max_bytes == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    for record in records {
        let record_len = record.value.len();
        if out.is_empty() && record_len > max_bytes {
            // Mirror Kafka behavior where the first batch may exceed max bytes.
            out.extend_from_slice(&record.value);
            break;
        }
        if out.len().saturating_add(record_len) > max_bytes {
            break;
        }
        out.extend_from_slice(&record.value);
    }
    out
}

fn partition_high_watermark(
    broker: &mut PartitionedBroker,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    // next_offset_for_partition is O(1): no log scan needed after open.
    broker.next_offset_for_partition(topic, partition).ok()
}

fn topic_id_to_hex(topic_id: [u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in topic_id {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Phase 3 helper: read all value bytes from a list of `FileRange`s into one
/// contiguous buffer using positional reads (pread / sendfile fallback).
///
/// On Linux this can be replaced with a `sendfile` loop once the nix crate is
/// available.  The function signature and call site remain unchanged.
pub(crate) fn collect_file_ranges(ranges: &[crate::FileRange]) -> Vec<u8> {
    use std::fs::File;
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;

    let total: usize = ranges
        .iter()
        .map(|r| usize::try_from(r.byte_len).unwrap_or(usize::MAX))
        .fold(0_usize, usize::saturating_add);
    let mut out = Vec::with_capacity(total);

    for range in ranges {
        let len = match usize::try_from(range.byte_len) {
            Ok(l) => l,
            Err(_) => continue,
        };
        let start = out.len();
        out.resize(start + len, 0);

        #[cfg(unix)]
        {
            if let Ok(file) = File::open(&range.path) {
                let _ = file.read_exact_at(&mut out[start..], range.file_byte_offset);
            }
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            if let Ok(mut file) = File::open(&range.path) {
                let _ = file.seek(SeekFrom::Start(range.file_byte_offset));
                let _ = file.read_exact(&mut out[start..]);
            }
        }
    }
    out
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn transport_error_class(error: &TransportError) -> &'static str {
    match error {
        TransportError::Io { .. } => "io",
        TransportError::FrameTooLarge { .. } => "frame_too_large",
        TransportError::InvalidFrameSize(_) => "invalid_frame_size",
        TransportError::Truncated => "truncated",
        TransportError::InvalidHeader(_) => "invalid_header",
        TransportError::InvalidUtf8 => "invalid_utf8",
        TransportError::UnsupportedApiKey(_) => "unsupported_api_key",
        TransportError::UnsupportedApiVersion { .. } => "unsupported_api_version",
        TransportError::Protocol(_) => "protocol",
        TransportError::Broker(_) => "broker",
        TransportError::Security(_) => "security",
        TransportError::Metrics(_) => "metrics",
        TransportError::Tls(_) => "tls",
    }
}

fn map_partition_error(err: &PartitionedBrokerError) -> i16 {
    match err {
        PartitionedBrokerError::InvalidTopic { .. } => ERROR_INVALID_TOPIC_EXCEPTION,
        PartitionedBrokerError::InvalidPartition { .. }
        | PartitionedBrokerError::UnknownPartition { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        PartitionedBrokerError::Storage(_) => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

pub(crate) fn map_fetch_partition_error(err: &PartitionedBrokerError) -> i16 {
    match err {
        PartitionedBrokerError::Storage(StorageError::OffsetOutOfRange { .. }) => {
            ERROR_OFFSET_OUT_OF_RANGE
        }
        other => map_partition_error(other),
    }
}

fn map_offset_coordinator_error(err: OffsetCoordinatorError) -> TransportError {
    match err {
        OffsetCoordinatorError::Io {
            operation, message, ..
        } => TransportError::Io { operation, message },
        other => TransportError::InvalidHeader(match other {
            OffsetCoordinatorError::Group(_) => "offset coordinator group error",
            OffsetCoordinatorError::InvalidGroupId => "offset coordinator invalid group id",
            OffsetCoordinatorError::InvalidTopic => "offset coordinator invalid topic",
            OffsetCoordinatorError::InvalidPartition(_) => "offset coordinator invalid partition",
            OffsetCoordinatorError::GroupIdNotFound => "offset coordinator group id not found",
            OffsetCoordinatorError::UnknownMemberId(_) => "offset coordinator unknown member id",
            OffsetCoordinatorError::IllegalGeneration => "offset coordinator illegal generation",
            OffsetCoordinatorError::StaleMemberEpoch => "offset coordinator stale member epoch",
            OffsetCoordinatorError::Io { .. } => unreachable!(),
        }),
    }
}

fn map_offset_commit_error(api_version: i16, err: &OffsetCoordinatorError) -> i16 {
    match err {
        OffsetCoordinatorError::InvalidGroupId => {
            if api_version >= 9 {
                ERROR_GROUP_ID_NOT_FOUND
            } else {
                ERROR_ILLEGAL_GENERATION
            }
        }
        OffsetCoordinatorError::InvalidTopic => {
            if api_version >= 10 {
                ERROR_UNKNOWN_TOPIC_ID
            } else {
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            }
        }
        OffsetCoordinatorError::InvalidPartition(_) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        OffsetCoordinatorError::GroupIdNotFound => ERROR_GROUP_ID_NOT_FOUND,
        OffsetCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        OffsetCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        OffsetCoordinatorError::StaleMemberEpoch => ERROR_STALE_MEMBER_EPOCH,
        OffsetCoordinatorError::Group(_) | OffsetCoordinatorError::Io { .. } => {
            ERROR_UNKNOWN_SERVER_ERROR
        }
    }
}

fn map_offset_fetch_group_error(err: &OffsetCoordinatorError) -> i16 {
    match err {
        OffsetCoordinatorError::InvalidGroupId | OffsetCoordinatorError::GroupIdNotFound => {
            ERROR_GROUP_ID_NOT_FOUND
        }
        OffsetCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        OffsetCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        OffsetCoordinatorError::StaleMemberEpoch => ERROR_STALE_MEMBER_EPOCH,
        OffsetCoordinatorError::InvalidTopic
        | OffsetCoordinatorError::InvalidPartition(_)
        | OffsetCoordinatorError::Group(_)
        | OffsetCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_transaction_coordinator_error(err: TransactionCoordinatorError) -> TransportError {
    match err {
        TransactionCoordinatorError::Io {
            operation, message, ..
        } => TransportError::Io { operation, message },
        _ => TransportError::InvalidHeader("transaction coordinator error"),
    }
}

fn map_transaction_error(err: &TransactionCoordinatorError) -> i16 {
    match err {
        TransactionCoordinatorError::InvalidTransactionalId => ERROR_INVALID_REQUEST,
        TransactionCoordinatorError::UnknownTransactionalId(_) => ERROR_INVALID_TXN_STATE,
        TransactionCoordinatorError::UnknownProducerId(_) => ERROR_UNKNOWN_PRODUCER_ID,
        TransactionCoordinatorError::InvalidProducerEpoch => ERROR_INVALID_PRODUCER_EPOCH,
        TransactionCoordinatorError::ProducerFenced => ERROR_PRODUCER_FENCED,
        TransactionCoordinatorError::InvalidTxnState => ERROR_INVALID_TXN_STATE,
        TransactionCoordinatorError::ConcurrentTransactions => ERROR_CONCURRENT_TRANSACTIONS,
        TransactionCoordinatorError::InvalidTopic => ERROR_INVALID_TOPIC_EXCEPTION,
        TransactionCoordinatorError::InvalidPartition(_) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        TransactionCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_classic_group_error(err: ClassicGroupCoordinatorError) -> TransportError {
    match err {
        ClassicGroupCoordinatorError::Io {
            operation, message, ..
        } => TransportError::Io { operation, message },
        _ => TransportError::InvalidHeader("classic group coordinator error"),
    }
}

fn map_join_group_error(err: &ClassicGroupCoordinatorError) -> i16 {
    match err {
        ClassicGroupCoordinatorError::InvalidGroupId => ERROR_INVALID_GROUP_ID,
        ClassicGroupCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        ClassicGroupCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        ClassicGroupCoordinatorError::InconsistentGroupProtocol => {
            ERROR_INCONSISTENT_GROUP_PROTOCOL
        }
        ClassicGroupCoordinatorError::MemberIdRequired(_) => ERROR_MEMBER_ID_REQUIRED,
        ClassicGroupCoordinatorError::RebalanceInProgress => ERROR_REBALANCE_IN_PROGRESS,
        ClassicGroupCoordinatorError::FencedInstanceId => ERROR_FENCED_INSTANCE_ID,
        ClassicGroupCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_sync_group_error(err: &ClassicGroupCoordinatorError) -> i16 {
    match err {
        ClassicGroupCoordinatorError::InvalidGroupId => ERROR_INVALID_GROUP_ID,
        ClassicGroupCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        ClassicGroupCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        ClassicGroupCoordinatorError::InconsistentGroupProtocol => {
            ERROR_INCONSISTENT_GROUP_PROTOCOL
        }
        ClassicGroupCoordinatorError::MemberIdRequired(_) => ERROR_MEMBER_ID_REQUIRED,
        ClassicGroupCoordinatorError::RebalanceInProgress => ERROR_REBALANCE_IN_PROGRESS,
        ClassicGroupCoordinatorError::FencedInstanceId => ERROR_FENCED_INSTANCE_ID,
        ClassicGroupCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_heartbeat_error(err: &ClassicGroupCoordinatorError) -> i16 {
    match err {
        ClassicGroupCoordinatorError::InvalidGroupId => ERROR_INVALID_GROUP_ID,
        ClassicGroupCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        ClassicGroupCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        ClassicGroupCoordinatorError::InconsistentGroupProtocol => {
            ERROR_INCONSISTENT_GROUP_PROTOCOL
        }
        ClassicGroupCoordinatorError::MemberIdRequired(_) => ERROR_MEMBER_ID_REQUIRED,
        ClassicGroupCoordinatorError::RebalanceInProgress => ERROR_REBALANCE_IN_PROGRESS,
        ClassicGroupCoordinatorError::FencedInstanceId => ERROR_FENCED_INSTANCE_ID,
        ClassicGroupCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_leave_group_error(err: &ClassicGroupCoordinatorError) -> i16 {
    match err {
        ClassicGroupCoordinatorError::InvalidGroupId => ERROR_INVALID_GROUP_ID,
        ClassicGroupCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        ClassicGroupCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        ClassicGroupCoordinatorError::InconsistentGroupProtocol => {
            ERROR_INCONSISTENT_GROUP_PROTOCOL
        }
        ClassicGroupCoordinatorError::MemberIdRequired(_) => ERROR_MEMBER_ID_REQUIRED,
        ClassicGroupCoordinatorError::RebalanceInProgress => ERROR_REBALANCE_IN_PROGRESS,
        ClassicGroupCoordinatorError::FencedInstanceId => ERROR_FENCED_INSTANCE_ID,
        ClassicGroupCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn map_leave_group_member_error(err: &ClassicGroupCoordinatorError) -> i16 {
    match err {
        ClassicGroupCoordinatorError::InvalidGroupId => ERROR_INVALID_GROUP_ID,
        ClassicGroupCoordinatorError::UnknownMemberId(_) => ERROR_UNKNOWN_MEMBER_ID,
        ClassicGroupCoordinatorError::IllegalGeneration => ERROR_ILLEGAL_GENERATION,
        ClassicGroupCoordinatorError::InconsistentGroupProtocol => {
            ERROR_INCONSISTENT_GROUP_PROTOCOL
        }
        ClassicGroupCoordinatorError::MemberIdRequired(_) => ERROR_MEMBER_ID_REQUIRED,
        ClassicGroupCoordinatorError::RebalanceInProgress => ERROR_REBALANCE_IN_PROGRESS,
        ClassicGroupCoordinatorError::FencedInstanceId => ERROR_FENCED_INSTANCE_ID,
        ClassicGroupCoordinatorError::Io { .. } => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

fn offset_topic_from_commit_request(
    api_version: i16,
    topic: &rafka_protocol::messages::OffsetCommitRequestOffsetCommitRequestTopic,
) -> Result<OffsetTopic, i16> {
    if api_version <= 9 {
        let Some(name) = topic.name.as_deref() else {
            return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        };
        if name.is_empty() {
            return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        }
        return Ok(OffsetTopic::by_name(name.to_string()));
    }

    let Some(topic_id) = topic.topic_id else {
        return Err(ERROR_UNKNOWN_TOPIC_ID);
    };
    if topic_id == [0_u8; 16] {
        return Err(ERROR_UNKNOWN_TOPIC_ID);
    }
    Ok(OffsetTopic::by_id(topic_id))
}

fn offset_topic_from_fetch_request(
    api_version: i16,
    topic_name: Option<&str>,
    topic_id: Option<[u8; 16]>,
) -> Result<OffsetTopic, i16> {
    if api_version <= 9 {
        let Some(name) = topic_name else {
            return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        };
        if name.is_empty() {
            return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        }
        return Ok(OffsetTopic::by_name(name.to_string()));
    }

    let Some(topic_id) = topic_id else {
        return Err(ERROR_UNKNOWN_TOPIC_ID);
    };
    if topic_id == [0_u8; 16] {
        return Err(ERROR_UNKNOWN_TOPIC_ID);
    }
    Ok(OffsetTopic::by_id(topic_id))
}

fn offset_topic_resource_name(topic: &OffsetTopic) -> String {
    if let Some(name) = &topic.name {
        return name.clone();
    }
    if let Some(topic_id) = topic.topic_id {
        return topic_id_to_hex(topic_id);
    }
    String::new()
}

fn default_offset_fetch_partition_legacy(
    version: i16,
    partition_index: i32,
    error_code: i16,
) -> OffsetFetchResponseOffsetFetchResponsePartition {
    OffsetFetchResponseOffsetFetchResponsePartition {
        partition_index,
        committed_offset: -1,
        committed_leader_epoch: (version >= 5).then_some(-1),
        metadata: Some(String::new()),
        error_code,
    }
}

fn default_offset_fetch_partition_grouped(
    partition_index: i32,
    error_code: i16,
) -> OffsetFetchResponseOffsetFetchResponsePartitions {
    OffsetFetchResponseOffsetFetchResponsePartitions {
        partition_index,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code,
    }
}

fn encode_offset_fetch_topics_legacy<F>(
    version: i16,
    offsets: &OffsetCoordinator,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: Option<i32>,
    topics: &[rafka_protocol::messages::OffsetFetchRequestOffsetFetchRequestTopic],
    mut is_authorized: F,
) -> Result<Vec<OffsetFetchResponseOffsetFetchResponseTopic>, OffsetCoordinatorError>
where
    F: FnMut(&OffsetTopic) -> bool,
{
    let mut response_topics = Vec::with_capacity(topics.len());
    for topic in topics {
        let partition_count = topic.partition_indexes.len();
        let mut partitions = Vec::with_capacity(partition_count);
        let topic_key = match offset_topic_from_fetch_request(version, Some(&topic.name), None) {
            Ok(topic_key) => topic_key,
            Err(error_code) => {
                for partition_index in &topic.partition_indexes {
                    partitions.push(default_offset_fetch_partition_legacy(
                        version,
                        *partition_index,
                        error_code,
                    ));
                }
                response_topics.push(OffsetFetchResponseOffsetFetchResponseTopic {
                    name: topic.name.clone(),
                    partitions,
                });
                continue;
            }
        };
        if !is_authorized(&topic_key) {
            for partition_index in &topic.partition_indexes {
                partitions.push(default_offset_fetch_partition_legacy(
                    version,
                    *partition_index,
                    ERROR_TOPIC_AUTHORIZATION_FAILED,
                ));
            }
            response_topics.push(OffsetFetchResponseOffsetFetchResponseTopic {
                name: topic.name.clone(),
                partitions,
            });
            continue;
        }

        for partition_index in &topic.partition_indexes {
            if *partition_index < 0 {
                partitions.push(default_offset_fetch_partition_legacy(
                    version,
                    *partition_index,
                    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            }

            let committed = offsets.fetch_offset(
                group_id,
                member_id,
                member_epoch,
                &topic_key,
                *partition_index,
            )?;
            if let Some(committed) = committed {
                partitions.push(OffsetFetchResponseOffsetFetchResponsePartition {
                    partition_index: *partition_index,
                    committed_offset: committed.committed_offset,
                    committed_leader_epoch: (version >= 5)
                        .then_some(committed.committed_leader_epoch),
                    metadata: committed.metadata,
                    error_code: ERROR_NONE,
                });
            } else {
                partitions.push(default_offset_fetch_partition_legacy(
                    version,
                    *partition_index,
                    ERROR_NONE,
                ));
            }
        }

        response_topics.push(OffsetFetchResponseOffsetFetchResponseTopic {
            name: topic.name.clone(),
            partitions,
        });
    }
    Ok(response_topics)
}

fn encode_offset_fetch_topics_legacy_all<F>(
    version: i16,
    offsets: &OffsetCoordinator,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: Option<i32>,
    mut is_authorized: F,
) -> Result<Vec<OffsetFetchResponseOffsetFetchResponseTopic>, OffsetCoordinatorError>
where
    F: FnMut(&OffsetTopic) -> bool,
{
    let entries = offsets.fetch_group_offsets(group_id, member_id, member_epoch)?;
    let mut grouped: BTreeMap<String, Vec<OffsetFetchResponseOffsetFetchResponsePartition>> =
        BTreeMap::new();
    for entry in entries {
        if !is_authorized(&entry.topic) {
            continue;
        }
        let Some(name) = entry.topic.name else {
            continue;
        };
        grouped
            .entry(name)
            .or_default()
            .push(OffsetFetchResponseOffsetFetchResponsePartition {
                partition_index: entry.partition,
                committed_offset: entry.value.committed_offset,
                committed_leader_epoch: (version >= 5)
                    .then_some(entry.value.committed_leader_epoch),
                metadata: entry.value.metadata,
                error_code: ERROR_NONE,
            });
    }

    let mut response = Vec::with_capacity(grouped.len());
    for (name, partitions) in grouped {
        response.push(OffsetFetchResponseOffsetFetchResponseTopic { name, partitions });
    }
    Ok(response)
}

fn encode_offset_fetch_topics_grouped<F>(
    version: i16,
    offsets: &OffsetCoordinator,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: Option<i32>,
    topics: &[rafka_protocol::messages::OffsetFetchRequestOffsetFetchRequestTopics],
    mut is_authorized: F,
) -> Result<Vec<OffsetFetchResponseOffsetFetchResponseTopics>, OffsetCoordinatorError>
where
    F: FnMut(&OffsetTopic) -> bool,
{
    offsets.validate_fetch_group(group_id, member_id, member_epoch)?;

    let mut response_topics = Vec::with_capacity(topics.len());
    for topic in topics {
        let mut partitions = Vec::with_capacity(topic.partition_indexes.len());
        let topic_key =
            match offset_topic_from_fetch_request(version, topic.name.as_deref(), topic.topic_id) {
                Ok(topic_key) => topic_key,
                Err(error_code) => {
                    for partition_index in &topic.partition_indexes {
                        partitions.push(default_offset_fetch_partition_grouped(
                            *partition_index,
                            error_code,
                        ));
                    }
                    response_topics.push(OffsetFetchResponseOffsetFetchResponseTopics {
                        name: topic.name.clone(),
                        topic_id: topic.topic_id,
                        partitions,
                    });
                    continue;
                }
            };
        if !is_authorized(&topic_key) {
            for partition_index in &topic.partition_indexes {
                partitions.push(default_offset_fetch_partition_grouped(
                    *partition_index,
                    ERROR_TOPIC_AUTHORIZATION_FAILED,
                ));
            }
            response_topics.push(OffsetFetchResponseOffsetFetchResponseTopics {
                name: topic.name.clone(),
                topic_id: topic.topic_id,
                partitions,
            });
            continue;
        }

        for partition_index in &topic.partition_indexes {
            if *partition_index < 0 {
                partitions.push(default_offset_fetch_partition_grouped(
                    *partition_index,
                    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            }
            let committed = offsets.fetch_offset(
                group_id,
                member_id,
                member_epoch,
                &topic_key,
                *partition_index,
            )?;
            if let Some(committed) = committed {
                partitions.push(OffsetFetchResponseOffsetFetchResponsePartitions {
                    partition_index: *partition_index,
                    committed_offset: committed.committed_offset,
                    committed_leader_epoch: committed.committed_leader_epoch,
                    metadata: committed.metadata,
                    error_code: ERROR_NONE,
                });
            } else {
                partitions.push(default_offset_fetch_partition_grouped(
                    *partition_index,
                    ERROR_NONE,
                ));
            }
        }

        response_topics.push(OffsetFetchResponseOffsetFetchResponseTopics {
            name: topic.name.clone(),
            topic_id: topic.topic_id,
            partitions,
        });
    }
    Ok(response_topics)
}

fn encode_offset_fetch_topics_grouped_all<F>(
    version: i16,
    offsets: &OffsetCoordinator,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: Option<i32>,
    mut is_authorized: F,
) -> Result<Vec<OffsetFetchResponseOffsetFetchResponseTopics>, OffsetCoordinatorError>
where
    F: FnMut(&OffsetTopic) -> bool,
{
    let entries = offsets.fetch_group_offsets(group_id, member_id, member_epoch)?;
    #[derive(Default)]
    struct TopicBuckets {
        name: Option<String>,
        topic_id: Option<[u8; 16]>,
        partitions: Vec<OffsetFetchResponseOffsetFetchResponsePartitions>,
    }

    let mut grouped: BTreeMap<String, TopicBuckets> = BTreeMap::new();
    for entry in entries {
        if !is_authorized(&entry.topic) {
            continue;
        }
        let key = if let Some(name) = &entry.topic.name {
            format!("name:{name}")
        } else if let Some(topic_id) = entry.topic.topic_id {
            format!("id:{}", topic_id_to_hex(topic_id))
        } else {
            continue;
        };

        let bucket = grouped.entry(key).or_default();
        if bucket.name.is_none() {
            bucket.name = entry.topic.name.clone();
        }
        if bucket.topic_id.is_none() {
            bucket.topic_id = entry.topic.topic_id;
        }
        bucket
            .partitions
            .push(OffsetFetchResponseOffsetFetchResponsePartitions {
                partition_index: entry.partition,
                committed_offset: entry.value.committed_offset,
                committed_leader_epoch: entry.value.committed_leader_epoch,
                metadata: entry.value.metadata,
                error_code: ERROR_NONE,
            });
    }

    let mut response = Vec::with_capacity(grouped.len());
    for (_, bucket) in grouped {
        if version <= 9 && bucket.name.is_none() {
            continue;
        }
        if version >= 10 && bucket.topic_id.is_none() {
            continue;
        }
        response.push(OffsetFetchResponseOffsetFetchResponseTopics {
            name: bucket.name,
            topic_id: bucket.topic_id,
            partitions: bucket.partitions,
        });
    }
    Ok(response)
}

fn decode_init_producer_id_request(
    version: i16,
    input: &[u8],
) -> Result<(InitProducerIdRequestBody, usize), TransportError> {
    let flexible = version >= 2;
    let mut cursor = 0;

    let transactional_id = read_nullable_string(input, &mut cursor, flexible)?;
    let transaction_timeout_ms = read_i32(input, &mut cursor)?;
    let producer_id = if version >= 3 {
        read_i64(input, &mut cursor)?
    } else {
        -1
    };
    let producer_epoch = if version >= 3 {
        read_i16(input, &mut cursor)?
    } else {
        -1
    };
    let enable_2pc = if version >= 6 {
        read_bool(input, &mut cursor)?
    } else {
        false
    };
    let keep_prepared_txn = if version >= 6 {
        read_bool(input, &mut cursor)?
    } else {
        false
    };
    if flexible {
        skip_tagged_fields(input, &mut cursor)?;
    }

    Ok((
        InitProducerIdRequestBody {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable_2pc,
            keep_prepared_txn,
        },
        cursor,
    ))
}

fn encode_init_producer_id_response(
    version: i16,
    response: &InitProducerIdResponseBody,
) -> Result<Vec<u8>, TransportError> {
    let flexible = version >= 2;
    let mut out = Vec::new();

    out.extend_from_slice(&response.throttle_time_ms.to_be_bytes());
    out.extend_from_slice(&response.error_code.to_be_bytes());
    out.extend_from_slice(&response.producer_id.to_be_bytes());
    out.extend_from_slice(&response.producer_epoch.to_be_bytes());
    if version >= 6 {
        out.extend_from_slice(&response.ongoing_txn_producer_id.to_be_bytes());
        out.extend_from_slice(&response.ongoing_txn_producer_epoch.to_be_bytes());
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    Ok(out)
}

fn decode_end_txn_request(
    version: i16,
    input: &[u8],
) -> Result<(EndTxnRequestBody, usize), TransportError> {
    let flexible = version >= 3;
    let mut cursor = 0;

    let transactional_id = read_string(input, &mut cursor, flexible)?;
    let producer_id = read_i64(input, &mut cursor)?;
    let producer_epoch = read_i16(input, &mut cursor)?;
    let committed = read_bool(input, &mut cursor)?;
    if flexible {
        skip_tagged_fields(input, &mut cursor)?;
    }

    Ok((
        EndTxnRequestBody {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        },
        cursor,
    ))
}

fn encode_end_txn_response(
    version: i16,
    response: &EndTxnResponseBody,
) -> Result<Vec<u8>, TransportError> {
    let flexible = version >= 3;
    let mut out = Vec::new();

    out.extend_from_slice(&response.throttle_time_ms.to_be_bytes());
    out.extend_from_slice(&response.error_code.to_be_bytes());
    if version >= 5 {
        out.extend_from_slice(&response.producer_id.to_be_bytes());
        out.extend_from_slice(&response.producer_epoch.to_be_bytes());
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    Ok(out)
}

fn decode_write_txn_markers_request(
    version: i16,
    input: &[u8],
) -> Result<(WriteTxnMarkersRequestBody, usize), TransportError> {
    let flexible = version >= 1;
    let mut cursor = 0;

    let marker_count = read_array_len(input, &mut cursor, flexible)?;
    let mut markers = Vec::with_capacity(marker_count);
    for _ in 0..marker_count {
        let producer_id = read_i64(input, &mut cursor)?;
        let producer_epoch = read_i16(input, &mut cursor)?;
        let transaction_result = read_bool(input, &mut cursor)?;

        let topic_count = read_array_len(input, &mut cursor, flexible)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(input, &mut cursor, flexible)?;
            let partition_count = read_array_len(input, &mut cursor, flexible)?;
            let mut partition_indexes = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partition_indexes.push(read_i32(input, &mut cursor)?);
            }
            if flexible {
                skip_tagged_fields(input, &mut cursor)?;
            }
            topics.push(WriteTxnMarkersRequestTopicBody {
                name,
                partition_indexes,
            });
        }

        let coordinator_epoch = read_i32(input, &mut cursor)?;
        let transaction_version = if version >= 2 {
            read_i8(input, &mut cursor)?
        } else {
            0
        };
        if flexible {
            skip_tagged_fields(input, &mut cursor)?;
        }

        markers.push(WriteTxnMarkersRequestMarkerBody {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
            transaction_version,
        });
    }

    if flexible {
        skip_tagged_fields(input, &mut cursor)?;
    }

    Ok((WriteTxnMarkersRequestBody { markers }, cursor))
}

fn encode_write_txn_markers_response(
    version: i16,
    response: &WriteTxnMarkersResponseBody,
) -> Result<Vec<u8>, TransportError> {
    let flexible = version >= 1;
    let mut out = Vec::new();

    write_array_len(&mut out, response.markers.len(), flexible)?;
    for marker in &response.markers {
        out.extend_from_slice(&marker.producer_id.to_be_bytes());
        write_array_len(&mut out, marker.topics.len(), flexible)?;
        for topic in &marker.topics {
            write_string(&mut out, &topic.name, flexible)?;
            write_array_len(&mut out, topic.partitions.len(), flexible)?;
            for partition in &topic.partitions {
                out.extend_from_slice(&partition.partition_index.to_be_bytes());
                out.extend_from_slice(&partition.error_code.to_be_bytes());
                if flexible {
                    write_unsigned_varint(&mut out, 0);
                }
            }
            if flexible {
                write_unsigned_varint(&mut out, 0);
            }
        }
        if flexible {
            write_unsigned_varint(&mut out, 0);
        }
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }

    Ok(out)
}

pub(crate) fn encode_produce_response(
    version: i16,
    topics: &[ProduceTopicResponse],
    throttle_time_ms: i32,
) -> Result<Vec<u8>, TransportError> {
    let flexible = version >= 9;
    let mut out = Vec::new();

    write_array_len(&mut out, topics.len(), flexible)?;
    for topic in topics {
        if version <= 12 {
            let name = topic.name.as_deref().ok_or(TransportError::InvalidHeader(
                "produce response missing topic name",
            ))?;
            write_string(&mut out, name, flexible)?;
        }
        if version >= 13 {
            let topic_id = topic.topic_id.ok_or(TransportError::InvalidHeader(
                "produce response missing topic id",
            ))?;
            out.extend_from_slice(&topic_id);
        }

        write_array_len(&mut out, topic.partitions.len(), flexible)?;
        for partition in &topic.partitions {
            out.extend_from_slice(&partition.index.to_be_bytes());
            out.extend_from_slice(&partition.error_code.to_be_bytes());
            out.extend_from_slice(&partition.base_offset.to_be_bytes());

            if version >= 2 {
                out.extend_from_slice(&(-1_i64).to_be_bytes());
            }
            if version >= 5 {
                out.extend_from_slice(&partition.log_start_offset.to_be_bytes());
            }
            if version >= 8 {
                write_array_len(&mut out, 0, flexible)?;
                write_nullable_string(&mut out, None, flexible)?;
            }
            if flexible {
                write_unsigned_varint(&mut out, 0);
            }
        }

        if flexible {
            write_unsigned_varint(&mut out, 0);
        }
    }

    if version >= 1 {
        out.extend_from_slice(&throttle_time_ms.to_be_bytes());
    }

    if flexible {
        write_unsigned_varint(&mut out, 0);
    }

    Ok(out)
}

pub(crate) fn decode_fetch_request(
    version: i16,
    input: &[u8],
) -> Result<(FetchRequestBody, usize), TransportError> {
    let flexible = version >= 12;
    let mut cursor = 0;

    let replica_id = if version <= 14 {
        read_i32(input, &mut cursor)?
    } else {
        -1
    };
    let mut replica_state_replica_id: Option<i32> = None;
    let mut replica_state_epoch: Option<i64> = None;
    let mut cluster_id: Option<String> = None;

    let _max_wait_ms = read_i32(input, &mut cursor)?;
    let _min_bytes = read_i32(input, &mut cursor)?;
    let _max_bytes = read_i32(input, &mut cursor)?;
    let isolation_level = read_i8(input, &mut cursor)?;
    if version >= 7 {
        let _session_id = read_i32(input, &mut cursor)?;
        let _session_epoch = read_i32(input, &mut cursor)?;
    }

    let topic_count = read_array_len(input, &mut cursor, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = if version <= 12 {
            Some(read_string(input, &mut cursor, flexible)?)
        } else {
            None
        };
        let topic_id = if version >= 13 {
            Some(read_uuid(input, &mut cursor)?)
        } else {
            None
        };

        let partition_count = read_array_len(input, &mut cursor, flexible)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let partition = read_i32(input, &mut cursor)?;
            if version >= 9 {
                let _current_leader_epoch = read_i32(input, &mut cursor)?;
            }
            let fetch_offset = read_i64(input, &mut cursor)?;
            if version >= 12 {
                let _last_fetched_epoch = read_i32(input, &mut cursor)?;
            }
            if version >= 5 {
                let _log_start_offset = read_i64(input, &mut cursor)?;
            }
            let partition_max_bytes = read_i32(input, &mut cursor)?;

            let mut replica_directory_id = None;
            let mut high_watermark = None;
            if flexible {
                read_tagged_fields(input, &mut cursor, |tag, payload| {
                    match tag {
                        0 if version >= 17 => {
                            let mut payload_cursor = 0;
                            replica_directory_id = Some(read_uuid(payload, &mut payload_cursor)?);
                            if payload_cursor != payload.len() {
                                return Err(TransportError::InvalidHeader(
                                    "replica directory id tag payload has trailing bytes",
                                ));
                            }
                        }
                        1 if version >= 18 => {
                            let mut payload_cursor = 0;
                            high_watermark = Some(read_i64(payload, &mut payload_cursor)?);
                            if payload_cursor != payload.len() {
                                return Err(TransportError::InvalidHeader(
                                    "high watermark tag payload has trailing bytes",
                                ));
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                })?;
            }

            partitions.push(FetchPartitionRequest {
                partition,
                fetch_offset,
                partition_max_bytes,
                replica_directory_id,
                high_watermark,
            });
        }

        if flexible {
            skip_tagged_fields(input, &mut cursor)?;
        }

        topics.push(FetchTopicRequest {
            name,
            topic_id,
            partitions,
        });
    }

    if version >= 7 {
        let forgotten_count = read_array_len(input, &mut cursor, flexible)?;
        for _ in 0..forgotten_count {
            if version <= 12 {
                let _topic = read_string(input, &mut cursor, flexible)?;
            } else {
                let _topic_id = read_uuid(input, &mut cursor)?;
            }
            let forgotten_partition_count = read_array_len(input, &mut cursor, flexible)?;
            for _ in 0..forgotten_partition_count {
                let _partition = read_i32(input, &mut cursor)?;
            }
            if flexible {
                skip_tagged_fields(input, &mut cursor)?;
            }
        }
    }

    if version >= 11 {
        let _rack_id = read_string(input, &mut cursor, flexible)?;
    }

    if flexible {
        read_tagged_fields(input, &mut cursor, |tag, payload| {
            match tag {
                0 if version >= 12 => {
                    let mut payload_cursor = 0;
                    cluster_id = read_nullable_string(payload, &mut payload_cursor, true)?;
                    if payload_cursor != payload.len() {
                        return Err(TransportError::InvalidHeader(
                            "cluster id tag payload has trailing bytes",
                        ));
                    }
                }
                1 if version >= 15 => {
                    let mut payload_cursor = 0;
                    replica_state_replica_id = Some(read_i32(payload, &mut payload_cursor)?);
                    replica_state_epoch = Some(read_i64(payload, &mut payload_cursor)?);
                    if payload_cursor != payload.len() {
                        return Err(TransportError::InvalidHeader(
                            "replica state tag payload has trailing bytes",
                        ));
                    }
                }
                _ => {}
            }
            Ok(())
        })?;
    }

    Ok((
        FetchRequestBody {
            replica_id: replica_state_replica_id.unwrap_or(replica_id),
            replica_epoch: replica_state_epoch,
            cluster_id,
            isolation_level,
            topics,
        },
        cursor,
    ))
}

pub(crate) fn encode_fetch_response(
    version: i16,
    throttle_time_ms: i32,
    top_level_error_code: i16,
    session_id: i32,
    topics: &[FetchTopicResponse],
    node_endpoints: &[FetchNodeEndpointResponse],
) -> Result<Vec<u8>, TransportError> {
    let flexible = version >= 12;
    let mut out = Vec::new();

    if version >= 1 {
        out.extend_from_slice(&throttle_time_ms.to_be_bytes());
    }
    if version >= 7 {
        out.extend_from_slice(&top_level_error_code.to_be_bytes());
        out.extend_from_slice(&session_id.to_be_bytes());
    }

    write_array_len(&mut out, topics.len(), flexible)?;
    for topic in topics {
        if version <= 12 {
            let name = topic.name.as_deref().ok_or(TransportError::InvalidHeader(
                "fetch response missing topic name",
            ))?;
            write_string(&mut out, name, flexible)?;
        }
        if version >= 13 {
            let topic_id = topic.topic_id.ok_or(TransportError::InvalidHeader(
                "fetch response missing topic id",
            ))?;
            out.extend_from_slice(&topic_id);
        }

        write_array_len(&mut out, topic.partitions.len(), flexible)?;
        for partition in &topic.partitions {
            out.extend_from_slice(&partition.partition_index.to_be_bytes());
            out.extend_from_slice(&partition.error_code.to_be_bytes());
            out.extend_from_slice(&partition.high_watermark.to_be_bytes());
            if version >= 4 {
                out.extend_from_slice(&partition.last_stable_offset.to_be_bytes());
            }
            if version >= 5 {
                out.extend_from_slice(&partition.log_start_offset.to_be_bytes());
            }
            if version >= 4 {
                write_nullable_array_len(
                    &mut out,
                    partition.aborted_transactions.as_ref().map(Vec::len),
                    flexible,
                )?;
                if let Some(aborted_transactions) = partition.aborted_transactions.as_ref() {
                    for aborted in aborted_transactions {
                        out.extend_from_slice(&aborted.producer_id.to_be_bytes());
                        out.extend_from_slice(&aborted.first_offset.to_be_bytes());
                        if flexible {
                            write_unsigned_varint(&mut out, 0);
                        }
                    }
                }
            }
            if version >= 11 {
                out.extend_from_slice(&partition.preferred_read_replica.to_be_bytes());
            }
            write_nullable_bytes(&mut out, partition.records.as_deref(), flexible)?;
            if flexible {
                write_unsigned_varint(&mut out, 0);
            }
        }
        if flexible {
            write_unsigned_varint(&mut out, 0);
        }
    }

    if flexible {
        let mut tagged_fields: Vec<(u32, Vec<u8>)> = Vec::new();
        if version >= 16 && !node_endpoints.is_empty() {
            let mut payload = Vec::new();
            write_array_len(&mut payload, node_endpoints.len(), true)?;
            for endpoint in node_endpoints {
                payload.extend_from_slice(&endpoint.node_id.to_be_bytes());
                write_string(&mut payload, &endpoint.host, true)?;
                payload.extend_from_slice(&endpoint.port.to_be_bytes());
                write_nullable_string(&mut payload, endpoint.rack.as_deref(), true)?;
                write_unsigned_varint(&mut payload, 0);
            }
            tagged_fields.push((0, payload));
        }
        write_tagged_fields(&mut out, tagged_fields)?;
    }

    Ok(out)
}

fn write_array_len(out: &mut Vec<u8>, len: usize, flexible: bool) -> Result<(), TransportError> {
    if flexible {
        let len_u32 = u32::try_from(len)
            .map_err(|_| TransportError::InvalidHeader("array length overflow"))?;
        write_unsigned_varint(out, len_u32.saturating_add(1));
    } else {
        let len_i32 = i32::try_from(len)
            .map_err(|_| TransportError::InvalidHeader("array length overflow"))?;
        out.extend_from_slice(&len_i32.to_be_bytes());
    }
    Ok(())
}

fn write_string(out: &mut Vec<u8>, value: &str, flexible: bool) -> Result<(), TransportError> {
    let bytes = value.as_bytes();
    if flexible {
        let len_u32 = u32::try_from(bytes.len())
            .map_err(|_| TransportError::InvalidHeader("string length overflow"))?;
        write_unsigned_varint(out, len_u32.saturating_add(1));
        out.extend_from_slice(bytes);
    } else {
        let len_i16 = i16::try_from(bytes.len())
            .map_err(|_| TransportError::InvalidHeader("string length overflow"))?;
        out.extend_from_slice(&len_i16.to_be_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(())
}

fn write_nullable_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
    flexible: bool,
) -> Result<(), TransportError> {
    match value {
        None => {
            if flexible {
                write_unsigned_varint(out, 0);
            } else {
                out.extend_from_slice(&(-1_i16).to_be_bytes());
            }
        }
        Some(value) => write_string(out, value, flexible)?,
    }
    Ok(())
}

fn write_nullable_bytes(
    out: &mut Vec<u8>,
    value: Option<&[u8]>,
    flexible: bool,
) -> Result<(), TransportError> {
    match value {
        Some(bytes) => {
            if flexible {
                let len_u32 = u32::try_from(bytes.len())
                    .map_err(|_| TransportError::InvalidHeader("bytes length overflow"))?;
                write_unsigned_varint(out, len_u32.saturating_add(1));
            } else {
                let len_i32 = i32::try_from(bytes.len())
                    .map_err(|_| TransportError::InvalidHeader("bytes length overflow"))?;
                out.extend_from_slice(&len_i32.to_be_bytes());
            }
            out.extend_from_slice(bytes);
        }
        None => {
            if flexible {
                write_unsigned_varint(out, 0);
            } else {
                out.extend_from_slice(&(-1_i32).to_be_bytes());
            }
        }
    }
    Ok(())
}

fn write_nullable_array_len(
    out: &mut Vec<u8>,
    len: Option<usize>,
    flexible: bool,
) -> Result<(), TransportError> {
    match len {
        Some(len) => write_array_len(out, len, flexible),
        None => {
            if flexible {
                write_unsigned_varint(out, 0);
            } else {
                out.extend_from_slice(&(-1_i32).to_be_bytes());
            }
            Ok(())
        }
    }
}

fn write_tagged_fields(
    out: &mut Vec<u8>,
    mut fields: Vec<(u32, Vec<u8>)>,
) -> Result<(), TransportError> {
    fields.sort_by_key(|(tag, _)| *tag);
    let count = u32::try_from(fields.len())
        .map_err(|_| TransportError::InvalidHeader("tagged field count overflow"))?;
    write_unsigned_varint(out, count);

    for (tag, payload) in fields {
        write_unsigned_varint(out, tag);
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| TransportError::InvalidHeader("tagged field payload length overflow"))?;
        write_unsigned_varint(out, payload_len);
        out.extend_from_slice(&payload);
    }

    Ok(())
}

fn read_array_len(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<usize, TransportError> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Err(TransportError::InvalidHeader(
                "compact array length cannot be 0",
            ));
        }
        return usize::try_from(raw - 1)
            .map_err(|_| TransportError::InvalidHeader("array length overflow"));
    }

    let len = read_i32(input, cursor)?;
    if len < 0 {
        return Err(TransportError::InvalidHeader("negative array length"));
    }
    usize::try_from(len).map_err(|_| TransportError::InvalidHeader("array length overflow"))
}

fn read_string(input: &[u8], cursor: &mut usize, flexible: bool) -> Result<String, TransportError> {
    let value = read_nullable_string(input, cursor, flexible)?;
    value.ok_or({
        if flexible {
            TransportError::InvalidHeader("compact string length cannot be 0")
        } else {
            TransportError::InvalidHeader("string cannot be null")
        }
    })
}

fn read_nullable_string(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<Option<String>, TransportError> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = usize::try_from(raw - 1)
            .map_err(|_| TransportError::InvalidHeader("string length overflow"))?;
        if input.len() < cursor.saturating_add(len) {
            return Err(TransportError::Truncated);
        }
        let bytes = &input[*cursor..*cursor + len];
        *cursor += len;
        return String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| TransportError::InvalidUtf8);
    }

    read_nullable_legacy_string(input, cursor)
}

#[cfg(test)]
fn read_nullable_bytes(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<Option<Vec<u8>>, TransportError> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = usize::try_from(raw - 1)
            .map_err(|_| TransportError::InvalidHeader("bytes length overflow"))?;
        if input.len() < cursor.saturating_add(len) {
            return Err(TransportError::Truncated);
        }
        let bytes = input[*cursor..*cursor + len].to_vec();
        *cursor += len;
        return Ok(Some(bytes));
    }

    let len = read_i32(input, cursor)?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(TransportError::InvalidHeader(
            "invalid nullable bytes length",
        ));
    }
    let len =
        usize::try_from(len).map_err(|_| TransportError::InvalidHeader("bytes length overflow"))?;
    if input.len() < cursor.saturating_add(len) {
        return Err(TransportError::Truncated);
    }
    let bytes = input[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(Some(bytes))
}

fn read_uuid(input: &[u8], cursor: &mut usize) -> Result<[u8; 16], TransportError> {
    if input.len() < cursor.saturating_add(16) {
        return Err(TransportError::Truncated);
    }
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&input[*cursor..*cursor + 16]);
    *cursor += 16;
    Ok(uuid)
}

fn skip_tagged_fields(input: &[u8], cursor: &mut usize) -> Result<(), TransportError> {
    read_tagged_fields(input, cursor, |_tag, _payload| Ok(()))
}

fn read_tagged_fields<F>(
    input: &[u8],
    cursor: &mut usize,
    mut on_field: F,
) -> Result<(), TransportError>
where
    F: FnMut(u32, &[u8]) -> Result<(), TransportError>,
{
    let num_fields = read_unsigned_varint(input, cursor)?;
    for _ in 0..num_fields {
        let tag = read_unsigned_varint(input, cursor)?;
        let size = read_unsigned_varint(input, cursor)?;
        let size_usize = usize::try_from(size)
            .map_err(|_| TransportError::InvalidHeader("tag size overflow"))?;
        if input.len() < cursor.saturating_add(size_usize) {
            return Err(TransportError::Truncated);
        }

        let payload_start = *cursor;
        let payload_end = payload_start + size_usize;
        *cursor = payload_end;
        on_field(tag, &input[payload_start..payload_end])?;
    }

    Ok(())
}

fn read_nullable_legacy_string(
    input: &[u8],
    cursor: &mut usize,
) -> Result<Option<String>, TransportError> {
    let length = read_i16(input, cursor)?;
    if length == -1 {
        return Ok(None);
    }
    if length < -1 {
        return Err(TransportError::InvalidHeader(
            "invalid nullable string length",
        ));
    }

    let length_usize = usize::try_from(length)
        .map_err(|_| TransportError::InvalidHeader("string length overflow"))?;
    if input.len() < cursor.saturating_add(length_usize) {
        return Err(TransportError::Truncated);
    }

    let value_bytes = &input[*cursor..*cursor + length_usize];
    *cursor += length_usize;
    let value = String::from_utf8(value_bytes.to_vec()).map_err(|_| TransportError::InvalidUtf8)?;
    Ok(Some(value))
}

fn read_unsigned_varint(input: &[u8], cursor: &mut usize) -> Result<u32, TransportError> {
    let mut value = 0_u32;
    let mut shift = 0_u32;

    for _ in 0..5 {
        let Some(&byte) = input.get(*cursor) else {
            return Err(TransportError::Truncated);
        };
        *cursor += 1;

        value |= u32::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
    }

    Err(TransportError::InvalidHeader("unsigned varint too long"))
}

fn write_unsigned_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_i16(input: &[u8], cursor: &mut usize) -> Result<i16, TransportError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(TransportError::Truncated);
    }
    let value = i16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_i8(input: &[u8], cursor: &mut usize) -> Result<i8, TransportError> {
    if input.len() < cursor.saturating_add(1) {
        return Err(TransportError::Truncated);
    }
    let value = i8::from_be_bytes([input[*cursor]]);
    *cursor += 1;
    Ok(value)
}

fn read_bool(input: &[u8], cursor: &mut usize) -> Result<bool, TransportError> {
    match read_i8(input, cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TransportError::InvalidHeader("invalid boolean value")),
    }
}

fn read_i32(input: &[u8], cursor: &mut usize) -> Result<i32, TransportError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(TransportError::Truncated);
    }
    let value = i32::from_be_bytes([
        input[*cursor],
        input[*cursor + 1],
        input[*cursor + 2],
        input[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

fn read_i64(input: &[u8], cursor: &mut usize) -> Result<i64, TransportError> {
    if input.len() < cursor.saturating_add(8) {
        return Err(TransportError::Truncated);
    }
    let value = i64::from_be_bytes([
        input[*cursor],
        input[*cursor + 1],
        input[*cursor + 2],
        input[*cursor + 3],
        input[*cursor + 4],
        input[*cursor + 5],
        input[*cursor + 6],
        input[*cursor + 7],
    ]);
    *cursor += 8;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_API_VERSIONS, 0).expect("v0"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_API_VERSIONS, 2).expect("v2"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_API_VERSIONS, 3).expect("v3"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_API_VERSIONS, 4).expect("v4"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_API_VERSIONS, 0).expect("v0 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_API_VERSIONS, 4).expect("v4 response"),
            0
        );
    }

    #[test]
    fn produce_header_versions_match_kafka_expectations() {
        assert_eq!(request_header_version(API_KEY_PRODUCE, 3).expect("v3"), 1);
        assert_eq!(request_header_version(API_KEY_PRODUCE, 8).expect("v8"), 1);
        assert_eq!(request_header_version(API_KEY_PRODUCE, 9).expect("v9"), 2);
        assert_eq!(request_header_version(API_KEY_PRODUCE, 13).expect("v13"), 2);

        assert_eq!(
            response_header_version(API_KEY_PRODUCE, 8).expect("v8 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_PRODUCE, 9).expect("v9 response"),
            1
        );
    }

    #[test]
    fn fetch_header_versions_match_kafka_expectations() {
        assert_eq!(request_header_version(API_KEY_FETCH, 4).expect("v4"), 1);
        assert_eq!(request_header_version(API_KEY_FETCH, 11).expect("v11"), 1);
        assert_eq!(request_header_version(API_KEY_FETCH, 12).expect("v12"), 2);
        assert_eq!(request_header_version(API_KEY_FETCH, 13).expect("v13"), 2);
        assert_eq!(request_header_version(API_KEY_FETCH, 18).expect("v18"), 2);

        assert_eq!(
            response_header_version(API_KEY_FETCH, 11).expect("v11 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_FETCH, 12).expect("v12 response"),
            1
        );
        assert_eq!(
            response_header_version(API_KEY_FETCH, 18).expect("v18 response"),
            1
        );
    }

    #[test]
    fn offset_commit_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_OFFSET_COMMIT, 2).expect("v2"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_COMMIT, 7).expect("v7"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_COMMIT, 8).expect("v8"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_COMMIT, 10).expect("v10"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_OFFSET_COMMIT, 7).expect("v7 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_OFFSET_COMMIT, 8).expect("v8 response"),
            1
        );
    }

    #[test]
    fn offset_fetch_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_OFFSET_FETCH, 1).expect("v1"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_FETCH, 5).expect("v5"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_FETCH, 6).expect("v6"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_OFFSET_FETCH, 10).expect("v10"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_OFFSET_FETCH, 5).expect("v5 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_OFFSET_FETCH, 6).expect("v6 response"),
            1
        );
    }

    #[test]
    fn join_group_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_JOIN_GROUP, 5).expect("v5"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_JOIN_GROUP, 6).expect("v6"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_JOIN_GROUP, 9).expect("v9"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_JOIN_GROUP, 5).expect("v5 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_JOIN_GROUP, 6).expect("v6 response"),
            1
        );
    }

    #[test]
    fn heartbeat_header_versions_match_kafka_expectations() {
        assert_eq!(request_header_version(API_KEY_HEARTBEAT, 3).expect("v3"), 1);
        assert_eq!(request_header_version(API_KEY_HEARTBEAT, 4).expect("v4"), 2);

        assert_eq!(
            response_header_version(API_KEY_HEARTBEAT, 3).expect("v3 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_HEARTBEAT, 4).expect("v4 response"),
            1
        );
    }

    #[test]
    fn leave_group_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_LEAVE_GROUP, 3).expect("v3"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_LEAVE_GROUP, 4).expect("v4"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_LEAVE_GROUP, 5).expect("v5"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_LEAVE_GROUP, 3).expect("v3 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_LEAVE_GROUP, 4).expect("v4 response"),
            1
        );
    }

    #[test]
    fn sync_group_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_SYNC_GROUP, 3).expect("v3"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_SYNC_GROUP, 4).expect("v4"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_SYNC_GROUP, 5).expect("v5"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_SYNC_GROUP, 3).expect("v3 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_SYNC_GROUP, 4).expect("v4 response"),
            1
        );
    }

    #[test]
    fn init_producer_id_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_INIT_PRODUCER_ID, 1).expect("v1"),
            1
        );
        assert_eq!(
            request_header_version(API_KEY_INIT_PRODUCER_ID, 2).expect("v2"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_INIT_PRODUCER_ID, 6).expect("v6"),
            2
        );

        assert_eq!(
            response_header_version(API_KEY_INIT_PRODUCER_ID, 1).expect("v1 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_INIT_PRODUCER_ID, 2).expect("v2 response"),
            1
        );
    }

    #[test]
    fn end_txn_header_versions_match_kafka_expectations() {
        assert_eq!(request_header_version(API_KEY_END_TXN, 2).expect("v2"), 1);
        assert_eq!(request_header_version(API_KEY_END_TXN, 3).expect("v3"), 2);
        assert_eq!(request_header_version(API_KEY_END_TXN, 5).expect("v5"), 2);

        assert_eq!(
            response_header_version(API_KEY_END_TXN, 2).expect("v2 response"),
            0
        );
        assert_eq!(
            response_header_version(API_KEY_END_TXN, 3).expect("v3 response"),
            1
        );
    }

    #[test]
    fn write_txn_markers_header_versions_match_kafka_expectations() {
        assert_eq!(
            request_header_version(API_KEY_WRITE_TXN_MARKERS, 1).expect("v1"),
            2
        );
        assert_eq!(
            request_header_version(API_KEY_WRITE_TXN_MARKERS, 2).expect("v2"),
            2
        );
        assert_eq!(
            response_header_version(API_KEY_WRITE_TXN_MARKERS, 1).expect("v1 response"),
            1
        );
        assert_eq!(
            response_header_version(API_KEY_WRITE_TXN_MARKERS, 2).expect("v2 response"),
            1
        );
    }

    #[test]
    fn read_and_write_unsigned_varint_roundtrip() {
        let values = [0_u32, 1, 127, 128, 16_383, 16_384, u32::MAX];
        for value in values {
            let mut bytes = Vec::new();
            write_unsigned_varint(&mut bytes, value);
            let mut cursor = 0;
            let decoded = read_unsigned_varint(&bytes, &mut cursor).expect("decode varint");
            assert_eq!(decoded, value);
            assert_eq!(cursor, bytes.len());
        }
    }

    #[test]
    fn decode_fetch_request_v18_reads_replica_state_and_partition_tags() {
        let version = 18;
        let mut body = Vec::new();

        body.extend_from_slice(&500_i32.to_be_bytes());
        body.extend_from_slice(&1_i32.to_be_bytes());
        body.extend_from_slice(&1_048_576_i32.to_be_bytes());
        body.push(0_u8);
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes());

        let topic_id = [
            0x10, 0x20, 0x30, 0x40, 0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0x02, 0x03, 0x04, 0xee, 0xff,
            0x11, 0x22,
        ];
        write_array_len(&mut body, 1, true).expect("topic count");
        body.extend_from_slice(&topic_id);

        write_array_len(&mut body, 1, true).expect("partition count");
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&5_i32.to_be_bytes());
        body.extend_from_slice(&42_i64.to_be_bytes());
        body.extend_from_slice(&4_i32.to_be_bytes());
        body.extend_from_slice(&3_i64.to_be_bytes());
        body.extend_from_slice(&8_192_i32.to_be_bytes());

        let replica_directory_id = [
            0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let partition_tagged_fields = vec![
            (0_u32, replica_directory_id.to_vec()),
            (1_u32, 123_i64.to_be_bytes().to_vec()),
        ];
        write_tagged_fields(&mut body, partition_tagged_fields).expect("partition tags");

        write_unsigned_varint(&mut body, 0);
        write_array_len(&mut body, 0, true).expect("forgotten topics");
        write_string(&mut body, "", true).expect("rack id");

        let mut top_level_tags = Vec::new();
        let mut cluster_id_payload = Vec::new();
        write_nullable_string(&mut cluster_id_payload, Some("cluster-a"), true)
            .expect("cluster id payload");
        top_level_tags.push((0_u32, cluster_id_payload));

        let mut replica_state_payload = Vec::new();
        replica_state_payload.extend_from_slice(&7_i32.to_be_bytes());
        replica_state_payload.extend_from_slice(&9_i64.to_be_bytes());
        top_level_tags.push((1_u32, replica_state_payload));
        write_tagged_fields(&mut body, top_level_tags).expect("top-level tags");

        let (decoded, read) = decode_fetch_request(version, &body).expect("decode fetch request");
        assert_eq!(read, body.len());
        assert_eq!(decoded.replica_id, 7);
        assert_eq!(decoded.replica_epoch, Some(9));
        assert_eq!(decoded.cluster_id.as_deref(), Some("cluster-a"));
        assert_eq!(decoded.isolation_level, 0);
        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].topic_id, Some(topic_id));
        assert_eq!(decoded.topics[0].partitions.len(), 1);
        let partition = &decoded.topics[0].partitions[0];
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.fetch_offset, 42);
        assert_eq!(partition.partition_max_bytes, 8_192);
        assert_eq!(partition.replica_directory_id, Some(replica_directory_id));
        assert_eq!(partition.high_watermark, Some(123));
    }

    #[test]
    fn encode_fetch_response_v16_includes_node_endpoints_tag() {
        let version = 16;
        let topic_id = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let topics = vec![FetchTopicResponse {
            name: None,
            topic_id: Some(topic_id),
            partitions: vec![FetchPartitionResponse {
                partition_index: 0,
                error_code: ERROR_NONE,
                high_watermark: 12,
                last_stable_offset: 12,
                log_start_offset: 0,
                aborted_transactions: None,
                preferred_read_replica: -1,
                records: Some(vec![1, 2, 3]),
            }],
        }];
        let node_endpoints = vec![FetchNodeEndpointResponse {
            node_id: 42,
            host: "broker-42".to_string(),
            port: 9092,
            rack: Some("rack-a".to_string()),
        }];

        let encoded = encode_fetch_response(version, 0, 0, 0, &topics, &node_endpoints)
            .expect("encode fetch response");
        let mut cursor = 0;

        assert_eq!(read_i32(&encoded, &mut cursor).expect("throttle"), 0);
        assert_eq!(read_i16(&encoded, &mut cursor).expect("top-level error"), 0);
        assert_eq!(read_i32(&encoded, &mut cursor).expect("session id"), 0);

        let topic_count = read_array_len(&encoded, &mut cursor, true).expect("topic count");
        assert_eq!(topic_count, 1);
        assert_eq!(
            read_uuid(&encoded, &mut cursor).expect("topic id"),
            topic_id
        );
        let partition_count = read_array_len(&encoded, &mut cursor, true).expect("partition count");
        assert_eq!(partition_count, 1);

        assert_eq!(read_i32(&encoded, &mut cursor).expect("partition index"), 0);
        assert_eq!(read_i16(&encoded, &mut cursor).expect("partition error"), 0);
        assert_eq!(read_i64(&encoded, &mut cursor).expect("high watermark"), 12);
        assert_eq!(read_i64(&encoded, &mut cursor).expect("lso"), 12);
        assert_eq!(
            read_i64(&encoded, &mut cursor).expect("log start offset"),
            0
        );
        assert_eq!(
            read_unsigned_varint(&encoded, &mut cursor).expect("aborted txns nullable array"),
            0
        );
        assert_eq!(
            read_i32(&encoded, &mut cursor).expect("preferred read replica"),
            -1
        );
        assert_eq!(
            read_nullable_bytes(&encoded, &mut cursor, true).expect("records"),
            Some(vec![1, 2, 3])
        );
        skip_tagged_fields(&encoded, &mut cursor).expect("partition tags");
        skip_tagged_fields(&encoded, &mut cursor).expect("topic tags");

        let mut decoded_endpoint_count = 0;
        let mut decoded_endpoint_id = -1;
        let mut decoded_endpoint_host = String::new();
        let mut decoded_endpoint_port = -1;
        let mut decoded_endpoint_rack = None;
        read_tagged_fields(&encoded, &mut cursor, |tag, payload| {
            if tag != 0 {
                return Ok(());
            }

            let mut payload_cursor = 0;
            decoded_endpoint_count = read_array_len(payload, &mut payload_cursor, true)?;
            assert_eq!(decoded_endpoint_count, 1);

            decoded_endpoint_id = read_i32(payload, &mut payload_cursor)?;
            decoded_endpoint_host = read_string(payload, &mut payload_cursor, true)?;
            decoded_endpoint_port = read_i32(payload, &mut payload_cursor)?;
            decoded_endpoint_rack = read_nullable_string(payload, &mut payload_cursor, true)?;
            skip_tagged_fields(payload, &mut payload_cursor)?;
            assert_eq!(payload_cursor, payload.len());
            Ok(())
        })
        .expect("top-level tags");

        assert_eq!(decoded_endpoint_count, 1);
        assert_eq!(decoded_endpoint_id, 42);
        assert_eq!(decoded_endpoint_host, "broker-42");
        assert_eq!(decoded_endpoint_port, 9092);
        assert_eq!(decoded_endpoint_rack.as_deref(), Some("rack-a"));
        assert_eq!(cursor, encoded.len());
    }
}
