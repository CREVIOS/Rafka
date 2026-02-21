use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rafka_broker::{
    AclAuthorizer, AclOperation, AclResourceType, PersistentLogConfig, SaslConfig, TransportError,
    TransportSecurityConfig, TransportServer,
};
use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, HeartbeatRequest, HeartbeatResponse, ProduceRequest,
    ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, VersionedCodec,
};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, StreamOwned};
use sha2::{Digest, Sha256, Sha512};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_HEARTBEAT: i16 = 12;
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-security-transport-{label}-{millis}-{}-{counter}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn log_config() -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 8,
        sync_on_append: false,
    }
}

fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        API_KEY_API_VERSIONS => {
            if api_version >= 3 {
                2
            } else {
                1
            }
        }
        API_KEY_PRODUCE => {
            if api_version >= 9 {
                2
            } else {
                1
            }
        }
        API_KEY_HEARTBEAT => {
            if api_version >= 4 {
                2
            } else {
                1
            }
        }
        API_KEY_SASL_HANDSHAKE => 1,
        API_KEY_SASL_AUTHENTICATE => {
            if api_version >= 2 {
                2
            } else {
                1
            }
        }
        _ => 1,
    }
}

fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        API_KEY_API_VERSIONS => 0,
        API_KEY_PRODUCE => {
            if api_version >= 9 {
                1
            } else {
                0
            }
        }
        API_KEY_HEARTBEAT => {
            if api_version >= 4 {
                1
            } else {
                0
            }
        }
        API_KEY_SASL_HANDSHAKE => 0,
        API_KEY_SASL_AUTHENTICATE => {
            if api_version >= 2 {
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn write_unsigned_varint(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_unsigned_varint(input: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let mut value = 0_u32;
    let mut shift = 0_u32;
    loop {
        if *cursor >= input.len() {
            return Err("truncated unsigned varint".to_string());
        }
        let byte = input[*cursor];
        *cursor += 1;
        value |= u32::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 28 {
            return Err("unsigned varint overflow".to_string());
        }
    }
}

fn encode_request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    body: &[u8],
) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&api_key.to_be_bytes());
    header.extend_from_slice(&api_version.to_be_bytes());
    header.extend_from_slice(&correlation_id.to_be_bytes());

    match client_id {
        Some(client_id) => {
            let client_id_bytes = client_id.as_bytes();
            let len = i16::try_from(client_id_bytes.len()).expect("client id length fits i16");
            header.extend_from_slice(&len.to_be_bytes());
            header.extend_from_slice(client_id_bytes);
        }
        None => header.extend_from_slice(&(-1_i16).to_be_bytes()),
    }

    if request_header_version(api_key, api_version) == 2 {
        write_unsigned_varint(&mut header, 0);
    }

    let frame_len = i32::try_from(header.len() + body.len()).expect("frame length fits i32");
    let mut frame = Vec::with_capacity(4 + header.len() + body.len());
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

fn decode_response_header(frame: &[u8], header_version: i16) -> (i32, usize) {
    let mut cursor = 0;
    let correlation_id = i32::from_be_bytes(
        frame[cursor..cursor + 4]
            .try_into()
            .expect("response correlation id bytes"),
    );
    cursor += 4;
    if header_version == 1 {
        let tagged_fields =
            read_unsigned_varint(frame, &mut cursor).expect("response header tagged fields");
        assert_eq!(
            tagged_fields, 0,
            "expected empty response header tagged fields"
        );
    }
    (correlation_id, cursor)
}

fn read_frame<R: Read>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf)?;
    let frame_len = i32::from_be_bytes(len_buf);
    assert!(frame_len >= 0, "frame length must be non-negative");
    let mut frame = vec![0_u8; usize::try_from(frame_len).expect("frame length fits usize")];
    reader.read_exact(&mut frame)?;
    Ok(frame)
}

fn send_request<S: Write>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) {
    let frame = encode_request_frame(
        api_key,
        api_version,
        correlation_id,
        Some("rafka-test"),
        body,
    );
    stream.write_all(&frame).expect("write request frame");
    stream.flush().expect("flush request frame");
}

fn decode_typed_response<T: VersionedCodec>(
    frame: &[u8],
    api_key: i16,
    api_version: i16,
) -> (i32, T) {
    let (correlation_id, header_len) =
        decode_response_header(frame, response_header_version(api_key, api_version));
    let (decoded, read) = T::decode(api_version, &frame[header_len..]).expect("decode response");
    assert_eq!(
        read,
        frame.len() - header_len,
        "response body should be fully decoded"
    );
    (correlation_id, decoded)
}

fn produce_v9_body(topic: &str, partition: i32, payload: Vec<u8>) -> Vec<u8> {
    ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(topic.to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: partition,
                records: Some(payload),
            }],
        }],
    }
    .encode(9)
    .expect("encode produce v9 request")
}

fn build_tls_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()]).expect("self-signed cert");

    let cert_der = cert.der().clone();
    let key_der = key_pair.serialize_der();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der));

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)
        .expect("build server tls config");

    let mut roots = RootCertStore::empty();
    roots.add(cert_der).expect("add root cert");
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    (Arc::new(server_config), Arc::new(client_config))
}

fn connect_tls(
    addr: SocketAddr,
    config: Arc<ClientConfig>,
) -> StreamOwned<ClientConnection, TcpStream> {
    let stream = TcpStream::connect(addr).expect("connect tls tcp");
    let server_name = ServerName::try_from("localhost").expect("server name");
    let connection = ClientConnection::new(config, server_name).expect("tls client connection");
    let mut tls_stream = StreamOwned::new(connection, stream);
    while tls_stream.conn.is_handshaking() {
        tls_stream
            .conn
            .complete_io(&mut tls_stream.sock)
            .expect("complete client handshake");
    }
    tls_stream
}

fn plain_auth_bytes(username: &str, password: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(username.len() + password.len() + 2);
    payload.push(0);
    payload.extend_from_slice(username.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password.as_bytes());
    payload
}

fn parse_scram_attributes(input: &str) -> BTreeMap<char, String> {
    let mut attrs = BTreeMap::new();
    for part in input.split(',') {
        if let Some((name, value)) = part.split_once('=') {
            let key = name
                .chars()
                .next()
                .expect("scram attribute key should not be empty");
            attrs.insert(key, value.to_string());
        }
    }
    attrs
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("valid hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha512::new_from_slice(key).expect("valid hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn xor_bytes(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    lhs.iter().zip(rhs.iter()).map(|(a, b)| a ^ b).collect()
}

fn scram_client_final_message(
    mechanism: &str,
    password: &str,
    client_first_bare: &str,
    server_first: &str,
) -> String {
    let attrs = parse_scram_attributes(server_first);
    let nonce = attrs.get(&'r').expect("server nonce");
    let salt_b64 = attrs.get(&'s').expect("server salt");
    let iterations = attrs
        .get(&'i')
        .expect("server iterations")
        .parse::<u32>()
        .expect("iterations should parse");
    let salt = STANDARD_NO_PAD
        .decode(salt_b64)
        .expect("decode base64 salt");

    let client_final_without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");

    let client_proof = match mechanism {
        "SCRAM-SHA-256" => {
            let mut salted = [0_u8; 32];
            pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);
            let client_key = hmac_sha256(&salted, b"Client Key");
            let stored_key = Sha256::digest(&client_key);
            let signature = hmac_sha256(stored_key.as_slice(), auth_message.as_bytes());
            xor_bytes(&client_key, &signature)
        }
        "SCRAM-SHA-512" => {
            let mut salted = [0_u8; 64];
            pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, iterations, &mut salted);
            let client_key = hmac_sha512(&salted, b"Client Key");
            let stored_key = Sha512::digest(&client_key);
            let signature = hmac_sha512(stored_key.as_slice(), auth_message.as_bytes());
            xor_bytes(&client_key, &signature)
        }
        _ => panic!("unsupported mechanism {mechanism}"),
    };
    let proof = STANDARD_NO_PAD.encode(client_proof);
    format!("{client_final_without_proof},p={proof}")
}

#[test]
fn transport_tls_apiversions_roundtrip_and_metrics() {
    let temp_dir = TempDir::new("tls-apiversions");
    let (tls_server_config, tls_client_config) = build_tls_configs();

    let security = TransportSecurityConfig {
        tls_server_config: Some(tls_server_config),
        sasl: None,
        acl: None,
    };
    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut stream = connect_tls(addr, tls_client_config);
    let request = ApiVersionsRequest::default()
        .encode(4)
        .expect("encode ApiVersions request");
    send_request(&mut stream, API_KEY_API_VERSIONS, 4, 1, &request);

    let frame = read_frame(&mut stream).expect("read ApiVersions response");
    let (correlation_id, response): (i32, ApiVersionsResponse) =
        decode_typed_response(&frame, API_KEY_API_VERSIONS, 4);
    assert_eq!(correlation_id, 1);
    assert_eq!(response.error_code, 0);
    assert!(response
        .api_keys
        .iter()
        .any(|api| api.api_key == API_KEY_API_VERSIONS));

    stream
        .sock
        .shutdown(Shutdown::Both)
        .expect("shutdown client");
    let (server, serve_result) = handle.join().expect("join server");
    assert!(
        serve_result.is_ok(),
        "serve_one_connection: {serve_result:?}"
    );

    let metrics = server.metrics_prometheus().expect("render metrics");
    assert!(metrics.contains("rafka_transport_tls_handshake_total{result=\"success\"} 1"));
}

#[test]
fn transport_tls_handshake_failure_records_failure_metric() {
    let temp_dir = TempDir::new("tls-failure");
    let (tls_server_config, _tls_client_config) = build_tls_configs();

    let security = TransportSecurityConfig {
        tls_server_config: Some(tls_server_config),
        sasl: None,
        acl: None,
    };
    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut plaintext = TcpStream::connect(addr).expect("connect plaintext stream");
    plaintext
        .write_all(b"not-a-tls-client-hello")
        .expect("write plaintext bytes");
    plaintext
        .shutdown(Shutdown::Write)
        .expect("shutdown plaintext writer");

    let (server, serve_result) = handle.join().expect("join server");
    match serve_result {
        Err(TransportError::Tls(_)) => {}
        other => panic!("expected TLS error, got {other:?}"),
    }

    let metrics = server.metrics_prometheus().expect("render metrics");
    assert!(metrics.contains("rafka_transport_tls_handshake_total{result=\"failure\"} 1"));
}

#[test]
fn transport_sasl_plain_wire_auth_then_apiversions() {
    let temp_dir = TempDir::new("sasl-plain");
    let mut sasl = SaslConfig::new();
    sasl.add_plain_user("alice", "s3cr3t")
        .expect("add plain user");
    let security = TransportSecurityConfig {
        tls_server_config: None,
        sasl: Some(sasl),
        acl: None,
    };

    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let handshake = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
    }
    .encode(1)
    .expect("encode sasl handshake");
    send_request(&mut stream, API_KEY_SASL_HANDSHAKE, 1, 10, &handshake);
    let handshake_frame = read_frame(&mut stream).expect("read handshake response");
    let (handshake_correlation_id, handshake_response): (i32, SaslHandshakeResponse) =
        decode_typed_response(&handshake_frame, API_KEY_SASL_HANDSHAKE, 1);
    assert_eq!(handshake_correlation_id, 10);
    assert_eq!(handshake_response.error_code, 0);
    assert!(handshake_response
        .mechanisms
        .iter()
        .any(|mechanism| mechanism == "PLAIN"));

    let auth = SaslAuthenticateRequest {
        auth_bytes: plain_auth_bytes("alice", "s3cr3t"),
    }
    .encode(2)
    .expect("encode sasl authenticate");
    send_request(&mut stream, API_KEY_SASL_AUTHENTICATE, 2, 11, &auth);
    let auth_frame = read_frame(&mut stream).expect("read authenticate response");
    let (auth_correlation_id, auth_response): (i32, SaslAuthenticateResponse) =
        decode_typed_response(&auth_frame, API_KEY_SASL_AUTHENTICATE, 2);
    assert_eq!(auth_correlation_id, 11);
    assert_eq!(auth_response.error_code, 0);
    assert_eq!(auth_response.error_message, None);
    assert_eq!(auth_response.session_lifetime_ms, Some(0));

    let request = ApiVersionsRequest::default()
        .encode(4)
        .expect("encode ApiVersions request");
    send_request(&mut stream, API_KEY_API_VERSIONS, 4, 12, &request);
    let versions_frame = read_frame(&mut stream).expect("read ApiVersions response");
    let (versions_correlation_id, versions_response): (i32, ApiVersionsResponse) =
        decode_typed_response(&versions_frame, API_KEY_API_VERSIONS, 4);
    assert_eq!(versions_correlation_id, 12);
    assert_eq!(versions_response.error_code, 0);
    assert!(versions_response
        .api_keys
        .iter()
        .any(|api| api.api_key == API_KEY_SASL_HANDSHAKE));
    assert!(versions_response
        .api_keys
        .iter()
        .any(|api| api.api_key == API_KEY_SASL_AUTHENTICATE));

    stream.shutdown(Shutdown::Both).expect("shutdown stream");
    let (server, serve_result) = handle.join().expect("join server");
    assert!(
        serve_result.is_ok(),
        "serve_one_connection: {serve_result:?}"
    );

    let metrics = server.metrics_prometheus().expect("render metrics");
    assert!(metrics
        .contains("rafka_transport_sasl_auth_total{mechanism=\"PLAIN\",result=\"success\"} 1"));
}

#[test]
fn transport_sasl_acl_uses_authenticated_principal_for_group_checks() {
    let temp_dir = TempDir::new("sasl-acl-principal");
    let mut sasl = SaslConfig::new();
    sasl.add_plain_user("alice", "s3cr3t")
        .expect("add plain user");
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "alice",
        AclOperation::Read,
        AclResourceType::Group,
        "secure-group",
    )
    .expect("allow group rule");
    let security = TransportSecurityConfig {
        tls_server_config: None,
        sasl: Some(sasl),
        acl: Some(acl),
    };

    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let handshake = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
    }
    .encode(1)
    .expect("encode sasl handshake");
    send_request(&mut stream, API_KEY_SASL_HANDSHAKE, 1, 50, &handshake);
    let handshake_frame = read_frame(&mut stream).expect("read handshake response");
    let (handshake_correlation_id, handshake_response): (i32, SaslHandshakeResponse) =
        decode_typed_response(&handshake_frame, API_KEY_SASL_HANDSHAKE, 1);
    assert_eq!(handshake_correlation_id, 50);
    assert_eq!(handshake_response.error_code, 0);

    let auth = SaslAuthenticateRequest {
        auth_bytes: plain_auth_bytes("alice", "s3cr3t"),
    }
    .encode(2)
    .expect("encode sasl authenticate");
    send_request(&mut stream, API_KEY_SASL_AUTHENTICATE, 2, 51, &auth);
    let auth_frame = read_frame(&mut stream).expect("read authenticate response");
    let (auth_correlation_id, auth_response): (i32, SaslAuthenticateResponse) =
        decode_typed_response(&auth_frame, API_KEY_SASL_AUTHENTICATE, 2);
    assert_eq!(auth_correlation_id, 51);
    assert_eq!(auth_response.error_code, 0);

    let heartbeat_allowed = HeartbeatRequest {
        group_id: "secure-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
        group_instance_id: None,
    }
    .encode(4)
    .expect("encode allowed heartbeat");
    send_request(&mut stream, API_KEY_HEARTBEAT, 4, 52, &heartbeat_allowed);
    let allowed_frame = read_frame(&mut stream).expect("read allowed heartbeat");
    let (allowed_correlation_id, allowed_response): (i32, HeartbeatResponse) =
        decode_typed_response(&allowed_frame, API_KEY_HEARTBEAT, 4);
    assert_eq!(allowed_correlation_id, 52);
    assert_eq!(allowed_response.error_code, 25);

    let heartbeat_denied = HeartbeatRequest {
        group_id: "blocked-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
        group_instance_id: None,
    }
    .encode(4)
    .expect("encode denied heartbeat");
    send_request(&mut stream, API_KEY_HEARTBEAT, 4, 53, &heartbeat_denied);
    let denied_frame = read_frame(&mut stream).expect("read denied heartbeat");
    let (denied_correlation_id, denied_response): (i32, HeartbeatResponse) =
        decode_typed_response(&denied_frame, API_KEY_HEARTBEAT, 4);
    assert_eq!(denied_correlation_id, 53);
    assert_eq!(denied_response.error_code, 30);

    stream.shutdown(Shutdown::Both).expect("shutdown stream");
    let (_server, serve_result) = handle.join().expect("join server");
    assert!(
        serve_result.is_ok(),
        "serve_one_connection: {serve_result:?}"
    );
}

#[test]
fn transport_sasl_rejects_produce_before_authentication() {
    let temp_dir = TempDir::new("sasl-preauth-reject");
    let mut sasl = SaslConfig::new();
    sasl.add_plain_user("alice", "s3cr3t")
        .expect("add plain user");
    let security = TransportSecurityConfig {
        tls_server_config: None,
        sasl: Some(sasl),
        acl: None,
    };

    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set read timeout");
    let produce = produce_v9_body("secured-topic", 0, vec![1, 2, 3, 4]);
    send_request(&mut stream, API_KEY_PRODUCE, 9, 30, &produce);
    let response = read_frame(&mut stream);
    assert!(
        response.is_err(),
        "unauthenticated produce should not receive a response frame"
    );

    let (server, serve_result) = handle.join().expect("join server");
    match serve_result {
        Err(TransportError::Security(_)) => {}
        other => panic!("expected security error, got {other:?}"),
    }

    let metrics = server.metrics_prometheus().expect("render metrics");
    assert!(metrics.contains(
        "rafka_transport_api_errors_total{api_key=\"0\",api_version=\"9\",class=\"security\"} 1"
    ));
}

fn run_scram_flow_test(mechanism: &str, configure: impl FnOnce(&mut SaslConfig)) {
    let temp_dir = TempDir::new("sasl-scram");
    let mut sasl = SaslConfig::new();
    configure(&mut sasl);
    let security = TransportSecurityConfig {
        tls_server_config: None,
        sasl: Some(sasl),
        acl: None,
    };

    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let username = "alice";
    let password = "scram-secret";
    let client_nonce = "clientnonce123";

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let handshake = SaslHandshakeRequest {
        mechanism: mechanism.to_string(),
    }
    .encode(1)
    .expect("encode sasl handshake");
    send_request(&mut stream, API_KEY_SASL_HANDSHAKE, 1, 40, &handshake);
    let handshake_frame = read_frame(&mut stream).expect("read handshake response");
    let (handshake_correlation_id, handshake_response): (i32, SaslHandshakeResponse) =
        decode_typed_response(&handshake_frame, API_KEY_SASL_HANDSHAKE, 1);
    assert_eq!(handshake_correlation_id, 40);
    assert_eq!(handshake_response.error_code, 0);
    assert!(handshake_response
        .mechanisms
        .iter()
        .any(|enabled| enabled == mechanism));

    let client_first_bare = format!("n={username},r={client_nonce}");
    let client_first = format!("n,,{client_first_bare}");
    let auth_first = SaslAuthenticateRequest {
        auth_bytes: client_first.as_bytes().to_vec(),
    }
    .encode(2)
    .expect("encode first authenticate");
    send_request(&mut stream, API_KEY_SASL_AUTHENTICATE, 2, 41, &auth_first);
    let first_frame = read_frame(&mut stream).expect("read first auth response");
    let (first_correlation_id, first_response): (i32, SaslAuthenticateResponse) =
        decode_typed_response(&first_frame, API_KEY_SASL_AUTHENTICATE, 2);
    assert_eq!(first_correlation_id, 41);
    assert_eq!(first_response.error_code, 0);
    let server_first = String::from_utf8(first_response.auth_bytes).expect("server-first utf8");
    assert!(
        server_first.starts_with(&format!("r={client_nonce}")),
        "server nonce should extend client nonce"
    );

    let client_final =
        scram_client_final_message(mechanism, password, &client_first_bare, &server_first);
    let auth_final = SaslAuthenticateRequest {
        auth_bytes: client_final.as_bytes().to_vec(),
    }
    .encode(2)
    .expect("encode final authenticate");
    send_request(&mut stream, API_KEY_SASL_AUTHENTICATE, 2, 42, &auth_final);
    let final_frame = read_frame(&mut stream).expect("read final auth response");
    let (final_correlation_id, final_response): (i32, SaslAuthenticateResponse) =
        decode_typed_response(&final_frame, API_KEY_SASL_AUTHENTICATE, 2);
    assert_eq!(final_correlation_id, 42);
    assert_eq!(final_response.error_code, 0);
    let server_final = String::from_utf8(final_response.auth_bytes).expect("server-final utf8");
    assert!(
        server_final.starts_with("v="),
        "expected server signature in final SCRAM message"
    );

    let request = ApiVersionsRequest::default()
        .encode(4)
        .expect("encode ApiVersions request");
    send_request(&mut stream, API_KEY_API_VERSIONS, 4, 43, &request);
    let versions_frame = read_frame(&mut stream).expect("read ApiVersions response");
    let (versions_correlation_id, versions_response): (i32, ApiVersionsResponse) =
        decode_typed_response(&versions_frame, API_KEY_API_VERSIONS, 4);
    assert_eq!(versions_correlation_id, 43);
    assert_eq!(versions_response.error_code, 0);

    stream.shutdown(Shutdown::Both).expect("shutdown stream");
    let (server, serve_result) = handle.join().expect("join server");
    assert!(
        serve_result.is_ok(),
        "serve_one_connection: {serve_result:?}"
    );

    let metrics = server.metrics_prometheus().expect("render metrics");
    let continue_metric = format!(
        "rafka_transport_sasl_auth_total{{mechanism=\"{mechanism}\",result=\"continue\"}} 1"
    );
    let success_metric = format!(
        "rafka_transport_sasl_auth_total{{mechanism=\"{mechanism}\",result=\"success\"}} 1"
    );
    assert!(metrics.contains(&continue_metric));
    assert!(metrics.contains(&success_metric));
}

#[test]
fn transport_sasl_scram_sha256_wire_auth_then_apiversions() {
    run_scram_flow_test("SCRAM-SHA-256", |config| {
        config
            .add_scram_sha256_user("alice", "scram-secret", b"scram256-salt".to_vec(), 4096)
            .expect("add scram256 user");
    });
}

#[test]
fn transport_sasl_scram_sha512_wire_auth_then_apiversions() {
    run_scram_flow_test("SCRAM-SHA-512", |config| {
        config
            .add_scram_sha512_user("alice", "scram-secret", b"scram512-salt".to_vec(), 4096)
            .expect("add scram512 user");
    });
}

#[test]
fn transport_tls_plus_sasl_plain_end_to_end() {
    let temp_dir = TempDir::new("tls-plus-sasl-plain");
    let (tls_server_config, tls_client_config) = build_tls_configs();
    let mut sasl = SaslConfig::new();
    sasl.add_plain_user("alice", "s3cr3t")
        .expect("add plain user");
    let security = TransportSecurityConfig {
        tls_server_config: Some(tls_server_config),
        sasl: Some(sasl),
        acl: None,
    };

    let mut server =
        TransportServer::bind_with_security("127.0.0.1:0", temp_dir.path(), log_config(), security)
            .expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || {
        let result = server.serve_one_connection();
        (server, result)
    });

    let mut stream = connect_tls(addr, tls_client_config);

    let handshake = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
    }
    .encode(1)
    .expect("encode sasl handshake");
    send_request(&mut stream, API_KEY_SASL_HANDSHAKE, 1, 80, &handshake);
    let handshake_frame = read_frame(&mut stream).expect("read handshake response");
    let (_handshake_correlation_id, handshake_response): (i32, SaslHandshakeResponse) =
        decode_typed_response(&handshake_frame, API_KEY_SASL_HANDSHAKE, 1);
    assert_eq!(handshake_response.error_code, 0);

    let auth = SaslAuthenticateRequest {
        auth_bytes: plain_auth_bytes("alice", "s3cr3t"),
    }
    .encode(2)
    .expect("encode sasl authenticate");
    send_request(&mut stream, API_KEY_SASL_AUTHENTICATE, 2, 81, &auth);
    let auth_frame = read_frame(&mut stream).expect("read auth response");
    let (_auth_correlation_id, auth_response): (i32, SaslAuthenticateResponse) =
        decode_typed_response(&auth_frame, API_KEY_SASL_AUTHENTICATE, 2);
    assert_eq!(auth_response.error_code, 0);

    let versions = ApiVersionsRequest::default()
        .encode(4)
        .expect("encode ApiVersions");
    send_request(&mut stream, API_KEY_API_VERSIONS, 4, 82, &versions);
    let versions_frame = read_frame(&mut stream).expect("read apiversions response");
    let (_versions_correlation_id, versions_response): (i32, ApiVersionsResponse) =
        decode_typed_response(&versions_frame, API_KEY_API_VERSIONS, 4);
    assert_eq!(versions_response.error_code, 0);

    stream
        .sock
        .shutdown(Shutdown::Both)
        .expect("shutdown client");
    let (server, serve_result) = handle.join().expect("join server");
    assert!(
        serve_result.is_ok(),
        "serve_one_connection: {serve_result:?}"
    );

    let metrics = server.metrics_prometheus().expect("render metrics");
    assert!(metrics.contains("rafka_transport_tls_handshake_total{result=\"success\"} 1"));
    assert!(metrics
        .contains("rafka_transport_sasl_auth_total{mechanism=\"PLAIN\",result=\"success\"} 1"));
}
