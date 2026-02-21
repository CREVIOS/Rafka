use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{AsyncTransportServer, PersistentLogConfig, TransportError};
use rafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse, VersionedCodec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const API_KEY_API_VERSIONS: i16 = 18;

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
            "rafka-async-transport-{label}-{millis}-{}-{counter}",
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

fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        API_KEY_API_VERSIONS => {
            if api_version >= 3 {
                2
            } else {
                1
            }
        }
        _ => 1,
    }
}

fn encode_request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&api_key.to_be_bytes());
    header.extend_from_slice(&api_version.to_be_bytes());
    header.extend_from_slice(&correlation_id.to_be_bytes());
    header.extend_from_slice(&(5_i16).to_be_bytes());
    header.extend_from_slice(b"async");
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

fn decode_response_header(frame: &[u8]) -> (i32, usize) {
    let correlation_id = i32::from_be_bytes(
        frame[0..4]
            .try_into()
            .expect("response correlation id bytes"),
    );
    (correlation_id, 4)
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let frame_len = i32::from_be_bytes(len_buf);
    assert!(
        frame_len >= 0,
        "response frame length should be non-negative"
    );
    let mut frame = vec![0_u8; usize::try_from(frame_len).expect("frame length fits usize")];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

async fn send_api_versions_and_decode(
    stream: &mut TcpStream,
    api_version: i16,
    correlation_id: i32,
) -> Result<ApiVersionsResponse, String> {
    let request = ApiVersionsRequest::default()
        .encode(api_version)
        .map_err(|err| format!("encode apiversions request: {err:?}"))?;
    let frame = encode_request_frame(API_KEY_API_VERSIONS, api_version, correlation_id, &request);
    stream
        .write_all(&frame)
        .await
        .map_err(|err| format!("write request frame: {err}"))?;
    stream
        .flush()
        .await
        .map_err(|err| format!("flush request frame: {err}"))?;

    let response_frame = read_frame(stream)
        .await
        .map_err(|err| format!("read response frame: {err}"))?;
    let (decoded_correlation_id, payload_offset) = decode_response_header(&response_frame);
    if decoded_correlation_id != correlation_id {
        return Err(format!(
            "correlation mismatch: expected {correlation_id}, got {decoded_correlation_id}"
        ));
    }
    let (response, read) =
        ApiVersionsResponse::decode(api_version, &response_frame[payload_offset..])
            .map_err(|err| format!("decode apiversions response: {err:?}"))?;
    if read != response_frame.len() - payload_offset {
        return Err("response payload has trailing bytes".to_string());
    }
    Ok(response)
}

#[tokio::test]
async fn async_transport_api_versions_roundtrip_matrix_on_single_connection() {
    let temp_dir = TempDir::new("apiversions-matrix");
    let server = AsyncTransportServer::bind("127.0.0.1:0", temp_dir.path(), log_config())
        .await
        .expect("bind async server");
    let addr = server.local_addr().expect("server addr");

    let client = async move {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|err| format!("connect client: {err}"))?;

        let response_v0 = send_api_versions_and_decode(&mut stream, 0, 1).await?;
        if response_v0.error_code != 0 {
            return Err(format!("v0 error code {}", response_v0.error_code));
        }

        let response_v4 = send_api_versions_and_decode(&mut stream, 4, 2).await?;
        if response_v4.error_code != 0 {
            return Err(format!("v4 error code {}", response_v4.error_code));
        }
        if !response_v4
            .api_keys
            .iter()
            .any(|api| api.api_key == API_KEY_API_VERSIONS)
        {
            return Err("ApiVersions key missing in v4 response".to_string());
        }

        stream
            .shutdown()
            .await
            .map_err(|err| format!("shutdown stream: {err}"))?;
        Ok::<(), String>(())
    };

    let (server_result, client_result) = tokio::join!(server.serve_one_connection(), client);
    assert!(server_result.is_ok(), "server result: {server_result:?}");
    assert!(client_result.is_ok(), "client result: {client_result:?}");

    let metrics = server.metrics_prometheus().await.expect("metrics payload");
    assert!(
        metrics.contains("rafka_transport_api_requests_total{api_key=\"18\",api_version=\"0\"} 1")
    );
    assert!(
        metrics.contains("rafka_transport_api_requests_total{api_key=\"18\",api_version=\"4\"} 1")
    );
}

#[tokio::test]
async fn async_transport_handles_many_connections_concurrently() {
    let temp_dir = TempDir::new("concurrent-connections");
    let server = AsyncTransportServer::bind("127.0.0.1:0", temp_dir.path(), log_config())
        .await
        .expect("bind async server");
    let addr = server.local_addr().expect("server addr");
    let connection_count: usize = 24;

    let clients = async move {
        let mut tasks = Vec::with_capacity(connection_count);
        for correlation in 0..connection_count {
            tasks.push(tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr)
                    .await
                    .map_err(|err| format!("connect client {correlation}: {err}"))?;
                let response = send_api_versions_and_decode(
                    &mut stream,
                    4,
                    i32::try_from(correlation).expect("correlation id fits i32"),
                )
                .await?;
                if response.error_code != 0 {
                    return Err(format!(
                        "client {correlation} error code {}",
                        response.error_code
                    ));
                }
                stream
                    .shutdown()
                    .await
                    .map_err(|err| format!("shutdown client {correlation}: {err}"))?;
                Ok::<(), String>(())
            }));
        }
        for task in tasks {
            let result = task
                .await
                .map_err(|err| format!("join client task: {err}"))?;
            result?;
        }
        Ok::<(), String>(())
    };

    let (server_result, client_result) = tokio::join!(
        server.serve_n_connections_concurrent(connection_count),
        clients
    );
    assert!(server_result.is_ok(), "server result: {server_result:?}");
    assert!(client_result.is_ok(), "client result: {client_result:?}");

    let metrics = server.metrics_prometheus().await.expect("metrics payload");
    assert!(
        metrics.contains("rafka_transport_api_requests_total{api_key=\"18\",api_version=\"4\"} 24")
    );
    assert!(metrics
        .contains("rafka_transport_api_responses_total{api_key=\"18\",api_version=\"4\"} 24"));
}

#[tokio::test]
async fn async_transport_rejects_oversized_frame() {
    let temp_dir = TempDir::new("oversized-frame");
    let server = AsyncTransportServer::bind("127.0.0.1:0", temp_dir.path(), log_config())
        .await
        .expect("bind async server");
    server
        .set_max_frame_size(8)
        .await
        .expect("set max frame size");
    let addr = server.local_addr().expect("server addr");

    let client = async move {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|err| format!("connect client: {err}"))?;
        stream
            .write_all(&16_i32.to_be_bytes())
            .await
            .map_err(|err| format!("write frame len: {err}"))?;
        stream
            .write_all(&[0_u8; 16])
            .await
            .map_err(|err| format!("write frame payload: {err}"))?;
        stream
            .flush()
            .await
            .map_err(|err| format!("flush stream: {err}"))?;
        Ok::<(), String>(())
    };

    let (server_result, client_result) = tokio::join!(server.serve_one_connection(), client);
    assert!(client_result.is_ok(), "client result: {client_result:?}");
    match server_result {
        Err(TransportError::FrameTooLarge { size, max_size }) => {
            assert_eq!(size, 16);
            assert_eq!(max_size, 8);
        }
        other => panic!("expected FrameTooLarge error, got {other:?}"),
    }
}
