#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, ApiVersionsResponseApiVersion,
    ApiVersionsResponseFinalizedFeatureKey, ApiVersionsResponseSupportedFeatureKey, ProduceRequest,
    ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData, VersionedCodec,
    API_VERSIONS_MAX_VERSION, API_VERSIONS_MIN_VERSION, PRODUCE_MAX_VERSION, PRODUCE_MIN_VERSION,
};

const ENABLE_ENV: &str = "RAFKA_ENABLE_JVM_DIFF";

struct JvmHarness {
    classpath: String,
}

static JVM_HARNESS: OnceLock<Result<JvmHarness, String>> = OnceLock::new();

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn rafka_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn gradle_user_home() -> PathBuf {
    rafka_root().join(".gradle-home")
}

fn jvm_diff_enabled() -> bool {
    std::env::var_os(ENABLE_ENV).is_some()
}

fn skip_if_disabled() -> bool {
    if jvm_diff_enabled() {
        return false;
    }
    eprintln!(
        "skipping JVM differential test (set {}=1 to enable)",
        ENABLE_ENV
    );
    true
}

fn run_command(
    cwd: &Path,
    program: &str,
    args: &[OsString],
    extra_env: &[(&str, OsString)],
) -> Result<String, String> {
    let mut cmd = Command::new(program);
    cmd.current_dir(cwd);
    cmd.args(args);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }

    let output = cmd
        .output()
        .map_err(|err| format!("failed to run {program}: {err}"))?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "command failed: {program} {:?}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            args, output.status, stdout, stderr
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn ensure_jvm_harness() -> Result<&'static JvmHarness, String> {
    JVM_HARNESS
        .get_or_init(|| {
            let repo = repo_root();
            let rafka = rafka_root();
            let gradle_home = gradle_user_home();
            let work_dir = rafka.join("target/jvm-diff");
            let java_src_path = work_dir.join("RafkaJvmWireDump.java");
            let java_classes_dir = work_dir.join("classes");
            let init_script_path = work_dir.join("print_clients_classpath.init.gradle");

            fs::create_dir_all(&work_dir)
                .map_err(|err| format!("failed to create {}: {err}", work_dir.display()))?;
            fs::create_dir_all(&java_classes_dir)
                .map_err(|err| format!("failed to create {}: {err}", java_classes_dir.display()))?;

            fs::write(&java_src_path, java_helper_source()).map_err(|err| {
                format!(
                    "failed to write java helper {}: {err}",
                    java_src_path.display()
                )
            })?;
            fs::write(&init_script_path, classpath_init_script()).map_err(|err| {
                format!(
                    "failed to write init script {}: {err}",
                    init_script_path.display()
                )
            })?;

            let gradle_env = [("GRADLE_USER_HOME", gradle_home.as_os_str().to_os_string())];

            run_command(
                &repo,
                "./gradlew",
                &[
                    OsString::from("--no-daemon"),
                    OsString::from(":clients:classes"),
                    OsString::from("--console=plain"),
                ],
                &gradle_env,
            )?;

            let cp_output = run_command(
                &repo,
                "./gradlew",
                &[
                    OsString::from("--no-daemon"),
                    OsString::from("-q"),
                    OsString::from("-I"),
                    init_script_path.as_os_str().to_os_string(),
                    OsString::from(":clients:printMainRuntimeClasspath"),
                ],
                &gradle_env,
            )?;
            let runtime_cp = cp_output
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .find(|line| line.contains("/clients/build/classes/java/main"))
                .ok_or_else(|| {
                    format!(
                        "failed to parse clients runtime classpath from gradle output:\n{}",
                        cp_output
                    )
                })?;

            run_command(
                &repo,
                "javac",
                &[
                    OsString::from("-cp"),
                    OsString::from(runtime_cp),
                    OsString::from("-d"),
                    java_classes_dir.as_os_str().to_os_string(),
                    java_src_path.as_os_str().to_os_string(),
                ],
                &[],
            )?;

            let classpath = format!("{}:{runtime_cp}", java_classes_dir.display());
            Ok(JvmHarness { classpath })
        })
        .as_ref()
        .map_err(Clone::clone)
}

fn java_wire_bytes(api: &str, scenario: &str, version: i16) -> Result<Vec<u8>, String> {
    let harness = ensure_jvm_harness()?;
    let output = run_command(
        &repo_root(),
        "java",
        &[
            OsString::from("-cp"),
            OsString::from(&harness.classpath),
            OsString::from("RafkaJvmWireDump"),
            OsString::from(api),
            OsString::from(scenario),
            OsString::from(version.to_string()),
        ],
        &[],
    )?;
    if output.trim().is_empty() {
        return Ok(Vec::new());
    }
    let hex = output
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .ok_or_else(|| "java helper produced no output".to_string())?;
    decode_hex(hex)
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len() / 2);
    let mut chars = input.as_bytes().iter().copied();
    while let Some(high) = chars.next() {
        let low = chars
            .next()
            .ok_or_else(|| format!("odd-length hex string: {input}"))?;
        let hi = hex_nibble(high).ok_or_else(|| format!("invalid hex: {input}"))?;
        let lo = hex_nibble(low).ok_or_else(|| format!("invalid hex: {input}"))?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn rust_api_versions_request(scenario: &str, version: i16) -> ApiVersionsRequest {
    match scenario {
        "min" if version >= 3 => ApiVersionsRequest {
            client_software_name: Some(String::new()),
            client_software_version: Some(String::new()),
        },
        "min" => ApiVersionsRequest::default(),
        "filled" if version >= 3 => ApiVersionsRequest {
            client_software_name: Some("rafka-rs".to_string()),
            client_software_version: Some("1.0.0".to_string()),
        },
        "unicode" if version >= 3 => ApiVersionsRequest {
            client_software_name: Some("rafka-cli".to_string()),
            client_software_version: Some("v1.beta".to_string()),
        },
        _ => ApiVersionsRequest::default(),
    }
}

fn rust_api_versions_response(scenario: &str, version: i16) -> ApiVersionsResponse {
    let mut api_keys = vec![ApiVersionsResponseApiVersion {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    }];
    if scenario == "dense" {
        api_keys.push(ApiVersionsResponseApiVersion {
            api_key: 0,
            min_version: 3,
            max_version: 13,
        });
    }
    let mut response = ApiVersionsResponse {
        error_code: 0,
        api_keys,
        throttle_time_ms: (version >= 1).then_some(123),
        supported_features: None,
        finalized_features_epoch: None,
        finalized_features: None,
        zk_migration_ready: None,
    };
    if scenario == "tagged" && version >= 3 {
        response.supported_features = Some(vec![ApiVersionsResponseSupportedFeatureKey {
            name: "metadata.version".to_string(),
            min_version: 1,
            max_version: 20,
        }]);
        response.finalized_features_epoch = Some(7);
        response.finalized_features = Some(vec![ApiVersionsResponseFinalizedFeatureKey {
            name: "metadata.version".to_string(),
            max_version_level: 20,
            min_version_level: 1,
        }]);
        response.zk_migration_ready = Some(true);
    }
    response
}

fn rust_topic_id(last_byte: u8) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id[15] = last_byte;
    id
}

fn rust_produce_request(scenario: &str, version: i16) -> ProduceRequest {
    let mut topic_data = vec![ProduceRequestTopicProduceData {
        name: (version <= 12).then_some("topic-a".to_string()),
        topic_id: (version >= 13).then_some(rust_topic_id(0x0a)),
        partition_data: vec![
            ProduceRequestPartitionProduceData {
                index: 0,
                records: if scenario == "payload" {
                    Some(vec![0, 1, 2, 3])
                } else {
                    None
                },
            },
            ProduceRequestPartitionProduceData {
                index: 1,
                records: Some(vec![]),
            },
        ],
    }];
    if scenario == "multi" {
        topic_data.push(ProduceRequestTopicProduceData {
            name: (version <= 12).then_some("topic-b".to_string()),
            topic_id: (version >= 13).then_some(rust_topic_id(0x0b)),
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 5,
                records: Some(vec![9, 8, 7]),
            }],
        });
    }

    ProduceRequest {
        transactional_id: if scenario == "min" {
            None
        } else {
            Some("tx-1".to_string())
        },
        acks: if scenario == "min" { 1 } else { -1 },
        timeout_ms: if scenario == "min" { 5_000 } else { 9_000 },
        topic_data,
    }
}

fn assert_jvm_matches_rust<T>(
    api: &str,
    scenario: &str,
    version: i16,
    message: &T,
) -> Result<(), String>
where
    T: VersionedCodec,
{
    let rust_bytes = message
        .encode(version)
        .map_err(|err| format!("rust encode failed for {api}/{scenario}/v{version}: {err}"))?;
    let jvm_bytes = java_wire_bytes(api, scenario, version)?;
    if rust_bytes != jvm_bytes {
        return Err(format!(
            "wire mismatch for {api}/{scenario}/v{version}\nrust: {:02x?}\njvm : {:02x?}",
            rust_bytes, jvm_bytes
        ));
    }
    Ok(())
}

#[test]
fn jvm_diff_api_versions_request_matrix() {
    if skip_if_disabled() {
        return;
    }

    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        let min = rust_api_versions_request("min", version);
        assert_jvm_matches_rust("apiversions-request", "min", version, &min).expect("jvm diff min");

        let filled = rust_api_versions_request("filled", version);
        assert_jvm_matches_rust("apiversions-request", "filled", version, &filled)
            .expect("jvm diff filled");
    }

    for version in 3..=API_VERSIONS_MAX_VERSION {
        let unicode = rust_api_versions_request("unicode", version);
        assert_jvm_matches_rust("apiversions-request", "unicode", version, &unicode)
            .expect("jvm diff unicode");
    }
}

#[test]
fn jvm_diff_api_versions_response_matrix() {
    if skip_if_disabled() {
        return;
    }

    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        let min = rust_api_versions_response("min", version);
        assert_jvm_matches_rust("apiversions-response", "min", version, &min)
            .expect("jvm diff min");

        let dense = rust_api_versions_response("dense", version);
        assert_jvm_matches_rust("apiversions-response", "dense", version, &dense)
            .expect("jvm diff dense");
    }

    for version in 3..=API_VERSIONS_MAX_VERSION {
        let tagged = rust_api_versions_response("tagged", version);
        assert_jvm_matches_rust("apiversions-response", "tagged", version, &tagged)
            .expect("jvm diff tagged");
    }
}

#[test]
fn jvm_diff_produce_request_matrix() {
    if skip_if_disabled() {
        return;
    }

    for version in PRODUCE_MIN_VERSION..=PRODUCE_MAX_VERSION {
        let min = rust_produce_request("min", version);
        assert_jvm_matches_rust("produce-request", "min", version, &min).expect("jvm diff min");

        let payload = rust_produce_request("payload", version);
        assert_jvm_matches_rust("produce-request", "payload", version, &payload)
            .expect("jvm diff payload");
    }

    for version in 9..=PRODUCE_MAX_VERSION {
        let multi = rust_produce_request("multi", version);
        assert_jvm_matches_rust("produce-request", "multi", version, &multi)
            .expect("jvm diff multi");
    }
}

fn classpath_init_script() -> &'static str {
    r#"
allprojects {
  afterEvaluate { project ->
    if (project.path == ":clients") {
      project.tasks.register("printMainRuntimeClasspath") {
        doLast {
          println project.sourceSets.main.runtimeClasspath.asPath
        }
      }
    }
  }
}
"#
}

fn java_helper_source() -> &'static str {
    r#"
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.internal.MemoryRecords;

public final class RafkaJvmWireDump {
    private RafkaJvmWireDump() {}

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: <api> <scenario> <version>");
        }
        String api = args[0];
        String scenario = args[1];
        short version = Short.parseShort(args[2]);
        byte[] bytes;
        switch (api) {
            case "apiversions-request":
                bytes = encodeApiVersionsRequest(scenario, version);
                break;
            case "apiversions-response":
                bytes = encodeApiVersionsResponse(scenario, version);
                break;
            case "produce-request":
                bytes = encodeProduceRequest(scenario, version);
                break;
            default:
                throw new IllegalArgumentException("unknown api: " + api);
        }
        System.out.println(toHex(bytes));
    }

    private static byte[] encodeApiVersionsRequest(String scenario, short version) {
        ApiVersionsRequestData data = new ApiVersionsRequestData();
        if (version >= 3) {
            if ("filled".equals(scenario)) {
                data.setClientSoftwareName("rafka-rs");
                data.setClientSoftwareVersion("1.0.0");
            } else if ("unicode".equals(scenario)) {
                data.setClientSoftwareName("rafka-cli");
                data.setClientSoftwareVersion("v1.beta");
            }
        }
        ByteBufferAccessor accessor = MessageUtil.toByteBufferAccessor(data, version);
        return MessageUtil.byteBufferToArray(accessor.buffer());
    }

    private static byte[] encodeApiVersionsResponse(String scenario, short version) {
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setErrorCode((short) 0);
        ApiVersionsResponseData.ApiVersionCollection apiKeys = new ApiVersionsResponseData.ApiVersionCollection(2);
        apiKeys.add(new ApiVersionsResponseData.ApiVersion().setApiKey((short) 18).setMinVersion((short) 0).setMaxVersion((short) 4));
        if ("dense".equals(scenario)) {
            apiKeys.add(new ApiVersionsResponseData.ApiVersion().setApiKey((short) 0).setMinVersion((short) 3).setMaxVersion((short) 13));
        }
        data.setApiKeys(apiKeys);

        if (version >= 1) {
            data.setThrottleTimeMs(123);
        }
        if (version >= 3 && "tagged".equals(scenario)) {
            ApiVersionsResponseData.SupportedFeatureKeyCollection supported =
                new ApiVersionsResponseData.SupportedFeatureKeyCollection(1);
            supported.add(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("metadata.version")
                .setMinVersion((short) 1)
                .setMaxVersion((short) 20));
            data.setSupportedFeatures(supported);

            data.setFinalizedFeaturesEpoch(7L);

            ApiVersionsResponseData.FinalizedFeatureKeyCollection finalized =
                new ApiVersionsResponseData.FinalizedFeatureKeyCollection(1);
            finalized.add(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("metadata.version")
                .setMaxVersionLevel((short) 20)
                .setMinVersionLevel((short) 1));
            data.setFinalizedFeatures(finalized);
            data.setZkMigrationReady(true);
        }

        ByteBufferAccessor accessor = MessageUtil.toByteBufferAccessor(data, version);
        return MessageUtil.byteBufferToArray(accessor.buffer());
    }

    private static byte[] encodeProduceRequest(String scenario, short version) {
        ProduceRequestData data = new ProduceRequestData();
        data.setTransactionalId("min".equals(scenario) ? null : "tx-1");
        data.setAcks("min".equals(scenario) ? (short) 1 : (short) -1);
        data.setTimeoutMs("min".equals(scenario) ? 5000 : 9000);

        ProduceRequestData.TopicProduceDataCollection topics =
            new ProduceRequestData.TopicProduceDataCollection("multi".equals(scenario) ? 2 : 1);
        topics.add(buildTopic(version, "topic-a", (byte) 0x0a, 0, scenario, false));
        if ("multi".equals(scenario)) {
            topics.add(buildTopic(version, "topic-b", (byte) 0x0b, 5, scenario, true));
        }
        data.setTopicData(topics);

        ByteBufferAccessor accessor = MessageUtil.toByteBufferAccessor(data, version);
        return MessageUtil.byteBufferToArray(accessor.buffer());
    }

    private static ProduceRequestData.TopicProduceData buildTopic(
        short version,
        String name,
        byte idLast,
        int partitionBase,
        String scenario,
        boolean isSecondMultiTopic
    ) {
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData();
        if (version <= 12) {
            topic.setName(name);
        } else {
            topic.setTopicId(topicId(idLast));
        }

        List<ProduceRequestData.PartitionProduceData> partitions = new ArrayList<>();
        MemoryRecords firstRecords = null;
        if ("payload".equals(scenario)) {
            firstRecords = MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
        } else if ("multi".equals(scenario) && isSecondMultiTopic) {
            firstRecords = MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[] {9, 8, 7}));
        }
        ProduceRequestData.PartitionProduceData first = new ProduceRequestData.PartitionProduceData()
            .setIndex(partitionBase)
            .setRecords(firstRecords);
        partitions.add(first);

        if (partitionBase == 0) {
            partitions.add(new ProduceRequestData.PartitionProduceData()
                .setIndex(1)
                .setRecords(MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[] {}))));
        }

        topic.setPartitionData(partitions);
        return topic;
    }

    private static Uuid topicId(byte lastByte) {
        return new Uuid(0L, (long) (lastByte & 0xff));
    }

    private static String toHex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }
}
"#
}
