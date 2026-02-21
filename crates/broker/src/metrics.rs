#![forbid(unsafe_code)]

use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};

#[derive(Debug, Clone)]
pub struct TransportMetrics {
    registry: Registry,
    api_requests_total: IntCounterVec,
    api_responses_total: IntCounterVec,
    api_errors_total: IntCounterVec,
    partition_events_total: IntCounterVec,
    partition_bytes_total: IntCounterVec,
    sasl_auth_total: IntCounterVec,
    tls_handshake_total: IntCounterVec,
}

impl TransportMetrics {
    pub fn new() -> Result<Self, String> {
        let registry =
            Registry::new_custom(Some("rafka".to_string()), None).map_err(|err| err.to_string())?;

        let api_requests_total = IntCounterVec::new(
            Opts::new(
                "transport_api_requests_total",
                "Total Kafka wire requests by API key and version",
            ),
            &["api_key", "api_version"],
        )
        .map_err(|err| err.to_string())?;
        let api_responses_total = IntCounterVec::new(
            Opts::new(
                "transport_api_responses_total",
                "Total Kafka wire responses by API key and version",
            ),
            &["api_key", "api_version"],
        )
        .map_err(|err| err.to_string())?;
        let api_errors_total = IntCounterVec::new(
            Opts::new(
                "transport_api_errors_total",
                "Total Kafka wire request handling errors by API key, version and class",
            ),
            &["api_key", "api_version", "class"],
        )
        .map_err(|err| err.to_string())?;
        let partition_events_total = IntCounterVec::new(
            Opts::new(
                "transport_partition_events_total",
                "Per-partition produce/fetch event counters",
            ),
            &["direction", "topic", "partition"],
        )
        .map_err(|err| err.to_string())?;
        let partition_bytes_total = IntCounterVec::new(
            Opts::new(
                "transport_partition_bytes_total",
                "Per-partition produced/fetched payload bytes",
            ),
            &["direction", "topic", "partition"],
        )
        .map_err(|err| err.to_string())?;
        let sasl_auth_total = IntCounterVec::new(
            Opts::new(
                "transport_sasl_auth_total",
                "SASL authentication attempts by mechanism and result",
            ),
            &["mechanism", "result"],
        )
        .map_err(|err| err.to_string())?;
        let tls_handshake_total = IntCounterVec::new(
            Opts::new(
                "transport_tls_handshake_total",
                "TLS handshake attempts by result",
            ),
            &["result"],
        )
        .map_err(|err| err.to_string())?;

        registry
            .register(Box::new(api_requests_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(api_responses_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(api_errors_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(partition_events_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(partition_bytes_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(sasl_auth_total.clone()))
            .map_err(|err| err.to_string())?;
        registry
            .register(Box::new(tls_handshake_total.clone()))
            .map_err(|err| err.to_string())?;

        Ok(Self {
            registry,
            api_requests_total,
            api_responses_total,
            api_errors_total,
            partition_events_total,
            partition_bytes_total,
            sasl_auth_total,
            tls_handshake_total,
        })
    }

    pub fn record_api_request(&self, api_key: i16, api_version: i16) {
        let api_key = api_key.to_string();
        let api_version = api_version.to_string();
        self.api_requests_total
            .with_label_values(&[&api_key, &api_version])
            .inc();
    }

    pub fn record_api_response(&self, api_key: i16, api_version: i16) {
        let api_key = api_key.to_string();
        let api_version = api_version.to_string();
        self.api_responses_total
            .with_label_values(&[&api_key, &api_version])
            .inc();
    }

    pub fn record_api_error(&self, api_key: i16, api_version: i16, class: &str) {
        let api_key = api_key.to_string();
        let api_version = api_version.to_string();
        self.api_errors_total
            .with_label_values(&[&api_key, &api_version, class])
            .inc();
    }

    pub fn record_partition_event(
        &self,
        direction: &str,
        topic: &str,
        partition: i32,
        bytes: usize,
    ) {
        let partition = partition.to_string();
        self.partition_events_total
            .with_label_values(&[direction, topic, &partition])
            .inc();
        self.partition_bytes_total
            .with_label_values(&[direction, topic, &partition])
            .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    pub fn record_sasl_auth(&self, mechanism: &str, result: &str) {
        self.sasl_auth_total
            .with_label_values(&[mechanism, result])
            .inc();
    }

    pub fn record_tls_handshake(&self, result: &str) {
        self.tls_handshake_total.with_label_values(&[result]).inc();
    }

    pub fn render_prometheus(&self) -> Result<String, String> {
        let metric_families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut output)
            .map_err(|err| err.to_string())?;
        String::from_utf8(output).map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_payload_with_expected_metrics() {
        let metrics = TransportMetrics::new().expect("metrics");
        metrics.record_api_request(18, 4);
        metrics.record_api_response(18, 4);
        metrics.record_api_error(18, 99, "unsupported_version");
        metrics.record_partition_event("produce", "topic-a", 0, 128);
        metrics.record_partition_event("fetch", "topic-a", 0, 64);
        metrics.record_sasl_auth("PLAIN", "success");
        metrics.record_tls_handshake("success");

        let rendered = metrics.render_prometheus().expect("render");
        assert!(rendered.contains("rafka_transport_api_requests_total"));
        assert!(rendered.contains("rafka_transport_partition_bytes_total"));
        assert!(rendered.contains("rafka_transport_sasl_auth_total"));
        assert!(rendered.contains("rafka_transport_tls_handshake_total"));
    }
}
