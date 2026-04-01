# Vendored Kafka message specifications

These JSON message definition files are vendored from the Apache Kafka
source tree (`clients/src/main/resources/common/message/`, `4.1.0` (vendored
2026-04) so that protocol code generation and the spec-dependent tests are
fully self-contained and reproducible without a Kafka checkout.

- Source: https://github.com/apache/kafka/tree/4.1.0/clients/src/main/resources/common/message
- License: Apache License 2.0 (see `KAFKA_NOTICE` and the repository `LICENSE` note below)
- Count: 203 message definitions (requests, responses, header versions, data,
  metadata, and coordinator record types)

The generator scripts (`scripts/generate_protocol_messages.sh` and
`scripts/generate_protocol_registry.sh`) prefer this directory and fall back
to a sibling Kafka checkout (`../clients/...`) when present.

To refresh: copy the contents of `clients/src/main/resources/common/message`
from the desired Apache Kafka revision into this directory and rerun the
generator scripts, then commit any regenerated artifacts.
