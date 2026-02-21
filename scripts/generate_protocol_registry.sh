#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAFKA_DIR="${SCRIPT_DIR}/.."

if [[ -d "${RAFKA_DIR}/clients/src/main/resources/common/message" ]]; then
  INPUT_DIR="${RAFKA_DIR}/clients/src/main/resources/common/message"
elif [[ -d "${RAFKA_DIR}/../clients/src/main/resources/common/message" ]]; then
  INPUT_DIR="${RAFKA_DIR}/../clients/src/main/resources/common/message"
else
  echo "rafka-codegen failed: Kafka message spec directory not found" >&2
  echo "checked:" >&2
  echo "  ${RAFKA_DIR}/clients/src/main/resources/common/message" >&2
  echo "  ${RAFKA_DIR}/../clients/src/main/resources/common/message" >&2
  exit 2
fi

OUTPUT_FILE="${RAFKA_DIR}/crates/protocol/src/generated_api_registry.rs"

: "${CARGO_HOME:=${RAFKA_DIR}/.cargo-home}"
: "${CARGO_TARGET_DIR:=${RAFKA_DIR}/target}"
export CARGO_HOME
export CARGO_TARGET_DIR

cargo run -p rafka-codegen -- \
  --input "${INPUT_DIR}" \
  --output "${OUTPUT_FILE}"
