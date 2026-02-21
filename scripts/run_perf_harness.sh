#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

: "${CARGO_HOME:=${PWD}/.cargo-home}"
: "${CARGO_TARGET_DIR:=${PWD}/target}"
export CARGO_HOME
export CARGO_TARGET_DIR

cargo run --release -p rafka-broker --bin perf_harness -- "$@"
