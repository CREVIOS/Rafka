#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

: "${CARGO_HOME:=${PWD}/.cargo-home}"
: "${CARGO_TARGET_DIR:=${PWD}/target}"
: "${GRADLE_USER_HOME:=${PWD}/.gradle-home}"
export CARGO_HOME
export CARGO_TARGET_DIR
export GRADLE_USER_HOME
export RAFKA_ENABLE_JVM_DIFF=1

cargo test -p rafka-protocol --test jvm_differential -- --nocapture
