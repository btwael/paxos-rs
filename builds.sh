#!/usr/bin/env bash
set -euo pipefail

cargo build --manifest-path examples/traceforge-paxos/Cargo.toml --release "$@"
