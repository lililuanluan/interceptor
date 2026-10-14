#!/bin/bash

set -e
LOGS="./logs"
mkdir -p $LOGS
TID="test_interceptor_dir"
rm -rf $LOGS/$TID
RUSTFLAGS="-Awarnings" cargo build --release
RUSTFLAGS="-Awarnings" timeout 100 cargo run --release -- --cluster-id $TID --log-root $LOGS
