#!/bin/bash

set -e
LOGS="./logs"
mkdir -p $LOGS
TID="test_interceptor_dir"
rm -rf $LOGS/$TID
RUSTFLAGS="-Awarnings" cargo build
RUSTFLAGS="-Awarnings" timeout 100 cargo run -- --cluster-id $TID --log-root $LOGS
