#!/bin/bash

set -e
LOGS="./logs"
mkdir -p $LOGS
TID="test_interceptor_dir"
rm -rf $LOGS/$TID
RUSTFLAGS="-Awarnings" cargo build --release
timeout --foreground 100 ./target/release/interceptor-dev --cluster-id $TID --log-root $LOGS

