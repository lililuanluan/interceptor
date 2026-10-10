LOGS="./logs"
mkdir -p $LOGS
TID="test_interceptor_dir"
rm -rf $LOGS/$TID
RUSTFLAGS="-Awarnings" timeout 10 cargo run -- --cluster-id $TID --log-root $LOGS
