TID="test_interceptor_dir"
rm -rf /tmp/$TID
RUSTFLAGS="-Awarnings" cargo run -- --cluster-id $TID



