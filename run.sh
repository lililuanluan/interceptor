TID="test_interceptor_dir"
rm -rf /tmp/$TID
RUSTFLAGS="-Awarnings" timeout 10 cargo run -- --cluster-id $TID
