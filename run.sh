TID="test_interceptor_dir"
rm -rf /tmp/$TID
RUSTFLAGS="-Awarnings" timeout 5 cargo run -- --cluster-id $TID
