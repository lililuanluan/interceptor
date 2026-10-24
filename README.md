# CometBFT Interceptor

A Rust prototype for running Dockerized CometBFT validators, intercepting and delaying complete P2P messages, monitoring blocks, and injecting test transactions.

## Usage

Requires Linux/WSL2, Rust/Cargo, and an accessible Docker daemon running Linux containers. Run from the repository root:

```sh
mkdir -p logs
cargo run --release -- \
  --num-nodes 4 \
  --max-block 15 \
  --log-root ./logs \
  --tx-config-yaml ./tx-example.yaml
```

- At least four nodes are required. The default image is `cometbft/cometbft:v0.40.0`; a missing image is pulled automatically.
- Docker assigns host RPC/P2P ports automatically. Validators connect through the interceptor in a full mesh. The current controller adds random delays of 0–200 ms to complete messages; Ping/Pong packets bypass this delay.
- The run stops when all nodes reach `--max-block`, or on Ctrl+C/SIGTERM. Shutdown removes validator containers and retains configuration and CSV logs.
- `--log-root` must already exist. `--cluster-id` defaults to the current timestamp; each run needs a new ID. Use `cargo run -- --help` for all options.

Outputs are stored under `<log-root>/<cluster-id>/`:

```text
config/node*/     Configuration and test keys
logs/blocks.csv   Observed blocks
logs/message.csv Forwarded messages
logs/tx.csv       Transaction submissions
```

Node runtime data stays inside containers. Generated files and directories are shared read/write, including test keys: use only in a trusted test environment.

### Transactions

Omit `--tx-config-yaml` to disable injection. The bundled `persistent_kvstore` application accepts key-value payloads:

```yaml
transactions:
  - node: 0
    after_heights: [3, 5, 8]
    kv: "name=alice"
    dt: 0.2
```

For each listed height observed on the zero-based `node`, submit to that node after `dt` seconds. Out-of-range nodes and heights outside `1 <= height < max_block` are skipped; duplicate heights are removed. Payloads receive a `[entry_index,height]` suffix to distinguish submissions (entry indices are assigned after filtering). Submission uses `broadcast_tx_sync`: CheckTx acceptance does not guarantee block inclusion.

## Modules

All Rust modules below are in `src/`.

| Modules | Responsibility |
| --- | --- |
| `main`, `configs`, `filepath` | Startup, options, run directories, and shutdown orchestration. |
| `docker`, `testnet`, `node` | Image checks, testnet generation, validator startup, and RPC/WebSocket access; use scripts in `scripts/`. |
| `network`, `p2p` | Full-mesh connections, authenticated encrypted handshakes, packet I/O, and interception tasks. |
| `message` | Reassemble incoming fragments and split complete messages for sending. |
| `controller` | Assign message delays; currently an in-process random-delay policy. |
| `forwarder` | Schedule messages with a deadline heap and send them to destination nodes. |
| `block_monitor`, `transaction` | Subscribe to block events, track target heights, and schedule YAML-defined transactions. |
| `logger`, `resource_manager` | Typed CSV logging and socket/container cleanup, respectively. |
