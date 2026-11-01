#!/bin/bash

#!/bin/bash

set -euo pipefail
cd -- "$(dirname -- "$0")"

mkdir -p ./logs

RUSTFLAGS="-Awarnings" cargo build --release

exec python3 -u controller/main.py

