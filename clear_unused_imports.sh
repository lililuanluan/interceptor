#!/bin/bash
# 一键清理未使用的 import。会直接改源文件，跑完请 git diff 复查。
#
# 用法: ./clear_unused_imports.sh [-y]
#   -y  工作区有未提交改动时不再询问（默认会停下来确认）
#
# 几个实测踩过的坑，别随手改掉：
#
# 1) RUSTFLAGS 必须是 "-Aunused -Wunused_imports"，而且要用 export 强制覆盖。
#    - 用 "-Awarnings"：它把警告整个封顶成 allow，之后再 -W 也打不开，
#      cargo fix 拿不到任何建议 → 空转（实测 0 条诊断，文件一字不改）。
#    - 裸 cargo fix：会顺手把未使用的变量改成 _xxx、动别的 lint；这个脚本只负责 import。
#    - 如果环境里已经 export 了 RUSTFLAGS（比如 run.sh 里那种），追加是没用的，必须覆盖。
#
# 2) --allow-dirty / --allow-staged：工作区有未提交或已暂存的改动时，
#    cargo fix 默认拒绝运行（这个仓库平时就是脏的，所以必须带上）。
#
# 3) RUSTFLAGS 变了会导致一次全量重编译，第一次跑慢是正常的。

set -euo pipefail
cd "$(dirname "$0")"

# 工作区脏的时候，清理完就没法简单回退到"运行前"的状态了（git restore 会连你原有的改动一起丢）
if [ -n "$(git status --porcelain)" ] && [ "${1:-}" != "-y" ]; then
	echo "⚠ 工作区有未提交的改动：清理后想回退会很麻烦（git restore 会连你原有的改动一起丢）。"
	echo "  建议先 git commit 或 git stash，或者用 -y 跳过这个检查。"
	read -r -p "  仍然继续？[y/N] " answer
	[ "$answer" = "y" ] || {
		echo "已取消"
		exit 1
	}
fi

# 强制覆盖（不是追加）：-Awarnings 一旦生效就再也打不开了
export RUSTFLAGS="-Aunused -Wunused_imports"

echo "==> cargo fix --all-targets（lib / bin / tests / examples）"
cargo fix --allow-dirty --allow-staged --all-targets

echo
