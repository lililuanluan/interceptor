#!/bin/bash
# Remove unused imports in one command. This edits source files; review git diff afterward.
#
# Usage: ./clear_unused_imports.sh [-y]
#   -y  Skip confirmation when the working tree has uncommitted changes (otherwise prompt).
#
# Pitfalls encountered in testing; do not change these settings casually:
#
# 1) RUSTFLAGS must be "-Aunused -Wunused_imports"; use export to override it explicitly.
#    - With "-Awarnings", warnings are capped at allow and a later -W does not re-enable them.
#      cargo fix gets no suggestions and does nothing (observed: zero diagnostics and no file changes).
#    - Plain cargo fix also renames unused variables to _xxx and applies other lint fixes;
#      this script is only for imports.
#    - If RUSTFLAGS is already exported (e.g. flags like those in run.sh), appending does not help;
#      override it instead.
#
# 2) --allow-dirty / --allow-staged: cargo fix refuses to run with uncommitted or staged changes
#    by default (this repository usually has such changes, so these flags are required).
#
# 3) Changing RUSTFLAGS triggers a full rebuild, so a slow first run is normal.

set -euo pipefail
cd "$(dirname "$0")"

# With a dirty working tree, restoring the pre-run state is not straightforward:
# git restore would also discard your pre-existing changes.
if [ -n "$(git status --porcelain)" ] && [ "${1:-}" != "-y" ]; then
	echo "⚠ 工作区有未提交的改动：清理后想回退会很麻烦（git restore 会连你原有的改动一起丢）。"
	echo "  建议先 git commit 或 git stash，或者用 -y 跳过这个检查。"
	read -r -p "  仍然继续？[y/N] " answer
	[ "$answer" = "y" ] || {
		echo "已取消"
		exit 1
	}
fi

# Override rather than append: once -Awarnings takes effect, warnings cannot be re-enabled this way.
export RUSTFLAGS="-Aunused -Wunused_imports"

echo "==> cargo fix --all-targets（lib / bin / tests / examples）"
cargo fix --allow-dirty --allow-staged --all-targets

echo
