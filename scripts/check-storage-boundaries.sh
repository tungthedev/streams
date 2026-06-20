#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

fail=0

echo "Checking storage capability boundaries ..."

search() {
  local pattern="$1"
  shift
  if command -v rg >/dev/null 2>&1; then
    rg -n "$pattern" "$@"
  else
    grep -R -n -E "$pattern" "$@"
  fi
}

index_targets=(src/index src/search/companion_manager.ts src/store/index_store.ts)

if search 'SqliteDurableStore|from ["'\''][^"'\'']*(/|^)(db|sqlite)/[^"'\'']*["'\'']|from ["'\'']bun:sqlite["'\'']|SqliteDatabase|SqliteStatement|\.db\.(query|transaction|prepare|exec)|iterWalRange' "${index_targets[@]}"; then
  echo
  echo "Storage boundary violation: index/search capability modules must not depend on concrete SQLite APIs or legacy WAL iteration."
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Storage boundary checks passed."
