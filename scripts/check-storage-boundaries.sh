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

check_max_lines() {
  local path="$1"
  local max="$2"
  local lines
  lines="$(wc -l < "$path" | tr -d ' ')"
  if [ "$lines" -gt "$max" ]; then
    echo
    echo "Storage boundary violation: $path has $lines lines; split before it exceeds $max."
    fail=1
  fi
}

check_max_lines src/postgres/routing_index.ts 300
check_max_lines src/postgres/secondary_index.ts 240
check_max_lines src/postgres/lexicon_index.ts 260
check_max_lines src/postgres/companions.ts 220
check_max_lines src/postgres/rows.ts 180
check_max_lines src/postgres/details.ts 240

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Storage boundary checks passed."
