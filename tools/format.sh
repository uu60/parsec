#!/bin/sh
# Format the C++ sources with the repository .clang-format, or check them.
#
# Usage: tools/format.sh [--check]
#
# CLANG_FORMAT selects the binary (default: clang-format-21 if present, else clang-format). CI uses clang-format 21;
# other major versions may format some constructs differently.
set -eu

cd "$(dirname -- "$0")/.."

if [ -z "${CLANG_FORMAT:-}" ]; then
  if command -v clang-format-21 >/dev/null 2>&1; then
    CLANG_FORMAT=clang-format-21
  else
    CLANG_FORMAT=clang-format
  fi
fi

files=$(git ls-files '*.h' '*.hpp' '*.cpp' | grep -v '/third_party/' || true)
[ -n "$files" ] || exit 0

if [ "${1:-}" = "--check" ]; then
  # shellcheck disable=SC2086
  exec "$CLANG_FORMAT" --dry-run --Werror --style=file $files
fi
# shellcheck disable=SC2086
exec "$CLANG_FORMAT" -i --style=file $files
