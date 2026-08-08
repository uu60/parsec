#!/usr/bin/env sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
OUTPUT_DIR=${1:-"$ROOT_DIR/dist"}

cd "$ROOT_DIR"
if [ -n "$(git status --porcelain=v1 --untracked-files=all)" ]; then
    echo "error: refusing to package a dirty or untracked worktree" >&2
    exit 2
fi

COMMIT=$(git rev-parse HEAD)
SHORT_COMMIT=$(git rev-parse --short=12 HEAD)
NAME="parsecdb-artifact-$SHORT_COMMIT"
mkdir -p "$OUTPUT_DIR"
ARCHIVE="$OUTPUT_DIR/$NAME.tar.gz"

git archive --format=tar.gz --prefix="$NAME/" --output="$ARCHIVE" "$COMMIT"
if ! tar -xOf "$ARCHIVE" "$NAME/artifact/experiments.yaml" | grep -F "$COMMIT" >/dev/null; then
    echo "error: archived experiments.yaml does not identify commit $COMMIT" >&2
    exit 3
fi
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$ARCHIVE" > "$ARCHIVE.sha256"
else
    shasum -a 256 "$ARCHIVE" > "$ARCHIVE.sha256"
fi

echo "Created $ARCHIVE"
echo "Commit $COMMIT"
cat "$ARCHIVE.sha256"
