#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)

if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
    exit 2
fi

"$SCRIPT_DIR/run.sh" doctor
"$SCRIPT_DIR/run.sh" smoke
"$SCRIPT_DIR/run.sh" figure2 --skip-build
"$SCRIPT_DIR/run.sh" figure4 --skip-build
"$SCRIPT_DIR/run.sh" figure5 --skip-build
"$SCRIPT_DIR/run.sh" figure7 --skip-build
"$SCRIPT_DIR/run.sh" figure8 --skip-build
"$SCRIPT_DIR/run.sh" table1 --skip-build

echo "All supported artifact workflows completed."
