#!/bin/sh
# Run a Parsec executable as three ranks on this machine.
#
# Usage: tools/run-local.sh [--comm mpi|tcp] [--base-port PORT] EXECUTABLE [ARGS...]
#
#   --comm mpi   start the ranks with mpirun (default)
#   --comm tcp   start three independent processes with the TCP backend
#   --base-port  first TCP port; rank r listens on PORT + r (default 18000)
#
# Extra mpirun flags can be passed through PARSEC_MPIRUN_FLAGS. With Open MPI, --oversubscribe is added
# automatically so three ranks also start on machines with fewer cores.
set -eu

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd -P)
COMM=mpi
BASE_PORT=18000

usage() {
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
  exit "$1"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --comm) COMM=${2:?--comm needs a value}; shift 2 ;;
    --comm=*) COMM=${1#--comm=}; shift ;;
    --base-port) BASE_PORT=${2:?--base-port needs a value}; shift 2 ;;
    --base-port=*) BASE_PORT=${1#--base-port=}; shift ;;
    -h|--help) usage 0 ;;
    --) shift; break ;;
    -*) echo "unknown option: $1" >&2; usage 1 ;;
    *) break ;;
  esac
done

[ "$#" -ge 1 ] || usage 1
EXE=$1
shift

case "$COMM" in
  mpi)
    MPIRUN=${MPIRUN:-mpirun}
    FLAGS=${PARSEC_MPIRUN_FLAGS:-}
    # Open MPI refuses to start more ranks than detected cores unless oversubscription is allowed.
    case "$(readlink -f "$(command -v "$MPIRUN")" 2>/dev/null) $("$MPIRUN" --version 2>&1 || true)" in
      *openmpi*|*"Open MPI"*|*OpenRTE*|*PRRTE*|*prterun*|*schizo-ompi*) FLAGS="--oversubscribe $FLAGS" ;;
    esac
    # shellcheck disable=SC2086
    exec "$MPIRUN" $FLAGS -np 3 "$EXE" "$@"
    ;;
  tcp)
    exec python3 "$SCRIPT_DIR/run_tcp_ranks.py" --base-port "$BASE_PORT" "$EXE" -- "$@"
    ;;
  *)
    echo "--comm must be mpi or tcp, got: $COMM" >&2
    exit 1
    ;;
esac
