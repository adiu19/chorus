#!/usr/bin/env bash
set -euo pipefail

# ───────────────────────────────────────────────────────────────────────────
# fuse-cas-m0: measure FUSE overhead vs tmpfs baseline.
# Sweeps concurrency, records JSONL of percentiles.
# ───────────────────────────────────────────────────────────────────────────

BLOBS=${BLOBS:-1000}
BLOB_SIZE=${BLOB_SIZE:-4096}
SEED=${SEED:-42}
OPS=${OPS:-1000}
TRIALS=${TRIALS:-10}
CONCURRENCY_LEVELS=${CONCURRENCY_LEVELS:-"1 4 16 64 256"}

FUSE_MOUNT=${FUSE_MOUNT:-/tmp/cas-fuse}
TMPFS_DIR=${TMPFS_DIR:-/dev/shm/cas-baseline}
RESULTS_DIR=${RESULTS_DIR:-"$(dirname "$0")/results"}
RESULTS_FILE="$RESULTS_DIR/results-$(date +%Y%m%d-%H%M%S).jsonl"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

mkdir -p "$RESULTS_DIR" "$FUSE_MOUNT" "$TMPFS_DIR"

cleanup() {
    if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    if mount | grep -q " $FUSE_MOUNT "; then
        umount "$FUSE_MOUNT" 2>/dev/null || true
    fi
    rm -rf "$TMPFS_DIR"
}
trap cleanup EXIT

echo "[run.sh] building binaries..."
(cd "$REPO_ROOT" && go build -o ./bin/m0-daemon ./fuse/cmd/m0-daemon)
(cd "$REPO_ROOT" && go build -o ./bin/fuse-loadgen ./cmd/fuse-loadgen)
(cd "$REPO_ROOT" && go build -o ./bin/populate-tmpfs ./cmd/populate-tmpfs)

echo "[run.sh] starting FUSE daemon at $FUSE_MOUNT"
"$REPO_ROOT/bin/m0-daemon" \
    -mount "$FUSE_MOUNT" \
    -blobs "$BLOBS" \
    -size "$BLOB_SIZE" \
    -seed "$SEED" \
    > "$RESULTS_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!

for i in 1 2 3 4 5; do
    if ls "$FUSE_MOUNT" >/dev/null 2>&1; then break; fi
    sleep 1
done
ls "$FUSE_MOUNT" > /dev/null

echo "[run.sh] populating tmpfs baseline at $TMPFS_DIR"
"$REPO_ROOT/bin/populate-tmpfs" -dir "$TMPFS_DIR" -blobs "$BLOBS" -size "$BLOB_SIZE" -seed "$SEED"

echo "[run.sh] sweeping concurrency=[$CONCURRENCY_LEVELS] x trials=$TRIALS"
for c in $CONCURRENCY_LEVELS; do
    for t in $(seq 1 "$TRIALS"); do
        echo "  - c=$c trial=$t FUSE"
        "$REPO_ROOT/bin/fuse-loadgen" \
            -mount "$FUSE_MOUNT" -concurrency "$c" -ops "$OPS" \
            -blobs "$BLOBS" -size "$BLOB_SIZE" -seed "$SEED" -trial "$t" \
            -label fuse -out "$RESULTS_FILE"
        echo "  - c=$c trial=$t tmpfs"
        "$REPO_ROOT/bin/fuse-loadgen" \
            -mount "$TMPFS_DIR" -concurrency "$c" -ops "$OPS" \
            -blobs "$BLOBS" -size "$BLOB_SIZE" -seed "$SEED" -trial "$t" \
            -label tmpfs -out "$RESULTS_FILE"
    done
done

echo "[run.sh] results written to $RESULTS_FILE"
echo "[run.sh] next: experiments/fuse-cas-m0/analyze.py $RESULTS_FILE"
