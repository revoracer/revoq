#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Aeron IPC comparative driver → semantic group volatile_ipc, durability none.
# Runs the canonical workload (../workload.md) across a payload-size x offered-
# rate sweep and emits a JSON array of ../schema.md-conformant objects to --out.
#
# For each point it launches THREE processes over a shared MediaDriver aeron dir:
#   MediaDriver (standalone), consumer (latency + HdrHistogram), producer (paced).
# The driver is torn down after each point so ports/dirs are clean.
#
# Usage:
#   run_ipc.sh [--sizes "32 64 256 1024"] [--rates "100000 500000 1000000 0"]
#              [--out PATH] [--path DIR] [--messages N] [--warmup N]
#              [--producer-core N] [--consumer-core N]
#              [--driver-cores "c1,c2,c3"] [--driver-threading DEDICATED|SHARED]
#              [--rt-priority N]
#   Note: --durability is accepted and ignored; Aeron IPC is always `none`.
#
# Tuning for clean tails (see README "Pinning & isolation"): pin producer,
# consumer, AND the media driver to DISJOINT cores, give the whole set to the
# kernel via isolcpus/nohz_full, disable turbo/C-states, and run under sudo with
# --rt-priority. Pinning busy-spin threads to non-isolated cores, or pinning
# producer/consumer but not the driver, makes the tail WORSE, not better.
#
# Environment overrides:
#   AERON_VERSION     value forced into build (-PaeronVersion=...)
#   AERON_JVM_FLAGS   extra JVM flags appended to every process
#   GRADLE            gradle launcher (default: gradle)
#
# Requires: JDK 17+, Gradle 8+, network access on first build (Maven Central).

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"

sizes="32 64 256 1024"
rates="100000 500000 1000000 0"
out="/dev/stdout"
path=""
messages="${AERON_MESSAGES:-5000000}"
warmup="${AERON_WARMUP:-200000}"
producer_core=""
consumer_core=""
driver_cores=""            # taskset core list for the media driver PROCESS
rt_priority=""             # chrt -f N for every aeron process (needs CAP_SYS_NICE)
threading="DEDICATED"      # DEDICATED (3 busy-spin driver threads) | SHARED (1)

while [ $# -gt 0 ]; do
  case "$1" in
    --sizes) sizes="$2"; shift ;;
    --rates) rates="$2"; shift ;;
    --out) out="$2"; shift ;;
    --path) path="$2"; shift ;;
    --durability) shift ;;   # accepted for a uniform CLI; IPC is always none
    --messages) messages="$2"; shift ;;
    --warmup) warmup="$2"; shift ;;
    --producer-core) producer_core="$2"; shift ;;
    --consumer-core) consumer_core="$2"; shift ;;
    --driver-cores) driver_cores="$2"; shift ;;
    --rt-priority) rt_priority="$2"; shift ;;
    --driver-threading) threading="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

# Build PREFIX=(taskset -c <cores>  chrt -f <prio>) for a launched process.
# IMPORTANT on this class of box: in DEDICATED mode the media driver runs 3
# busy-spin threads; if you pin producer/consumer but leave the driver floating,
# a driver thread will periodically land on a pinned busy-spin core and cause
# ms-scale stalls. Give --driver-cores its own isolated set (>= 3 cores for
# DEDICATED, or use --driver-threading SHARED to fold it to 1 thread).
build_prefix() {
  PREFIX=()
  [ -n "$1" ] && PREFIX+=(taskset -c "$1")
  [ -n "$rt_priority" ] && PREFIX+=(chrt -f "$rt_priority")
  return 0
}

for s in $sizes; do
  case "$s" in
    32|64|256|1024) ;;
    *) echo "error: unsupported payload size '$s' (allowed: 32 64 256 1024)" >&2; exit 2 ;;
  esac
done

GRADLE="${GRADLE:-gradle}"
if ! command -v "$GRADLE" >/dev/null 2>&1; then
  echo "error: '$GRADLE' not found. Install Gradle 8+ (and a JDK 17+), or set GRADLE=." >&2
  exit 1
fi

# --- build once -------------------------------------------------------------
echo "  aeron-ipc: building (gradle installDist) ..." >&2
build_args=(installDist -q)
[ -n "${AERON_VERSION:-}" ] && build_args+=("-PaeronVersion=${AERON_VERSION}")
( cd "$HERE" && "$GRADLE" "${build_args[@]}" )

libdir="$HERE/build/install/aeron-bench/lib"
if [ ! -d "$libdir" ]; then
  echo "error: build did not produce $libdir" >&2
  exit 1
fi
CP="$libdir/*"

# Aeron's recommended JDK 16+ opens (direct buffers / cleaner) plus bounds-check
# off for the hot path. Extra flags via AERON_JVM_FLAGS.
jvm_flags=(
  -Dagrona.disable.bounds.checks=true
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/java.util.zip=ALL-UNNAMED
)
# shellcheck disable=SC2206
[ -n "${AERON_JVM_FLAGS:-}" ] && jvm_flags+=(${AERON_JVM_FLAGS})

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

wait_for_driver() {  # wait_for_driver <aeron_dir>
  local d="$1" i=0
  while [ ! -f "$d/cnc.dat" ]; do
    i=$((i + 1)); [ "$i" -gt 200 ] && { echo "error: media driver did not come up ($d)" >&2; return 1; }
    sleep 0.05
  done
}

idx=0
for size in $sizes; do
  for rate in $rates; do
    if [ "$rate" -eq 0 ]; then interval=0; else interval=$(( 1000000000 / rate )); fi

    if [ -n "$path" ]; then aeron_dir="$path/aeron_${size}_${rate}_$$"; else aeron_dir="$tmp/aeron_${idx}"; fi
    rm -rf "$aeron_dir"

    echo "  aeron-ipc: size=${size}B rate=${rate} msg/s (interval_ns=${interval}) ..." >&2

    # 1) standalone MediaDriver (pinned to --driver-cores if given)
    build_prefix "$driver_cores"
    "${PREFIX[@]}" java "${jvm_flags[@]}" -Daeron.dir="$aeron_dir" \
      -Daeron.threading.mode="$threading" \
      -cp "$CP" io.aeron.driver.MediaDriver >/dev/null 2>"$tmp/driver_${idx}.err" &
    md_pid=$!
    if ! wait_for_driver "$aeron_dir"; then cat "$tmp/driver_${idx}.err" >&2; kill "$md_pid" 2>/dev/null || true; exit 1; fi

    build_prefix "$consumer_core"
    cons_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.AeronIpcBench consumer
              --aeron-dir "$aeron_dir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --out "$tmp/consumer_${idx}.out")
    build_prefix "$producer_core"
    prod_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.AeronIpcBench producer
              --aeron-dir "$aeron_dir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --interval-ns "$interval" --out "$tmp/producer_${idx}.out")

    # Consumer (subscription) first, then producer.
    "${cons_cmd[@]}" >/dev/null 2>"$tmp/consumer_${idx}.err" &
    cons_pid=$!
    "${prod_cmd[@]}" >/dev/null 2>"$tmp/producer_${idx}.err" &
    prod_pid=$!

    prod_ok=1; cons_ok=1
    wait "$prod_pid" || prod_ok=0
    wait "$cons_pid" || cons_ok=0
    kill "$md_pid" 2>/dev/null || true
    wait "$md_pid" 2>/dev/null || true

    if [ "$prod_ok" = 0 ]; then echo "  ! producer failed:" >&2; cat "$tmp/producer_${idx}.err" >&2; exit 1; fi
    if [ "$cons_ok" = 0 ]; then echo "  ! consumer failed:" >&2; cat "$tmp/consumer_${idx}.err" >&2; exit 1; fi

    printf '%s\n' "$rate" > "$tmp/rate_${idx}.txt"
    rm -rf "$aeron_dir"
    idx=$((idx + 1))
  done
done

python3 "$HERE/_merge.py" "$tmp" "$idx" "aeron-ipc" "none" "$warmup" "$out"
