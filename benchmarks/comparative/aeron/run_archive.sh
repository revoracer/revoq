#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Aeron Archive comparative driver → semantic group journal_replayable,
# durability archive. Runs the canonical workload (../workload.md) across a
# payload-size x offered-rate sweep and emits a JSON array of
# ../schema.md-conformant objects to --out.
#
# For each point it launches THREE processes over a shared aeron dir:
#   ArchivingMediaDriver (driver + archive), consumer (live subscriber, latency),
#   producer (starts a LOCAL recording, then pace-publishes). See README for the
#   honest statement of archive semantics (consumer reads the live stream while
#   recording is active; it does not replay from disk).
#
# Usage:
#   run_archive.sh [--sizes "32 64 256 1024"] [--rates "100000 500000 1000000 0"]
#                  [--out PATH] [--path DIR] [--messages N] [--warmup N]
#                  [--producer-core N] [--consumer-core N]
#                  [--driver-cores "c1,c2,c3,..."] [--driver-threading DEDICATED|SHARED]
#                  [--rt-priority N]
#   Note: --durability is accepted and ignored; Aeron Archive is always `archive`.
#   The ArchivingMediaDriver has MANY busy-spin threads (media + archive); give
#   --driver-cores a generous isolated set disjoint from producer/consumer. See
#   run_ipc.sh header and README "Pinning & isolation".
#
# Environment overrides:
#   AERON_VERSION     value forced into build (-PaeronVersion=...)
#   AERON_JVM_FLAGS   extra JVM flags appended to every process
#   AERON_CTRL_PORT   base UDP port for the archive control channel (default 8010)
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
driver_cores=""            # taskset core list for the ArchivingMediaDriver PROCESS
rt_priority=""             # chrt -f N for every aeron process (needs CAP_SYS_NICE)
threading="DEDICATED"
ctrl_base="${AERON_CTRL_PORT:-8010}"

while [ $# -gt 0 ]; do
  case "$1" in
    --sizes) sizes="$2"; shift ;;
    --rates) rates="$2"; shift ;;
    --out) out="$2"; shift ;;
    --path) path="$2"; shift ;;
    --durability) shift ;;   # accepted for a uniform CLI; Archive is always archive
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
echo "  aeron-archive: building (gradle installDist) ..." >&2
build_args=(installDist -q)
[ -n "${AERON_VERSION:-}" ] && build_args+=("-PaeronVersion=${AERON_VERSION}")
( cd "$HERE" && "$GRADLE" "${build_args[@]}" )

libdir="$HERE/build/install/aeron-bench/lib"
if [ ! -d "$libdir" ]; then
  echo "error: build did not produce $libdir" >&2
  exit 1
fi
CP="$libdir/*"

jvm_flags=(
  -Dagrona.disable.bounds.checks=true
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/java.util.zip=ALL-UNNAMED
  --add-opens java.base/java.io=ALL-UNNAMED
)
# shellcheck disable=SC2206
[ -n "${AERON_JVM_FLAGS:-}" ] && jvm_flags+=(${AERON_JVM_FLAGS})

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Build PREFIX=(taskset -c <cores>  chrt -f <prio>) for a launched process. The
# ArchivingMediaDriver has even more busy-spin threads than a plain driver
# (media conductor/sender/receiver + archive conductor/recorder/replayer), so
# give --driver-cores a generous isolated set disjoint from producer/consumer.
build_prefix() {
  PREFIX=()
  [ -n "$1" ] && PREFIX+=(taskset -c "$1")
  [ -n "$rt_priority" ] && PREFIX+=(chrt -f "$rt_priority")
  return 0
}

wait_for_driver() {  # wait_for_driver <aeron_dir>
  local d="$1" i=0
  while [ ! -f "$d/cnc.dat" ]; do
    i=$((i + 1)); [ "$i" -gt 200 ] && { echo "error: archiving media driver did not come up ($d)" >&2; return 1; }
    sleep 0.05
  done
}

idx=0
for size in $sizes; do
  for rate in $rates; do
    if [ "$rate" -eq 0 ]; then interval=0; else interval=$(( 1000000000 / rate )); fi
    ctrl_port=$(( ctrl_base + idx ))
    control="aeron:udp?endpoint=localhost:${ctrl_port}"
    response="aeron:udp?endpoint=localhost:0"

    if [ -n "$path" ]; then
      aeron_dir="$path/aeron_${size}_${rate}_$$"; archive_dir="$path/archive_${size}_${rate}_$$"
    else
      aeron_dir="$tmp/aeron_${idx}"; archive_dir="$tmp/archive_${idx}"
    fi
    rm -rf "$aeron_dir" "$archive_dir"; mkdir -p "$archive_dir"

    echo "  aeron-archive: size=${size}B rate=${rate} msg/s (interval_ns=${interval}) ..." >&2

    # 1) ArchivingMediaDriver (media driver + archive recorder), pinned to --driver-cores
    build_prefix "$driver_cores"
    "${PREFIX[@]}" java "${jvm_flags[@]}" \
      -Daeron.dir="$aeron_dir" -Daeron.threading.mode="$threading" \
      -Daeron.archive.dir="$archive_dir" \
      -Daeron.archive.control.channel="$control" \
      -Daeron.archive.replication.channel="aeron:udp?endpoint=localhost:0" \
      -cp "$CP" io.aeron.archive.ArchivingMediaDriver \
      >/dev/null 2>"$tmp/driver_${idx}.err" &
    md_pid=$!
    if ! wait_for_driver "$aeron_dir"; then cat "$tmp/driver_${idx}.err" >&2; kill "$md_pid" 2>/dev/null || true; exit 1; fi

    build_prefix "$consumer_core"
    cons_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.AeronArchiveBench consumer
              --aeron-dir "$aeron_dir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --out "$tmp/consumer_${idx}.out")
    build_prefix "$producer_core"
    prod_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.AeronArchiveBench producer
              --aeron-dir "$aeron_dir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --interval-ns "$interval" --archive-control "$control" --archive-response "$response"
              --out "$tmp/producer_${idx}.out")

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
    rm -rf "$aeron_dir" "$archive_dir"
    idx=$((idx + 1))
  done
done

python3 "$HERE/_merge.py" "$tmp" "$idx" "aeron-archive" "archive" "$warmup" "$out"
