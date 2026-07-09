#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Chronicle Queue OSS comparative driver. Runs the canonical workload
# (../workload.md) across a payload-size x offered-rate sweep and emits a JSON
# array of ../schema.md-conformant result objects (one per point) to --out.
#
# For each point it launches TWO processes over a shared Chronicle queue file:
#   consumer (latency + HdrHistogram, source of truth)  and
#   producer (paced open-loop appender, source of achieved_rate).
#
# Usage:
#   run.sh [--sizes "32 64 256 1024"] [--rates "100000 500000 1000000 0"]
#          [--out PATH] [--path DIR] [--durability page_cache|none|fsync]
#          [--messages N] [--warmup N]
#          [--producer-core N] [--consumer-core N] [--rt-priority N]
#   For clean tails: pin producer/consumer to disjoint isolated cores
#   (isolcpus/nohz_full), disable turbo/C-states, run under sudo with --rt-priority.
#
# Environment overrides:
#   CHRONICLE_VERSION     value forced into build (-PchronicleVersion=...)
#   CHRONICLE_JVM_FLAGS   extra JVM flags appended to both processes
#   GRADLE                gradle launcher (default: gradle)
#
# Requires: JDK 17+, Gradle 8+, network access on first build (Maven Central).
# durability_mode defaults to page_cache (Chronicle's mmap-to-page-cache path).
# Put --path on tmpfs/hugetlbfs AND pass --durability none for the volatile case.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"

sizes="32 64 256 1024"
rates="100000 500000 1000000 0"
out="/dev/stdout"
path=""
durability="page_cache"
messages="${CHRONICLE_MESSAGES:-5000000}"
warmup="${CHRONICLE_WARMUP:-200000}"
producer_core=""
consumer_core=""
rt_priority=""             # chrt -f N for producer/consumer (needs CAP_SYS_NICE)

while [ $# -gt 0 ]; do
  case "$1" in
    --sizes) sizes="$2"; shift ;;
    --rates) rates="$2"; shift ;;
    --out) out="$2"; shift ;;
    --path) path="$2"; shift ;;
    --durability) durability="$2"; shift ;;
    --messages) messages="$2"; shift ;;
    --warmup) warmup="$2"; shift ;;
    --producer-core) producer_core="$2"; shift ;;
    --consumer-core) consumer_core="$2"; shift ;;
    --rt-priority) rt_priority="$2"; shift ;;
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
case "$durability" in
  page_cache|none|fsync) ;;
  *) echo "error: --durability must be page_cache|none|fsync" >&2; exit 2 ;;
esac

GRADLE="${GRADLE:-gradle}"
if ! command -v "$GRADLE" >/dev/null 2>&1; then
  echo "error: '$GRADLE' not found. Install Gradle 8+ (and a JDK 17+), or set GRADLE=." >&2
  exit 1
fi

# --- build once -------------------------------------------------------------
echo "  chronicle: building (gradle installDist) ..." >&2
build_args=(installDist -q)
[ -n "${CHRONICLE_VERSION:-}" ] && build_args+=("-PchronicleVersion=${CHRONICLE_VERSION}")
( cd "$HERE" && "$GRADLE" "${build_args[@]}" )

libdir="$HERE/build/install/chronicle-bench/lib"
if [ ! -d "$libdir" ]; then
  echo "error: build did not produce $libdir" >&2
  exit 1
fi
CP="$libdir/*"

# Chronicle needs these module opens on JDK 17+; analytics phone-home disabled
# for reproducibility. Extra flags via CHRONICLE_JVM_FLAGS.
jvm_flags=(
  -Dchronicle.analytics.disable=true
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens java.base/java.io=ALL-UNNAMED
  --add-opens java.base/java.util=ALL-UNNAMED
)
# shellcheck disable=SC2206
[ -n "${CHRONICLE_JVM_FLAGS:-}" ] && jvm_flags+=(${CHRONICLE_JVM_FLAGS})

fsync_flag="false"
[ "$durability" = "fsync" ] && fsync_flag="true"

# Authoritative resolved version: the chronicle-queue jar that installDist put on
# the classpath (its manifest carries no Implementation-Version, so read the name).
version="${CHRONICLE_VERSION:-}"
if [ -z "$version" ]; then
  cqjar="$(ls "$libdir"/chronicle-queue-*.jar 2>/dev/null | head -1)"
  [ -n "$cqjar" ] && version="$(basename "$cqjar" .jar | sed 's/^chronicle-queue-//')"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Build PREFIX=(taskset -c <core>  chrt -f <prio>) for a launched process.
build_prefix() {
  PREFIX=()
  [ -n "$1" ] && PREFIX+=(taskset -c "$1")
  [ -n "$rt_priority" ] && PREFIX+=(chrt -f "$rt_priority")
  return 0
}

idx=0
for size in $sizes; do
  for rate in $rates; do
    if [ "$rate" -eq 0 ]; then interval=0; else interval=$(( 1000000000 / rate )); fi

    if [ -n "$path" ]; then
      qdir="$path/q_${size}_${rate}_$$"
    else
      qdir="$tmp/q_${idx}"
    fi
    mkdir -p "$qdir"

    echo "  chronicle: size=${size}B rate=${rate} msg/s (interval_ns=${interval}) ..." >&2

    build_prefix "$consumer_core"
    cons_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.ChronicleBench consumer
              --queue "$qdir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --out "$tmp/consumer_${idx}.out")
    build_prefix "$producer_core"
    prod_cmd=("${PREFIX[@]}" java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.ChronicleBench producer
              --queue "$qdir" --bytes "$size" --warmup "$warmup" --measured "$messages"
              --interval-ns "$interval" --fsync "$fsync_flag"
              --out "$tmp/producer_${idx}.out")

    # Start consumer first (tailer reads from the queue head), then producer.
    "${cons_cmd[@]}" >/dev/null 2>"$tmp/consumer_${idx}.err" &
    cons_pid=$!
    "${prod_cmd[@]}" >/dev/null 2>"$tmp/producer_${idx}.err" &
    prod_pid=$!

    if ! wait "$prod_pid"; then
      echo "  ! producer failed (see below):" >&2; cat "$tmp/producer_${idx}.err" >&2; exit 1
    fi
    if ! wait "$cons_pid"; then
      echo "  ! consumer failed (see below):" >&2; cat "$tmp/consumer_${idx}.err" >&2; exit 1
    fi

    printf '%s\n' "$rate" > "$tmp/rate_${idx}.txt"
    [ -z "$path" ] && rm -rf "$qdir"
    idx=$((idx + 1))
  done
done

REVOQ_TMP="$tmp" REVOQ_COUNT="$idx" REVOQ_VERSION_OUT="$version" \
REVOQ_DURABILITY="$durability" REVOQ_WARMUP_OUT="$warmup" \
python3 - "$out" <<'PY'
import json, os, sys, datetime

tmp = os.environ["REVOQ_TMP"]
count = int(os.environ["REVOQ_COUNT"])
version_override = os.environ["REVOQ_VERSION_OUT"]
durability = os.environ["REVOQ_DURABILITY"]
warmup = int(os.environ["REVOQ_WARMUP_OUT"])
out_path = sys.argv[1]

def parse_kv(text):
    kv = {}
    for line in text.splitlines():
        if line.startswith("latency_hdr_b64="):
            kv["latency_hdr_b64"] = line[len("latency_hdr_b64="):]
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            kv[k.strip()] = v.strip()
    return kv

now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
results = []
for i in range(count):
    with open(f"{tmp}/consumer_{i}.out") as f:
        c = parse_kv(f.read())
    with open(f"{tmp}/producer_{i}.out") as f:
        p = parse_kv(f.read())
    rate = int(open(f"{tmp}/rate_{i}.txt").read().strip())
    paced = rate > 0

    lat = {
        "p50": int(c["p50_ns"]),
        "p90": int(c["p90_ns"]),
        "p99": int(c["p99_ns"]),
        "p99_9": int(c["p999_ns"]),
        "p99_99": int(c["p9999_ns"]),
        "p99_999": int(c["p99999_ns"]),
        "max": int(c["max_ns"]),
    }
    if c.get("latency_hdr_b64"):
        lat["hdr_histogram_b64"] = c["latency_hdr_b64"]

    achieved = float(p["achieved_mps"]) if "achieved_mps" in p else None
    achieved_int = int(round(achieved)) if achieved is not None else None
    saturated = (paced and achieved is not None and achieved < 0.98 * rate)

    version = version_override or c.get("chronicle_version") or "unknown"

    def as_int(v):
        try: return int(v)
        except (TypeError, ValueError): return None

    jvm = {
        "jdk_vendor": c.get("jdk_vendor", ""),
        "jdk_version": c.get("jdk_version", ""),
        "gc": c.get("gc_name", ""),
        "gc_pause_count": as_int(c.get("gc_pause_count")),
        "gc_pause_max_ns": as_int(c.get("gc_pause_max_ns")),
        "jit_steady_state_confirmed": c.get("jit_steady_state_confirmed") == "true",
    }

    results.append({
        "schema_version": 1,
        "system": "chronicle-queue",
        "system_version": version,
        "system_edition": "oss",
        "semantic_group": "journal_replayable" if durability != "none" else "volatile_ipc",
        "durability_mode": durability,
        "load_model": "open_loop" if paced else "unpaced",
        "message_bytes": int(c.get("payload_bytes", 0)),
        "offered_rate_mps": rate if paced else None,
        "topology": "1p1c",
        "warmup_count": warmup,
        "measured_count": int(c.get("samples", 0)),
        "batch_size": 1,
        "timestamp_point": "intended_send" if paced else "actual_send",
        "achieved_rate_mps": achieved_int,
        "saturated": saturated if paced else None,
        "latency_ns": lat,
        "jvm": jvm,
        "env_ref": "env.json",
        "started_utc": now,
        "notes": "unpaced points are throughput/saturation, not latency percentiles",
    })

text = json.dumps(results, indent=2)
if out_path == "/dev/stdout":
    sys.stdout.write(text + "\n")
else:
    with open(out_path, "w") as f:
        f.write(text + "\n")
    print(f"  wrote {len(results)} result point(s) to {out_path}", file=sys.stderr)
PY
