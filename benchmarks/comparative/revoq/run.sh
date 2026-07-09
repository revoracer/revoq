#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# revoq comparative driver. Runs the p99 latency benchmark across an offered-rate
# sweep and emits a JSON array of ../schema.md-conformant result objects (one per
# rate point) to --out.
#
# Usage:
#   run.sh [--sizes "32 64 256 1024"] [--rates "100000 500000 1000000 0"]
#          [--out PATH] [--path DIR] [--durability page_cache|none|fsync|archive]
#          [--messages N] [--warmup N]
#
# Environment overrides:
#   REVOQ_P99_BIN     path to revoq_benchmark_stream_handler_benchmark_p99
#   REVOQ_VERSION     value for system_version in the JSON (default: git describe)
#
# Notes (kept honest, see ./README.md):
#   - Payload is swept via the benchmark's --payload-bytes (32/64/256/1024);
#     each size is a schema field, never mixed within a chart.
#   - achieved_rate_mps is measured over the benchmark's measured window; a paced
#     point falling >2% short of its offered rate is flagged saturated.
#   - durability_mode defaults to page_cache but MUST match the filesystem of
#     --path. If --path is on tmpfs/hugetlbfs, pass --durability none.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"

sizes="32"
rates="100000 500000 1000000 0"
out="/dev/stdout"
path=""
durability="page_cache"
messages="${REVOQ_MESSAGES:-5000000}"
warmup="${REVOQ_WARMUP:-200000}"

# Tuned-mode passthrough (revoq's best-case path): hugetlbfs --path, prefaulted
# journal, pinned cores, RT priority, forked cursor. Requires a hugetlbfs --path,
# four explicit CPUs, and CAP_SYS_NICE/root — i.e. run this script under sudo.
# Because hugetlbfs is volatile RAM, tuned runs are tagged durability=none
# (semantic group volatile_ipc, comparable with Aeron IPC) unless you override
# --durability. See ../workload.md and ./README.md.
tuned=0
appender_cpu=""; cursor_cpu=""; appender_warmer_cpu=""; cursor_warmer_cpu=""
rt_priority=""; fork=0
durability_set=0

while [ $# -gt 0 ]; do
  case "$1" in
    --sizes) sizes="$2"; shift ;;
    --rates) rates="$2"; shift ;;
    --out) out="$2"; shift ;;
    --path) path="$2"; shift ;;
    --durability) durability="$2"; durability_set=1; shift ;;
    --messages) messages="$2"; shift ;;
    --warmup) warmup="$2"; shift ;;
    --tuned) tuned=1 ;;
    --fork) fork=1 ;;
    --appender-cpu) appender_cpu="$2"; shift ;;
    --cursor-cpu) cursor_cpu="$2"; shift ;;
    --appender-warmer-cpu) appender_warmer_cpu="$2"; shift ;;
    --cursor-warmer-cpu) cursor_warmer_cpu="$2"; shift ;;
    --rt-priority) rt_priority="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ "$tuned" = 1 ]; then
  if [ -z "$path" ] || [ -z "$appender_cpu" ] || [ -z "$cursor_cpu" ] \
     || [ -z "$appender_warmer_cpu" ] || [ -z "$cursor_warmer_cpu" ]; then
    echo "error: --tuned requires --path (hugetlbfs) and --appender-cpu/--cursor-cpu/" >&2
    echo "       --appender-warmer-cpu/--cursor-warmer-cpu; usually run under sudo." >&2
    exit 2
  fi
  # hugetlbfs is volatile RAM: default the durability tag to none (volatile_ipc)
  # unless the caller explicitly set it.
  [ "$durability_set" = 0 ] && durability="none"
fi

for s in $sizes; do
  case "$s" in
    32|64|256|1024) ;;
    *) echo "error: unsupported payload size '$s' (allowed: 32 64 256 1024)" >&2; exit 2 ;;
  esac
done

bin="${REVOQ_P99_BIN:-}"
if [ -z "$bin" ]; then
  for c in \
    "$ROOT/build/revoq_benchmark_stream_handler_benchmark_p99" \
    "$ROOT/build/benchmarks/revoq_benchmark_stream_handler_benchmark_p99"; do
    [ -x "$c" ] && bin="$c" && break
  done
fi
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
  echo "error: p99 benchmark binary not found. Build it with:" >&2
  echo "         ./build.sh benchmarks" >&2
  echo "       or set REVOQ_P99_BIN=/path/to/binary" >&2
  exit 1
fi

version="${REVOQ_VERSION:-$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

idx=0
for size in $sizes; do
  for rate in $rates; do
    if [ "$rate" -eq 0 ]; then
      interval=0
    else
      interval=$(( 1000000000 / rate ))
    fi
    args=(--messages "$messages" --warmup "$warmup" --interval-ns "$interval"
          --payload-bytes "$size")
    point_path=""
    if [ "$tuned" = 1 ]; then
      # per-point subdir so sweep points don't collide on the hugetlbfs mount
      point_path="$path/p_${size}_${rate}_$$"
      mkdir -p "$point_path"
      args+=(--path "$point_path" --tuned
             --appender-cpu "$appender_cpu" --cursor-cpu "$cursor_cpu"
             --appender-warmer-cpu "$appender_warmer_cpu"
             --cursor-warmer-cpu "$cursor_warmer_cpu")
      [ -n "$rt_priority" ] && args+=(--rt-priority "$rt_priority")
      [ "$fork" = 1 ] && args+=(--fork)
    elif [ -n "$path" ]; then
      args+=(--path "$path")
    fi
    label="revoq"; [ "$tuned" = 1 ] && label="revoq (tuned)"
    echo "  ${label}: size=${size}B rate=${rate} msg/s (interval_ns=${interval}) ..." >&2
    if "$bin" "${args[@]}" > "$tmp/run_${idx}.txt" 2>"$tmp/run_${idx}.err"; then
      :
    else
      status=$?
      echo "  ! revoq benchmark failed: size=${size}B rate=${rate} msg/s (exit ${status})" >&2
      if [ -s "$tmp/run_${idx}.err" ]; then
        echo "  ! stderr:" >&2
        sed -n '1,120p' "$tmp/run_${idx}.err" >&2
      fi
      if [ -s "$tmp/run_${idx}.txt" ]; then
        echo "  ! stdout before failure:" >&2
        sed -n '1,80p' "$tmp/run_${idx}.txt" >&2
      fi
      [ -n "$point_path" ] && rm -rf "$point_path"
      exit "$status"
    fi
    printf '%s\n' "$rate" > "$tmp/rate_${idx}.txt"
    [ -n "$point_path" ] && rm -rf "$point_path"
    idx=$((idx + 1))
  done
done

REVOQ_TMP="$tmp" REVOQ_COUNT="$idx" REVOQ_VERSION_OUT="$version" \
REVOQ_DURABILITY="$durability" REVOQ_WARMUP_OUT="$warmup" \
python3 - "$out" <<'PY'
import json, os, sys, datetime

tmp = os.environ["REVOQ_TMP"]
count = int(os.environ["REVOQ_COUNT"])
version = os.environ["REVOQ_VERSION_OUT"]
durability = os.environ["REVOQ_DURABILITY"]
warmup = int(os.environ["REVOQ_WARMUP_OUT"])
out_path = sys.argv[1]

def parse_kv(text):
    kv = {}
    for line in text.splitlines():
        if line.startswith("latency_hdr_b64="):
            kv["latency_hdr_b64"] = line[len("latency_hdr_b64="):]
            continue
        for tok in line.split():
            if "=" in tok:
                k, _, v = tok.partition("=")
                kv[k] = v
    return kv

now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
results = []
for i in range(count):
    with open(f"{tmp}/run_{i}.txt") as f:
        kv = parse_kv(f.read())
    rate = int(open(f"{tmp}/rate_{i}.txt").read().strip())
    paced = rate > 0
    lat = {
        "p50": int(kv["p50_ns"]),
        "p90": int(kv["p90_ns"]),
        "p99": int(kv["p99_ns"]),
        "p99_9": int(kv["p999_ns"]),
        "max": int(kv["max_ns"]),
    }
    if "p9999_ns" in kv:
        lat["p99_99"] = int(kv["p9999_ns"])
    if "p99999_ns" in kv:
        lat["p99_999"] = int(kv["p99999_ns"])
    if kv.get("latency_hdr_b64"):
        lat["hdr_histogram_b64"] = kv["latency_hdr_b64"]
    achieved = float(kv["achieved_mps"]) if "achieved_mps" in kv else None
    achieved_int = int(round(achieved)) if achieved is not None else None
    # Saturated = the producer could not keep the offered schedule (achieved
    # fell more than 2% short). Only meaningful for paced runs.
    saturated = (paced and achieved is not None and achieved < 0.98 * rate)
    results.append({
        "schema_version": 1,
        "system": "revoq",
        "system_version": version,
        "system_edition": "oss",
        "semantic_group": "journal_replayable" if durability != "none" else "volatile_ipc",
        "durability_mode": durability,
        "load_model": "open_loop" if paced else "unpaced",
        "message_bytes": int(kv.get("payload_bytes", 32)),
        "offered_rate_mps": rate if paced else None,
        "topology": "1p1c",
        "warmup_count": warmup,
        "measured_count": int(kv.get("samples", 0)),
        "batch_size": 1,
        "timestamp_point": "intended_send" if paced else "actual_send",
        "achieved_rate_mps": achieved_int,
        "saturated": saturated if paced else None,
        "latency_ns": lat,
        "jvm": None,
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
