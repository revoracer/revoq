#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# revoq typed-dispatch comparative driver (semantic group typed_dispatch). Runs
# the existing typed_dispatch_overhead micro benchmark, which writes a mixed
# 3-type stream then times decoding+dispatching all of it through revoq's
# TypedDispatcher, and emits ../schema.md-conformant throughput results to --out.
#
# See ../workload.md section 11. This is a decode/dispatch OVERHEAD micro
# (ns/message), not a latency curve: results carry a `throughput` block,
# load_model=unpaced, and no offered_rate.
#
# It emits TWO array elements — one per durability medium — so revoq partners
# both Chronicle (page_cache) and Aeron (none):
#   page_cache : journal on the default local-fs temp dir
#   none       : journal on tmpfs ($TMPDIR=/dev/shm)
# Decode overhead is largely medium-independent; the durability tag records where
# the journal was written and keeps charts within one durability, as the rules
# require.
#
# Usage:
#   run_typed.sh [--out PATH] [--messages N] [--durability page_cache|none|both]
#
# Environment overrides:
#   REVOQ_TYPED_BIN   path to revoq_benchmark_typed_dispatch_overhead
#   REVOQ_VERSION     value for system_version (default: git describe)
#   REVOQ_SHM_DIR     tmpfs dir for the `none` run (default: /dev/shm)

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"

out="/dev/stdout"
messages="${REVOQ_MESSAGES:-5000000}"
which="both"
shm_dir="${REVOQ_SHM_DIR:-/dev/shm}"

while [ $# -gt 0 ]; do
  case "$1" in
    --out) out="$2"; shift ;;
    --messages) messages="$2"; shift ;;
    --durability) which="$2"; shift ;;
    # accepted for a uniform CLI with the transport driver; ignored here (the
    # typed schema is fixed, not swept):
    --sizes) shift ;;
    --rates) shift ;;
    --path) shift ;;
    --warmup) shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

bin="${REVOQ_TYPED_BIN:-}"
if [ -z "$bin" ]; then
  for c in \
    "$ROOT/build/revoq_benchmark_typed_dispatch_overhead" \
    "$ROOT/build/benchmarks/revoq_benchmark_typed_dispatch_overhead"; do
    [ -x "$c" ] && bin="$c" && break
  done
fi
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
  echo "error: typed_dispatch_overhead binary not found. Build it with:" >&2
  echo "         ./build.sh benchmarks   (or set REVOQ_TYPED_BIN=/path/to/binary)" >&2
  exit 1
fi

version="${REVOQ_VERSION:-$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Run one durability variant: $1=durability, $2=TMPDIR to use (empty = default).
run_one() {
  local durability="$1" tmpdir="$2" i="$3"
  echo "  revoq-typed: durability=${durability} messages=${messages} ..." >&2
  if [ -n "$tmpdir" ]; then
    TMPDIR="$tmpdir" "$bin" --messages "$messages" > "$tmp/typed_${i}.txt" 2>/dev/null
  else
    "$bin" --messages "$messages" > "$tmp/typed_${i}.txt" 2>/dev/null
  fi
  printf '%s\n' "$durability" > "$tmp/dur_${i}.txt"
}

idx=0
if [ "$which" = "page_cache" ] || [ "$which" = "both" ]; then
  run_one "page_cache" "" "$idx"; idx=$((idx + 1))
fi
if [ "$which" = "none" ] || [ "$which" = "both" ]; then
  if [ -d "$shm_dir" ] && [ -w "$shm_dir" ]; then
    run_one "none" "$shm_dir" "$idx"; idx=$((idx + 1))
  else
    echo "  ! revoq-typed: $shm_dir not writable; skipping the 'none' variant" >&2
  fi
fi

REVOQ_TMP="$tmp" REVOQ_COUNT="$idx" REVOQ_VERSION_OUT="$version" \
python3 - "$out" <<'PY'
import json, os, sys, datetime

tmp = os.environ["REVOQ_TMP"]
count = int(os.environ["REVOQ_COUNT"])
version = os.environ["REVOQ_VERSION_OUT"]
out_path = sys.argv[1]

# Nominal max record size of the typed schema (Trade = i64+u32+f64, 24B with
# alignment). Shared verbatim by every typed_dispatch driver for cell-matching;
# the headline metric is ns/message, not bytes.
NOMINAL_BYTES = 24

def parse_typed_row(text):
    for line in text.splitlines():
        f = line.split()
        if f and f[0] == "typed_dispatcher":
            # name messages seconds Mmsg/s ns/msg guardkind guard
            return {"messages": int(f[1]), "seconds": float(f[2])}
    raise RuntimeError("no typed_dispatcher row in benchmark output")

now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
results = []
for i in range(count):
    row = parse_typed_row(open(f"{tmp}/typed_{i}.txt").read())
    durability = open(f"{tmp}/dur_{i}.txt").read().strip()
    n = row["messages"]
    secs = row["seconds"]
    mps = int(round(n / secs)) if secs > 0 else 0
    results.append({
        "schema_version": 1,
        "system": "revoq",
        "system_version": version,
        "system_edition": "oss",
        "semantic_group": "typed_dispatch",
        "durability_mode": durability,
        "load_model": "unpaced",
        "message_bytes": NOMINAL_BYTES,
        "offered_rate_mps": None,
        "topology": "1p1c",
        "warmup_count": 0,        # native: no JIT warmup needed
        "measured_count": n,
        "batch_size": 1,
        "timestamp_point": "actual_send",
        "achieved_rate_mps": None,
        "saturated": None,
        "throughput": {
            "messages_per_s": mps,
            "bytes_per_s": mps * NOMINAL_BYTES,
            "ns_per_message": (secs * 1e9 / n) if n else None,
        },
        "jvm": None,
        "env_ref": "env.json",
        "started_utc": now,
        "notes": "typed decode+dispatch overhead via TypedDispatcher; mixed "
                 "3-type schema; ns_per_message is the headline metric",
    })

text = json.dumps(results, indent=2)
if out_path == "/dev/stdout":
    sys.stdout.write(text + "\n")
else:
    with open(out_path, "w") as f:
        f.write(text + "\n")
    print(f"  wrote {len(results)} result point(s) to {out_path}", file=sys.stderr)
PY
