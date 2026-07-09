#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Aeron typed-dispatch driver (semantic group typed_dispatch, durability none).
# Single process: encode a mixed 3-type stream into a fixed-layout buffer, then
# time decoding+dispatching every record through a type switch into a typed
# handler (the FragmentHandler/SBE decode shape). Emits one ../schema.md
# throughput result to --out. See ../workload.md section 11.
#
# Usage:
#   run_typed.sh [--out PATH] [--messages N] [--warmup N]
#   (--sizes/--rates/--path are accepted and ignored: the typed schema is fixed.)
#
# Environment overrides: AERON_VERSION, AERON_JVM_FLAGS, GRADLE.
# Requires: JDK 17+, Gradle 8+, network on first build (Maven Central).

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

out="/dev/stdout"
messages="${AERON_MESSAGES:-5000000}"
warmup="${AERON_WARMUP:-200000}"

while [ $# -gt 0 ]; do
  case "$1" in
    --out) out="$2"; shift ;;
    --messages) messages="$2"; shift ;;
    --warmup) warmup="$2"; shift ;;
    --sizes) shift ;;   # accepted for a uniform CLI; ignored
    --rates) shift ;;
    --path) shift ;;
    --durability) shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

GRADLE="${GRADLE:-gradle}"
if ! command -v "$GRADLE" >/dev/null 2>&1; then
  echo "error: '$GRADLE' not found. Install Gradle 8+ (and a JDK 17+), or set GRADLE=." >&2
  exit 1
fi

echo "  aeron-typed: building (gradle installDist) ..." >&2
build_args=(installDist -q)
[ -n "${AERON_VERSION:-}" ] && build_args+=("-PaeronVersion=${AERON_VERSION}")
( cd "$HERE" && "$GRADLE" "${build_args[@]}" )

libdir="$HERE/build/install/aeron-bench/lib"
[ -d "$libdir" ] || { echo "error: build did not produce $libdir" >&2; exit 1; }
CP="$libdir/*"

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

echo "  aeron-typed: messages=${messages} warmup=${warmup} ..." >&2
java "${jvm_flags[@]}" -cp "$CP" racer.revoq.bench.AeronTypedBench \
  --warmup "$warmup" --measured "$messages" --out "$tmp/typed.out" \
  >/dev/null 2>"$tmp/typed.err" || { echo "  ! aeron-typed failed:" >&2; cat "$tmp/typed.err" >&2; exit 1; }

REVOQ_TMP="$tmp" REVOQ_WARMUP_OUT="$warmup" \
python3 - "$out" <<'PY'
import json, os, sys, datetime

tmp = os.environ["REVOQ_TMP"]
warmup = int(os.environ["REVOQ_WARMUP_OUT"])
out_path = sys.argv[1]
NOMINAL_BYTES = 24  # shared verbatim across typed_dispatch drivers (see workload.md §11)

def parse_kv(text):
    kv = {}
    for line in text.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            kv[k.strip()] = v.strip()
    return kv

def as_int(v):
    try: return int(v)
    except (TypeError, ValueError): return None

c = parse_kv(open(f"{tmp}/typed.out").read())
mps = int(round(float(c["messages_per_s"])))
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

result = {
    "schema_version": 1,
    "system": "aeron-ipc",
    "system_version": c.get("aeron_version", "unknown"),
    "system_edition": "oss",
    "semantic_group": "typed_dispatch",
    "durability_mode": "none",
    "load_model": "unpaced",
    "message_bytes": NOMINAL_BYTES,
    "offered_rate_mps": None,
    "topology": "1p1c",
    "warmup_count": warmup,
    "measured_count": int(c.get("measured", 0)),
    "batch_size": 1,
    "timestamp_point": "actual_send",
    "achieved_rate_mps": None,
    "saturated": None,
    "throughput": {
        "messages_per_s": mps,
        "bytes_per_s": mps * NOMINAL_BYTES,
        "ns_per_message": float(c.get("ns_per_message", 0.0)),
    },
    "jvm": {
        "jdk_vendor": c.get("jdk_vendor", ""),
        "jdk_version": c.get("jdk_version", ""),
        "gc": c.get("gc_name", ""),
        "gc_pause_count": as_int(c.get("gc_pause_count")),
        "gc_pause_max_ns": as_int(c.get("gc_pause_max_ns")),
        "jit_steady_state_confirmed": c.get("jit_steady_state_confirmed") == "true",
    },
    "env_ref": "env.json",
    "started_utc": now,
    "notes": "typed decode+dispatch overhead via a fixed-layout type switch into "
             "a typed handler; mixed 3-type schema; ns_per_message is the headline metric",
}

text = json.dumps([result], indent=2)
if out_path == "/dev/stdout":
    sys.stdout.write(text + "\n")
else:
    with open(out_path, "w") as f:
        f.write(text + "\n")
    print(f"  wrote 1 result point(s) to {out_path}", file=sys.stderr)
PY
