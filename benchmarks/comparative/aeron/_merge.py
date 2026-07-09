#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Merge the per-point consumer/producer key=value outputs of an Aeron driver run
# into a JSON array of ../schema.md-conformant result objects. Shared by
# run_ipc.sh and run_archive.sh (only the system name and durability differ).
#
#   _merge.py <tmp_dir> <count> <system> <durability> <warmup> <out_path>
#
# system:     "aeron-ipc" | "aeron-archive"
# durability: "none" (volatile_ipc) | "archive" (journal_replayable)
import json
import os
import sys
import datetime

tmp = sys.argv[1]
count = int(sys.argv[2])
system = sys.argv[3]
durability = sys.argv[4]
warmup = int(sys.argv[5])
out_path = sys.argv[6]


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


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
# Durability decides the semantic group: a recorded/replayable log is
# journal_replayable; a transient ring buffer is volatile_ipc.
semantic_group = "volatile_ipc" if durability == "none" else "journal_replayable"

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
        "system": system,
        "system_version": c.get("aeron_version", "unknown"),
        "system_edition": "oss",
        "semantic_group": semantic_group,
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
