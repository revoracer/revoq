#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
"""Validate comparative benchmark result JSON and enforce fair-comparison cells.

This is the mechanical guard that keeps the repo from publishing a misleading
chart. It checks each result against the schema's required fields, then groups
results into comparison *cells* and reports which results may be charted together.

A latency chart across two results is permitted ONLY if they share:
    (semantic_group, durability_mode, load_model, message_bytes, offered_rate_mps)
and each is a valid latency run (timestamp_point == "intended_send",
batch_size == 1). Published charts additionally require system_edition == "oss".

Usage:
    validate_results.py results/2026-07-07-host-linux/     # validate a run dir
    validate_results.py --require-oss results/<dir>/        # stricter (publish gate)

Exit code 0 = all good; non-zero = at least one problem.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REQUIRED = [
    "schema_version", "system", "system_version", "system_edition",
    "semantic_group", "durability_mode", "load_model", "message_bytes",
    "topology", "warmup_count", "measured_count", "timestamp_point", "env_ref",
]
CELL_KEYS = ("semantic_group", "durability_mode", "load_model",
             "message_bytes", "offered_rate_mps")
SEMANTIC_GROUPS = {"journal_replayable", "volatile_ipc", "typed_dispatch"}
DURABILITY_MODES = {"none", "page_cache", "fsync", "archive"}


def load_results(run_dir: Path) -> list[tuple[str, dict]]:
    """Return (label, result) pairs. A result file may hold a single object or a
    JSON array of objects (a driver's rate sweep); arrays are flattened."""
    out = []
    for p in sorted(run_dir.glob("*.json")):
        if p.name == "env.json":
            continue
        try:
            doc = json.loads(p.read_text())
        except json.JSONDecodeError as e:
            print(f"ERROR {p.name}: invalid JSON: {e}", file=sys.stderr)
            out.append((p.name, None))
            continue
        items = doc if isinstance(doc, list) else [doc]
        for i, item in enumerate(items):
            label = p.name if not isinstance(doc, list) else f"{p.name}[{i}]"
            out.append((label, item if isinstance(item, dict) else None))
    return out


def validate_one(label: str, r: dict | None, require_oss: bool) -> list[str]:
    errs: list[str] = []
    if r is None:
        return [f"{label}: could not parse"]
    for k in REQUIRED:
        if k not in r:
            errs.append(f"{label}: missing required field '{k}'")
    if "latency_ns" not in r and "throughput" not in r:
        errs.append(f"{label}: must have at least one of latency_ns/throughput")
    if r.get("semantic_group") not in SEMANTIC_GROUPS:
        errs.append(f"{label}: bad semantic_group {r.get('semantic_group')!r}")
    if r.get("durability_mode") not in DURABILITY_MODES:
        errs.append(f"{label}: bad durability_mode {r.get('durability_mode')!r}")
    # A latency run only means an open-loop (paced) point. Unpaced runs carry a
    # latency_ns block too, but they are throughput/saturation summaries, not
    # coordinated-omission-free percentiles, so the CO check does not apply.
    if "latency_ns" in r and r.get("load_model") != "unpaced":
        if r.get("timestamp_point") != "intended_send":
            errs.append(f"{label}: latency run must use timestamp_point="
                        f"'intended_send' (coordinated omission), got "
                        f"{r.get('timestamp_point')!r}")
        if r.get("batch_size", 1) != 1:
            errs.append(f"{label}: latency run must be unbatched (batch_size=1)")
        if r.get("offered_rate_mps") is None:
            errs.append(f"{label}: paced latency run needs offered_rate_mps")
    if require_oss and r.get("system_edition") != "oss":
        errs.append(f"{label}: system_edition must be 'oss' to publish, got "
                    f"{r.get('system_edition')!r}")
    return errs


def cell_of(r: dict) -> tuple:
    return tuple(r.get(k) for k in CELL_KEYS)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=Path)
    ap.add_argument("--require-oss", action="store_true",
                    help="publish gate: reject non-OSS editions")
    args = ap.parse_args()

    if not args.run_dir.is_dir():
        print(f"ERROR: {args.run_dir} is not a directory", file=sys.stderr)
        return 2
    if not (args.run_dir / "env.json").exists():
        print(f"WARNING: {args.run_dir}/env.json missing (provenance incomplete)",
              file=sys.stderr)

    results = load_results(args.run_dir)
    if not results:
        print(f"ERROR: no result JSON found in {args.run_dir}", file=sys.stderr)
        return 2

    all_errs: list[str] = []
    for label, r in results:
        all_errs += validate_one(label, r, args.require_oss)

    # Report comparison cells: which systems can be charted together.
    cells: dict[tuple, set[str]] = {}
    for label, r in results:
        if r is None:
            continue
        cells.setdefault(cell_of(r), set()).add(r.get("system", label))

    print(f"\nComparison cells in {args.run_dir}:")
    for cell, systems in sorted(cells.items(), key=lambda x: str(x[0])):
        keyed = dict(zip(CELL_KEYS, cell))
        joined = ", ".join(sorted(systems))
        chartable = "CHARTABLE TOGETHER" if len(systems) > 1 else "single system"
        print(f"  [{chartable}] {keyed}")
        print(f"      systems: {joined}")

    if all_errs:
        print("\nProblems:", file=sys.stderr)
        for e in all_errs:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("\nOK: all results valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
