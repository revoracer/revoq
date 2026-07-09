#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Orchestrate a comparative benchmark run: capture env, run each available
# driver, collect result JSON into a dated results directory, then validate.
#
# Drivers are opt-in via flags so a partial run (e.g. revoq-only in phase 1)
# is normal. Each driver writes a schema.md-conformant JSON array (one object
# per size x offered-rate point) to the --out path passed below.
#
# Usage:
#   run_all.sh --revoq [--chronicle] [--aeron-ipc] [--aeron-archive]
#              [--sizes "32 64 256 1024"] [--rates "100000 500000 1000000 0"]

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

run_revoq=0 run_chronicle=0 run_aeron_ipc=0 run_aeron_archive=0 run_typed=0
sizes="32 64 256 1024"
rates="100000 500000 1000000 0"   # 0 = unpaced/saturation

while [ $# -gt 0 ]; do
  case "$1" in
    --revoq) run_revoq=1 ;;
    --chronicle) run_chronicle=1 ;;
    --aeron-ipc) run_aeron_ipc=1 ;;
    --aeron-archive) run_aeron_archive=1 ;;
    --typed) run_typed=1 ;;   # typed_dispatch group across every enabled system
    --sizes) sizes="$2"; shift ;;
    --rates) rates="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

host=$(hostname -s 2>/dev/null || hostname)
kern=$(uname -r | cut -d- -f1)
outdir="$ROOT/results/$(date -u +%Y-%m-%d)-${host}-linux-${kern}"
mkdir -p "$outdir"
echo "Results dir: $outdir"

echo "Capturing environment..."
bash "$HERE/env_capture.sh" > "$outdir/env.json"

failures=0

driver() {  # driver <name> <script> <json-out>
  local name="$1" script="$2" out="$3"
  if [ ! -x "$script" ] && [ ! -f "$script" ]; then
    echo "  ! $name driver not implemented yet ($script) — skipping"
    return
  fi
  echo "  running $name ..."
  if ! bash "$script" --sizes "$sizes" --rates "$rates" --out "$out"; then
    echo "  ! $name driver FAILED"
    failures=$((failures + 1))
  fi
}

[ "$run_revoq" = 1 ]         && driver "revoq"         "$ROOT/revoq/run.sh"          "$outdir/revoq.json"
[ "$run_chronicle" = 1 ]     && driver "chronicle"     "$ROOT/chronicle/run.sh"      "$outdir/chronicle.json"
[ "$run_aeron_ipc" = 1 ]     && driver "aeron-ipc"     "$ROOT/aeron/run_ipc.sh"      "$outdir/aeron-ipc.json"
[ "$run_aeron_archive" = 1 ] && driver "aeron-archive" "$ROOT/aeron/run_archive.sh"  "$outdir/aeron-archive.json"

# typed_dispatch group (decode/dispatch overhead) for each enabled system. Kept
# in separate *-typed.json files; the validator forms typed_dispatch cells from
# them (revoq spans page_cache+none so it partners both Chronicle and Aeron).
if [ "$run_typed" = 1 ]; then
  [ "$run_revoq" = 1 ]     && driver "revoq-typed"     "$ROOT/revoq/run_typed.sh"     "$outdir/revoq-typed.json"
  [ "$run_chronicle" = 1 ] && driver "chronicle-typed" "$ROOT/chronicle/run_typed.sh" "$outdir/chronicle-typed.json"
  [ "$run_aeron_ipc" = 1 ] && driver "aeron-typed"     "$ROOT/aeron/run_typed.sh"     "$outdir/aeron-typed.json"
fi

echo "Validating..."
validation_ok=1
python3 "$HERE/validate_results.py" "$outdir" || validation_ok=0

if [ "$failures" -ne 0 ] || [ "$validation_ok" -ne 1 ]; then
  echo "FAILED: $failures driver failure(s); validation $([ "$validation_ok" = 1 ] && echo ok || echo FAILED)." >&2
  echo "Do NOT publish $outdir — results are incomplete or invalid." >&2
  exit 1
fi
echo "Done. Commit $outdir (env.json + *.json) alongside any generated charts."
