#!/bin/bash

# Usage:
#   ./run.sh                                      → single run
#   ./run.sh --runs 5                             → 5 runs
#   ./run.sh --runs 5 --delay 3                   → 5 runs, 3s between each
#   ./run.sh --runs 5 --out my_results.json       → custom output file
#   ./run.sh --runs 5 --delay 3 --out results.json

RUNS=""
DELAY=""
OUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)   RUNS="$2";  shift 2 ;;
    --delay)  DELAY="$2"; shift 2 ;;
    --out)    OUT="$2";   shift 2 ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: ./run.sh [--runs N] [--delay SECS] [--out FILE]"
      exit 1
      ;;
  esac
done

rm -rf experiment/results/*

if [ -n "$RUNS" ]; then
  CMD="python experiment/run_multi.py experiment/runs/aegean.json --runs $RUNS"
  [ -n "$DELAY" ] && CMD="$CMD --delay $DELAY"
  [ -n "$OUT"   ] && CMD="$CMD --out $OUT"
  eval "$CMD"
else
  python experiment/main.py experiment/runs/aegean.json
fi