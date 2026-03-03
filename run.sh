#!/bin/bash

# Usage:
#   ./run.sh              → single run
#   ./run.sh --runs 5     → multi run (5 times)
#   ./run.sh --runs 1     → multi run (1 time, with aggregated output)

rm -rf experiment/results/*

if [ "$1" == "--runs" ]; then
  python experiment/run_multi.py experiment/runs/aegean.json --runs "$2"
else
  python experiment/main.py experiment/runs/aegean.json
fi