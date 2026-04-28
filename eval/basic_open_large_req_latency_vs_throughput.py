#!/usr/bin/env python3

import argparse
from pathlib import Path

from latency_vs_throughput_utils import (
    DEFAULT_FILENAME,
    DEFAULT_RESULTS_ROOT,
    generate_workload_plot,
)


WORKLOAD_NAME = "basic_open_large_req"
DEFAULT_MIN_QPS = 250
DEFAULT_MAX_QPS = 400


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot basic_open_large_req latency against realized throughput."
    )
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--filename", default=DEFAULT_FILENAME)
    parser.add_argument("--min-qps", type=int, default=DEFAULT_MIN_QPS)
    parser.add_argument("--max-qps", type=int, default=DEFAULT_MAX_QPS)
    args = parser.parse_args()

    output_path = generate_workload_plot(
        WORKLOAD_NAME,
        results_root=args.results_root,
        filename=args.filename,
        min_offered_qps=args.min_qps,
        max_offered_qps=args.max_qps,
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
