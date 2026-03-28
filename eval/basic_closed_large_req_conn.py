#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

from plot_utils import (
    collect_data,
    collect_data_aggregated,
    plot_latency,
    plot_latency_aggregated,
    plot_throughput,
    plot_throughput_aggregated,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "basic_closed_large_req_conn"
CONN_DIR_RE = re.compile(r"conn_(\d+)$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--aggregated", action="store_true",
                        help="Use aggregated multi-run data (expects conn_*/*/node0.log)")
    args = parser.parse_args()

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    throughput_path = results_dir / "throughput_vs_connections.png"
    latency_path = results_dir / "latency_vs_connections.png"

    if args.aggregated:
        rows = collect_data_aggregated(results_dir, "conn_*/*/node0.log", CONN_DIR_RE)
        plot_throughput_aggregated(rows, throughput_path, "Connections", "Throughput vs Connections")
        plot_latency_aggregated(rows, latency_path, "Connections", "Latency vs Connections")
    else:
        rows = collect_data(results_dir, "conn_*/node0.log", CONN_DIR_RE)
        plot_throughput(rows, throughput_path, "Connections", "Throughput vs Connections")
        plot_latency(rows, latency_path, "Connections", "Latency vs Connections")

    print(f"Wrote {throughput_path}")
    print(f"Wrote {latency_path}")


if __name__ == "__main__":
    main()
