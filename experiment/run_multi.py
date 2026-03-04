import argparse
import glob
import json
import math
import os
import re
import subprocess
import sys
import time


def parse_oha_log(log_path):
    """
    Parse oha summary output from a node log file.
    Returns a dict of stats, or None if no oha output found.
    """
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    stats = {}

    patterns = {
        "success_rate":   r"Success rate:\s+([\d.]+)%",
        "total_sec":      r"Total:\s+([\d.]+) sec",
        "slowest_sec":    r"Slowest:\s+([\d.]+) sec",
        "fastest_sec":    r"Fastest:\s+([\d.]+) sec",
        "average_sec":    r"Average:\s+([\d.]+) sec",
        "requests_sec":   r"Requests/sec:\s+([\d.]+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            stats[key] = float(m.group(1))

    percentile_pattern = r"([\d.]+)%\s+in\s+([\d.]+) sec"
    for m in re.finditer(percentile_pattern, text):
        pct = float(m.group(1))
        val = float(m.group(2))
        label = f"p{pct}".replace(".0", "").replace(".", "_")
        stats[label] = val

    histogram = []
    histogram_pattern = r"([\d.]+) sec \[(\d+)\]"
    for m in re.finditer(histogram_pattern, text):
        histogram.append([float(m.group(1)), int(m.group(2))])
    if histogram:
        stats["histogram"] = histogram

    status_pattern = r"\[(\d+)\]\s+(\d+) responses"
    status_codes = {}
    for m in re.finditer(status_pattern, text):
        status_codes[int(m.group(1))] = int(m.group(2))
    if status_codes:
        stats["status_codes"] = status_codes

    return stats if stats else None


def find_oha_log(run_dir):
    logs_dir = os.path.join(run_dir, "logs")
    if not os.path.isdir(logs_dir):
        return None
    for log_file in sorted(glob.glob(os.path.join(logs_dir, "*.log"))):
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        if "Response time distribution" in content:
            return log_file
    return None


# Stats
def mean(values):
    return sum(values) / len(values)

def stdev(values):
    m = mean(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return math.sqrt(variance)

def median(values):
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

def aggregate_runs(all_stats):
    keys = set()
    for s in all_stats:
        for k, v in s.items():
            if isinstance(v, (int, float)):
                keys.add(k)

    aggregated = {}
    for key in sorted(keys):
        values = [s[key] for s in all_stats if key in s and isinstance(s[key], (int, float))]
        if not values:
            continue
        aggregated[key] = {
            "mean":   round(mean(values), 4),
            "median": round(median(values), 4),
            "min":    round(min(values), 4),
            "max":    round(max(values), 4),
            "stdev":  round(stdev(values), 4) if len(values) > 1 else 0.0,
            "values": [round(v, 4) for v in values],
        }

    return aggregated


def print_summary(aggregated, num_runs):
    print()
    print("=" * 60)
    print(f"  AGGREGATED RESULTS  ({num_runs} runs)")
    print("=" * 60)

    priority_keys = [
        ("average_sec",    "Average latency"),
        ("p50",            "p50 latency"),
        ("p75",            "p75 latency"),
        ("p90",            "p90 latency"),
        ("p95",            "p95 latency"),
        ("p99",            "p99 latency"),
        ("p99_9",          "p99.9 latency"),
        ("fastest_sec",    "Fastest"),
        ("slowest_sec",    "Slowest"),
        ("requests_sec",   "Requests/sec"),
        ("success_rate",   "Success rate (%)"),
        ("total_sec",      "Total duration (sec)"),
    ]

    def fmt_row(label, data):
        print(f"  {label:<25}  mean={data['mean']:.4f}  "
              f"median={data['median']:.4f}  "
              f"min={data['min']:.4f}  "
              f"max={data['max']:.4f}  "
              f"stdev={data['stdev']:.4f}")

    print()
    print("  Latency (seconds):")
    print("  " + "-" * 56)
    for key, label in priority_keys:
        if key in aggregated:
            fmt_row(label, aggregated[key])

    dist_keys = [k for k in aggregated if k.startswith("p") and k not in dict(priority_keys)]
    if dist_keys:
        print()
        print("  Other percentiles:")
        print("  " + "-" * 56)
        for key in sorted(dist_keys):
            fmt_row(key, aggregated[key])

    print()
    print("  Per-run values (p50 / p90 / p99):")
    print("  " + "-" * 56)
    for key in ["p50", "p90", "p99"]:
        if key in aggregated:
            vals = "  ".join(f"{v:.4f}" for v in aggregated[key]["values"])
            print(f"  {key:<10} {vals}")
    print()
    print("=" * 60)
    print()


def print_histogram(all_stats):
    all_histograms = [s["histogram"] for s in all_stats if "histogram" in s]
    if not all_histograms:
        return

    buckets = [b[0] for b in all_histograms[0]]
    avg_counts = []
    for i, bucket in enumerate(buckets):
        counts = [h[i][1] for h in all_histograms if i < len(h)]
        avg_counts.append(mean(counts))

    total = sum(avg_counts) or 1
    max_count = max(avg_counts) or 1
    bar_width = 32

    print("=" * 60)
    print(f"  RESPONSE TIME DISTRIBUTION  (avg across {len(all_histograms)} runs)")
    print("=" * 60)
    print()
    for bucket, count in zip(buckets, avg_counts):
        bar_len = int((count / max_count) * bar_width)
        bar = "■" * bar_len
        pct = (count / total) * 100
        print(f"  {bucket:.3f}s  [{int(count):>5}]  {bar:<{bar_width}}  {pct:5.1f}%")
    print()


def run_experiment(config_path, run_number, total_runs):
    print(f"\n{'='*60}")
    print(f"  RUN {run_number}/{total_runs}")
    print(f"{'='*60}")

    result = subprocess.run(
        ["python", "experiment/main.py", config_path],
        check=False,
    )
    if result.returncode != 0:
        print(f"  WARNING: experiment exited with code {result.returncode}")

    # Find the most recently created results dir
    run_dirs = sorted(glob.glob("experiment/results/*/"))
    if not run_dirs:
        print("  ERROR: no results directory found after run")
        return None

    return run_dirs[-1].rstrip("/")


def main():
    parser = argparse.ArgumentParser(description="Run Aegean experiment multiple times and aggregate results")
    parser.add_argument("config_path", help="Path to run config JSON")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs (default: 1)")
    parser.add_argument("--out", default=None, help="Output JSON file path (default: experiment/results/aggregated_<timestamp>.json)")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds to wait between runs (default: 2)")
    args = parser.parse_args()

    all_stats = []
    run_dirs = []

    for i in range(1, args.runs + 1):
        run_dir = run_experiment(args.config_path, i, args.runs)
        if run_dir is None:
            print(f"  Skipping run {i} — no results dir found")
            continue

        log_file = find_oha_log(run_dir)
        if log_file is None:
            print(f"  WARNING: no oha output found in {run_dir}/logs/")
            continue

        stats = parse_oha_log(log_file)
        if stats is None:
            print(f"  WARNING: could not parse oha stats from {log_file}")
            continue

        def fmt(val, decimals=4):
            return f"{val:.{decimals}f}" if isinstance(val, (int, float)) else "?"

        print(f"\n  Run {i} results (from {os.path.basename(log_file)}):")
        print(f"    p50={fmt(stats.get('p50'))}s  "
              f"p90={fmt(stats.get('p90'))}s  "
              f"p99={fmt(stats.get('p99'))}s  "
              f"avg={fmt(stats.get('average_sec'))}s  "
              f"rps={fmt(stats.get('requests_sec'), 1)}")

        all_stats.append(stats)
        run_dirs.append(run_dir)

        if i < args.runs:
            print(f"\n  Waiting {args.delay}s before next run...")
            time.sleep(args.delay)

    if not all_stats:
        print("ERROR: no successful runs to aggregate")
        sys.exit(1)

    aggregated = aggregate_runs(all_stats)
    print_summary(aggregated, len(all_stats))
    print_histogram(all_stats)

    # Write output JSON
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = args.out or f"experiment/results/aggregated_{timestamp}.json"
    output = {
        "num_runs": len(all_stats),
        "run_dirs": run_dirs,
        "config": args.config_path,
        "timestamp": timestamp,
        "per_run": all_stats,
        "aggregated": aggregated,
    }

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)


    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Results saved to: {out_path}\n")


if __name__ == "__main__":
    main()