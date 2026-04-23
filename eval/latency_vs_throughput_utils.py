from dataclasses import dataclass
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise SystemExit(
        "matplotlib is required. Install with: pip install matplotlib"
    ) from exc

from plot_utils import parse_metrics_log


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_ROOT = REPO_ROOT / "results"
DEFAULT_FILENAME = "latency_vs_throughput.png"


@dataclass(frozen=True)
class MetricPoint:
    throughput: float
    median_ms: float
    p90_ms: float


def humanize_workload_name(workload_name: str) -> str:
    return workload_name.replace("_", " ").title()


def series_label(series_dir: Path, workload_name: str) -> str:
    return "EO" if series_dir.name == f"{workload_name}_eo" else "Baseline"


def existing_series_dirs(results_root: Path, workload_name: str) -> list[Path]:
    candidates = [
        results_root / workload_name,
        results_root / f"{workload_name}_eo",
    ]
    return [path for path in candidates if path.is_dir()]


def load_series_points(series_dir: Path) -> list[MetricPoint]:
    points: list[MetricPoint] = []

    for log_path in sorted(series_dir.glob("qps_*/node0.log")):
        try:
            throughput, median_ms, p90_ms = parse_metrics_log(log_path)
        except ValueError:
            continue
        points.append(
            MetricPoint(
                throughput=throughput,
                median_ms=median_ms,
                p90_ms=p90_ms,
            )
        )

    if not points:
        raise ValueError(f"No complete client logs found under {series_dir}")

    points.sort(key=lambda point: point.throughput)
    return points


def collect_series(results_root: Path, workload_name: str) -> dict[str, list[MetricPoint]]:
    series: dict[str, list[MetricPoint]] = {}

    for series_dir in existing_series_dirs(results_root, workload_name):
        try:
            points = load_series_points(series_dir)
        except ValueError:
            continue
        series[series_label(series_dir, workload_name)] = points

    if not series:
        raise ValueError(f"No complete result sets found for {workload_name}")

    return series


def plot_latency_vs_throughput(
    series: dict[str, list[MetricPoint]],
    output_path: Path,
    title: str,
) -> None:
    plt.figure(figsize=(9, 6))

    for label, points in series.items():
        throughput = [point.throughput for point in points]
        median_ms = [point.median_ms for point in points]
        p90_ms = [point.p90_ms for point in points]

        plt.plot(throughput, median_ms, marker="o", linewidth=2, label=f"{label} Median")
        plt.plot(
            throughput,
            p90_ms,
            marker="s",
            linewidth=2,
            linestyle="--",
            label=f"{label} P90",
        )

    plt.xlabel("Realized Throughput (req/s)")
    plt.ylabel("Latency (ms)")
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def output_path_for(results_root: Path, workload_name: str, filename: str) -> Path:
    return results_root / workload_name / filename


def generate_workload_plot(
    workload_name: str,
    *,
    results_root: Path = DEFAULT_RESULTS_ROOT,
    filename: str = DEFAULT_FILENAME,
) -> Path:
    series = collect_series(results_root, workload_name)
    output_path = output_path_for(results_root, workload_name, filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_latency_vs_throughput(
        series,
        output_path,
        f"{humanize_workload_name(workload_name)} Latency vs Realized Throughput",
    )
    return output_path
