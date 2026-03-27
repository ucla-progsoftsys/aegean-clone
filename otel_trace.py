#!/usr/bin/env python3
# Usage:
#   python3 otel_trace.py results/aegean/node1.otel.json --request-id 300
#   python3 otel_trace.py results/aegean/node1.otel.json --trace-id <trace_id>

import argparse
import json
from datetime import datetime
from pathlib import Path


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def attr_value(span: dict, key: str):
    for attr in span.get("Attributes") or []:
        if attr.get("Key") == key:
            return attr.get("Value", {}).get("Value")
    return None


def load_spans(path: Path) -> list[dict]:
    spans = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            spans.append(json.loads(line))
    return spans


def filter_spans(spans: list[dict], request_id: str | None, trace_id: str | None) -> list[dict]:
    filtered = spans
    if request_id is not None:
        filtered = [span for span in filtered if str(attr_value(span, "request.id")) == request_id]
    if trace_id is not None:
        filtered = [span for span in filtered if span.get("SpanContext", {}).get("TraceID") == trace_id]
    return sorted(filtered, key=lambda span: span.get("StartTime", ""))


def print_spans(spans: list[dict]) -> int:
    if not spans:
        return 1

    base = parse_time(spans[0]["StartTime"])
    print(f"{'offset_ms':>10}  {'dur_ms':>10}  {'trace_id':<32}  {'meta':<36}  span")
    for span in spans:
        start = parse_time(span["StartTime"])
        end = parse_time(span["EndTime"])
        offset_ms = (start - base).total_seconds() * 1000
        dur_ms = (end - start).total_seconds() * 1000
        trace_id = span.get("SpanContext", {}).get("TraceID", "")
        meta_parts = []
        for key, label in [
            ("batch.seq_num", "seq"),
            ("gate.next_verify_seq", "next"),
            ("gate.stable_seq_num", "stable"),
            ("parallel_batch.index", "pb"),
            ("parallel_batch.count", "pbn"),
            ("parallel_batch.size", "pbsz"),
            ("batch.request_count", "reqs"),
            ("exec.worker_count", "workers"),
        ]:
            value = attr_value(span, key)
            if value not in (None, ""):
                meta_parts.append(f"{label}={value}")
        meta = " ".join(meta_parts)
        print(f"{offset_ms:10.3f}  {dur_ms:10.3f}  {trace_id:<32}  {meta:<36}  {span.get('Name', '')}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize exported OpenTelemetry spans.")
    parser.add_argument("path", help="Path to a node .otel.json file")
    parser.add_argument("--request-id", help="Filter spans by request.id")
    parser.add_argument("--trace-id", help="Filter spans by TraceID")
    args = parser.parse_args()

    if not args.request_id and not args.trace_id:
        parser.error("provide --request-id or --trace-id")

    spans = load_spans(Path(args.path))
    filtered = filter_spans(spans, args.request_id, args.trace_id)
    return print_spans(filtered)


if __name__ == "__main__":
    raise SystemExit(main())
