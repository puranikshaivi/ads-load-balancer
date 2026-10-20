import argparse
import csv
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path


RESPONSE_PATTERN = re.compile(
    r"response backend=([^ ]+) .*?response_ms=([0-9.]+)"
)


def read_response_events(log_path):
    events = []
    with Path(log_path).open(encoding="utf-8-sig") as log_file:
        for line in log_file:
            match = RESPONSE_PATTERN.search(line)
            if match:
                backend = match.group(1).rsplit(":", 1)[0]
                events.append((backend, float(match.group(2))))
    if not events:
        raise ValueError(f"no proxy response timings found in {log_path}")
    return events


def nearest_rank_p99(values):
    return sorted(values)[max(0, math.ceil(0.99 * len(values)) - 1)]


def summarize_proxy_timings(log_path, result_paths):
    events = read_response_events(log_path)
    event_index = 0
    grouped = defaultdict(list)

    for result_path in result_paths:
        with Path(result_path).open(encoding="utf-8") as result_file:
            report = json.load(result_file)
        for run in report["runs"]:
            if run.get("failed_requests", 0) or run.get("warmup_failures", 0):
                raise ValueError(
                    "cannot align proxy timings for a run with failed requests"
                )
            event_index += run["warmup_requests"]
            end = event_index + run["dispatched_requests"]
            if end > len(events):
                raise ValueError(
                    "proxy log has fewer response events than the benchmark "
                    "runs require"
                )
            for backend, response_ms in events[event_index:end]:
                grouped[(report["workload"], backend)].append(response_ms)
            event_index = end

    if event_index != len(events):
        raise ValueError(
            f"matched {event_index} proxy events but found {len(events)}; "
            "the log may include unrelated calls or belong to different runs"
        )

    rows = []
    for (workload, backend), samples in sorted(grouped.items()):
        rows.append({
            "workload": workload,
            "backend": backend,
            "measured_requests": len(samples),
            "mean_proxy_response_ms": round(statistics.mean(samples), 2),
            "p99_proxy_response_ms": round(nearest_rank_p99(samples), 2),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Summarize proxy response timings aligned to benchmark runs."
    )
    parser.add_argument("proxy_log")
    parser.add_argument("results", nargs="+", help="ordered benchmark JSON files")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        rows = summarize_proxy_timings(args.proxy_log, args.results)
    except (OSError, UnicodeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        parser.error(str(exc))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=[
                "workload",
                "backend",
                "measured_requests",
                "mean_proxy_response_ms",
                "p99_proxy_response_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Proxy timing summary saved to {output_path}")


if __name__ == "__main__":
    main()
