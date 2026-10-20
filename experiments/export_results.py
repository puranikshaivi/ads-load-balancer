import argparse
import csv
import json
from pathlib import Path


FIELDS = [
    "configuration",
    "algorithm",
    "workload",
    "offered_rate_requests_per_second",
    "repetitions",
    "duration_seconds",
    "scheduled_requests",
    "dispatched_requests",
    "completed_requests",
    "successful_requests",
    "failed_requests",
    "average_latency_ms",
    "p99_latency_ms",
    "actual_request_start_rate_per_second",
    "achieved_throughput_requests_per_second",
    "servers",
    "cache_hits",
    "cache_misses",
]


def load_rows(path):
    with Path(path).open(encoding="utf-8") as result_file:
        report = json.load(result_file)
    rows = []
    for aggregate in report["rates"]:
        row = {
            field: aggregate.get(field, "")
            for field in FIELDS
        }
        row["configuration"] = report["label"]
        row["algorithm"] = report["algorithm"]
        row["workload"] = report["workload"]
        row["duration_seconds"] = report["duration_seconds_per_run"]
        row["servers"] = json.dumps(aggregate["servers"], sort_keys=True)
        rows.append(row)
    return rows


def write_rows(path, rows):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as result_file:
        writer = csv.DictWriter(result_file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Export normalized CSVs from the raw repeated benchmark JSON."
    )
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--phase2-output", default="phase2_results.csv")
    parser.add_argument(
        "--phase3-output",
        default="results/phase3_results.csv",
    )
    args = parser.parse_args()
    results_dir = Path(args.results_dir)

    phase2_rows = []
    for filename in (
        "phase2-warm-cache-verified.json",
        "phase2-compute-miss-verified.json",
    ):
        phase2_rows.extend(load_rows(results_dir / filename))
    phase3_rows = []
    for filename in (
        "phase3-least-connections-warm-cache.json",
        "phase3-least-connections-compute-miss.json",
        "phase3-least-response-time-warm-cache.json",
        "phase3-least-response-time-compute-miss.json",
    ):
        phase3_rows.extend(load_rows(results_dir / filename))

    write_rows(args.phase2_output, phase2_rows)
    write_rows(args.phase3_output, phase3_rows)
    print(f"Phase 2 CSV saved to {args.phase2_output}")
    print(f"Phase 3 CSV saved to {args.phase3_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
