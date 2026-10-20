import argparse
import concurrent.futures
import json
import math
import re
import statistics
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import rpyc


KEYWORDS = [
    "elizabeth",
    "darcy",
    "jane",
    "bingley",
    "lydia",
    "wickham",
    "bennet",
    "love",
    "marriage",
    "family",
    "sister",
    "daughter",
    "gentleman",
    "lady",
    "pride",
    "prejudice",
    "fortune",
    "society",
    "proposal",
    "relationship",
]


def parse_endpoint(endpoint):
    host, separator, port = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError(f"expected HOST:PORT, received {endpoint!r}")
    return host, int(port)


def percentile(values, percentile_value):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = max(0, math.ceil(percentile_value * len(ordered)) - 1)
    return ordered[index]


def make_request(
    endpoint,
    filename,
    keyword,
    expected_count,
    expected_cache_hit,
    timeout,
    measurement_origin=None,
):
    started = time.perf_counter()
    if measurement_origin is None:
        measurement_origin = started
    connection = None
    result = None
    try:
        host, port = parse_endpoint(endpoint)
        connection = rpyc.connect(
            host,
            port,
            config={"sync_request_timeout": timeout},
        )
        count, cache_hit, server_name = connection.root.count_word(
            keyword,
            filename,
        )
        latency_ms = (time.perf_counter() - started) * 1000
        errors = []
        if count != expected_count:
            errors.append(
                f"expected {expected_count} occurrences, received {count}"
            )
        if expected_cache_hit is not None and bool(cache_hit) != expected_cache_hit:
            expected = "HIT" if expected_cache_hit else "MISS"
            actual = "HIT" if cache_hit else "MISS"
            errors.append(f"expected cache {expected}, received {actual}")
        result = {
            "ok": not errors,
            "latency_ms": latency_ms,
            "server": server_name,
            "cache_hit": bool(cache_hit),
            "keyword": keyword,
            "errors": errors,
            "started_offset_seconds": started - measurement_origin,
            "completed_offset_seconds": (
                time.perf_counter() - measurement_origin
            ),
        }
    except Exception as exc:
        completed = time.perf_counter()
        result = {
            "ok": False,
            "latency_ms": (completed - started) * 1000,
            "server": None,
            "cache_hit": None,
            "keyword": keyword,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "started_offset_seconds": started - measurement_origin,
            "completed_offset_seconds": completed - measurement_origin,
        }
    finally:
        if connection is not None:
            try:
                connection.close()
            except (ConnectionError, EOFError, OSError) as exc:
                result["ok"] = False
                result["errors"].append(
                    f"{type(exc).__name__} while closing connection: {exc}"
                )
    return result


def workload_request(workload, run_id, index, word_counts):
    if workload == "warm_cache":
        keyword = KEYWORDS[index % len(KEYWORDS)]
        return keyword, word_counts[keyword], True
    keyword = f"benchmiss{run_id}{index}"
    return keyword, 0, False


def warm_up(endpoint, filename, workload, run_id, count, word_counts, timeout):
    results = []
    for index in range(count):
        keyword, expected_count, expected_cache_hit = workload_request(
            workload,
            f"{run_id}warm",
            index,
            word_counts,
        )
        result = make_request(
            endpoint,
            filename,
            keyword,
            expected_count,
            None if workload == "warm_cache" else expected_cache_hit,
            timeout,
        )
        results.append(result)
    return results


def run_rate(
    endpoint,
    filename,
    workload,
    algorithm,
    rate,
    duration,
    concurrency,
    warmup_requests,
    timeout,
    word_counts,
):
    run_id = uuid.uuid4().hex
    warmup = warm_up(
        endpoint,
        filename,
        workload,
        run_id,
        warmup_requests,
        word_counts,
        timeout,
    )
    measurement_started = time.perf_counter()
    measurement_ends = measurement_started + duration
    scheduled_requests = math.ceil(rate * duration)
    results = []
    in_flight = set()
    dispatched_requests = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrency,
    ) as executor:
        for index in range(scheduled_requests):
            target = measurement_started + index / rate
            delay = target - time.perf_counter()
            if delay > 0:
                time.sleep(delay)

            keyword, expected_count, expected_cache_hit = workload_request(
                workload,
                run_id,
                index,
                word_counts,
            )
            in_flight.add(executor.submit(
                make_request,
                endpoint,
                filename,
                keyword,
                expected_count,
                expected_cache_hit,
                timeout,
                measurement_started,
            ))
            dispatched_requests += 1

        remaining_window = measurement_ends - time.perf_counter()
        if remaining_window > 0:
            time.sleep(remaining_window)

        drain_started = time.perf_counter()
        for future in concurrent.futures.as_completed(in_flight):
            results.append(future.result())
        drain_seconds = time.perf_counter() - drain_started
        actual_duration = time.perf_counter() - measurement_started

    window_results = [
        result for result in results
        if result["completed_offset_seconds"] <= duration
    ]
    successful = [result for result in results if result["ok"]]
    latencies = [result["latency_ms"] for result in results]
    server_counts = Counter(
        result["server"] for result in successful if result["server"] is not None
    )
    cache_hits = sum(result["cache_hit"] is True for result in successful)
    cache_misses = sum(result["cache_hit"] is False for result in successful)
    errors = [
        error
        for result in results
        for error in result["errors"]
    ]

    return {
        "algorithm": algorithm,
        "workload": workload,
        "offered_rate_requests_per_second": rate,
        "duration_seconds": duration,
        "actual_duration_seconds": round(actual_duration, 3),
        "concurrency_limit": concurrency,
        "warmup_requests": warmup_requests,
        "scheduled_requests": scheduled_requests,
        "dispatched_requests": dispatched_requests,
        "missed_due_to_concurrency": 0,
        "completed_requests": len(results),
        "successful_requests": len(successful),
        "failed_requests": len(results) - len(successful),
        "average_latency_ms": (
            round(statistics.mean(latencies), 2) if latencies else None
        ),
        "p99_latency_ms": round(percentile(latencies, 0.99), 2),
        "actual_request_start_rate_per_second": round(
            dispatched_requests / actual_duration,
            2,
        ),
        "achieved_throughput_requests_per_second": round(
            len(successful) / actual_duration,
            2,
        ),
        "completed_during_offered_window": len(window_results),
        "window_completion_rate_per_second": round(
            len(window_results) / duration,
            2,
        ),
        "drain_seconds": round(drain_seconds, 3),
        "servers": dict(server_counts),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "warmup_failures": sum(not result["ok"] for result in warmup),
        "errors": errors[:20],
    }


def aggregate_runs(runs, rates, duration, repetitions):
    aggregates = []
    for rate in rates:
        matching = [
            run for run in runs
            if run["offered_rate_requests_per_second"] == rate
        ]
        total_duration = sum(
            run.get("duration_seconds", duration)
            for run in matching
        )
        total_actual_duration = sum(
            run.get("actual_duration_seconds", run.get("duration_seconds", duration))
            for run in matching
        )
        successes = sum(run["successful_requests"] for run in matching)
        servers = Counter()
        for run in matching:
            servers.update(run["servers"])
        average_latencies = [
            run["average_latency_ms"]
            for run in matching
            if run["average_latency_ms"] is not None
        ]
        aggregates.append({
            "offered_rate_requests_per_second": rate,
            "repetitions": len(matching),
            "duration_seconds": (
                round(total_duration / len(matching), 3) if matching else 0
            ),
            "actual_duration_seconds": round(total_actual_duration, 3),
            "scheduled_requests": sum(
                run["scheduled_requests"] for run in matching
            ),
            "dispatched_requests": sum(
                run["dispatched_requests"] for run in matching
            ),
            "completed_requests": sum(
                run["completed_requests"] for run in matching
            ),
            "successful_requests": successes,
            "failed_requests": sum(
                run["failed_requests"] for run in matching
            ),
            "missed_due_to_concurrency": sum(
                run.get("missed_due_to_concurrency", 0) for run in matching
            ),
            "average_latency_ms": (
                round(statistics.mean(average_latencies), 2)
                if average_latencies else None
            ),
            "p99_latency_ms": round(statistics.mean(
                run["p99_latency_ms"] for run in matching
            ), 2),
            "actual_request_start_rate_per_second": round(
                sum(run["dispatched_requests"] for run in matching)
                / total_actual_duration,
                2,
            ),
            "achieved_throughput_requests_per_second": round(
                successes / total_actual_duration,
                2,
            ),
            "servers": dict(servers),
            "cache_hits": sum(run["cache_hits"] for run in matching),
            "cache_misses": sum(run["cache_misses"] for run in matching),
        })
    return aggregates


def build_report(args):
    file_path = Path(args.file)
    text = file_path.read_text(encoding="utf-8")
    word_counts = Counter(re.findall(r"\b[\w']+\b", text.lower()))
    rates = [int(rate) for rate in args.rates.split(",")]
    if not rates or any(rate < 1 for rate in rates):
        raise ValueError("--rates must be comma-separated positive integers")
    if args.duration <= 0 or args.concurrency < 1 or args.repetitions < 1:
        raise ValueError("duration, concurrency, and repetitions must be positive")
    if args.requests_per_rate is not None and args.requests_per_rate < 1:
        raise ValueError("--requests-per-rate must be positive")
    if args.warmup_requests < 0:
        raise ValueError("--warmup-requests cannot be negative")

    runs = []
    for rate in rates:
        duration = (
            args.requests_per_rate / rate
            if args.requests_per_rate is not None
            else args.duration
        )
        for repetition in range(1, args.repetitions + 1):
            print(
                f"{args.algorithm} workload={args.workload} "
                f"rate={rate}/s repetition={repetition}/{args.repetitions}",
                flush=True,
            )
            runs.append(run_rate(
                args.endpoint,
                file_path.name,
                args.workload,
                args.algorithm,
                rate,
                duration,
                args.concurrency,
                args.warmup_requests,
                args.timeout,
                word_counts,
            ))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": args.endpoint,
        "algorithm": args.algorithm,
        "workload": args.workload,
        "label": f"{args.algorithm} - {args.workload}",
        "file": str(file_path),
        "rate_control": (
            "paced open-loop submission; all scheduled requests are queued "
            "when worker concurrency is saturated"
        ),
        "latency_boundary": (
            "client perf_counter immediately before TCP/RPyC connection "
            "through response received, before connection close"
        ),
        "duration_seconds_per_run": (
            None if args.requests_per_rate is not None else args.duration
        ),
        "requests_per_rate": args.requests_per_rate,
        "repetitions_per_rate": args.repetitions,
        "concurrency_limit": args.concurrency,
        "warmup_requests_per_run": args.warmup_requests,
        "requested_rates": rates,
        "rates": aggregate_runs(
            runs,
            rates,
            args.duration,
            args.repetitions,
        ),
        "runs": runs,
    }


def print_terminal_summary(report):
    repetitions_by_rate = Counter()
    print(
        f"\n{report['algorithm']} | {report['workload']} | "
        f"{report['endpoint']}"
    )
    print(
        "Rate     Run  Requests  Submitted  Success  Errors  "
        "Mean ms  P99 ms  Throughput/s  Cache hits  Backend distribution"
    )
    print("-" * 118)
    for run in report["runs"]:
        rate = run["offered_rate_requests_per_second"]
        repetitions_by_rate[rate] += 1
        cache_hit_percent = (
            100 * run["cache_hits"] / run["successful_requests"]
            if run["successful_requests"] else 0
        )
        distribution = ", ".join(
            f"{name}={count}"
            for name, count in sorted(run["servers"].items())
        ) or "none"
        print(
            f"{rate:>4}/s  "
            f"{repetitions_by_rate[rate]:>3}  "
            f"{run['scheduled_requests']:>8}  "
            f"{run['dispatched_requests']:>4}  "
            f"{run['successful_requests']:>7}  "
            f"{run['failed_requests']:>6}  "
            f"{run['average_latency_ms'] or 0:>7.2f}  "
            f"{run['p99_latency_ms']:>6.2f}  "
            f"{run['achieved_throughput_requests_per_second']:>12.2f}  "
            f"{cache_hit_percent:>8.1f}%  "
            f"{distribution}"
        )
        if run["missed_due_to_concurrency"]:
            print(
                f"      Note: {run['missed_due_to_concurrency']} offered "
                "requests were skipped at the concurrency limit."
            )


def main():
    parser = argparse.ArgumentParser(
        description="Run repeatable paced-load benchmarks against an RPyC endpoint."
    )
    parser.add_argument("--endpoint", required=True, help="HOST:PORT endpoint")
    parser.add_argument(
        "--algorithm",
        required=True,
        help="Algorithm label, e.g. PHASE2_SINGLE_SERVER, LEAST_CONNECTIONS",
    )
    parser.add_argument(
        "--workload",
        choices=("warm_cache", "compute_miss"),
        required=True,
    )
    parser.add_argument(
        "--file",
        default="/app/server/texts/pride-and-prejudice.txt",
    )
    parser.add_argument("--rates", default="10,25,50,100,150")
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument(
        "--requests-per-rate",
        type=int,
        help="schedule this many requests at each rate (duration is derived)",
    )
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--warmup-requests", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        report = build_report(args)
    except (OSError, UnicodeError, ValueError) as exc:
        parser.error(str(exc))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print_terminal_summary(report)
    print(f"Results saved to {output_path}")
    failures = sum(run["failed_requests"] for run in report["runs"])
    warmup_failures = sum(run["warmup_failures"] for run in report["runs"])
    return 1 if failures or warmup_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
