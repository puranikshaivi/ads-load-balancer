import argparse
import concurrent.futures
import json
import math
import os
import re
import statistics
import time
from collections import Counter
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


def send_request(endpoint, filename, keyword, expected_count, timeout):
    started = time.perf_counter()
    connection = None
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
        if count != expected_count:
            return {
                "ok": False,
                "latency_ms": latency_ms,
                "keyword": keyword,
                "error": (
                    f"expected {expected_count} occurrences, received {count}"
                ),
            }
        return {
            "ok": True,
            "latency_ms": latency_ms,
            "keyword": keyword,
            "server": server_name,
            "cache_hit": bool(cache_hit),
        }
    except Exception as exc:
        return {
            "ok": False,
            "latency_ms": (time.perf_counter() - started) * 1000,
            "keyword": keyword,
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if connection is not None:
            connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark concurrent, one-RPyC-connection-per-request calls."
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("LOAD_BALANCER_ENDPOINT", "localhost:18860"),
        help="HOST:PORT of the load balancer or one server",
    )
    parser.add_argument(
        "--file",
        default="/app/server/texts/pride-and-prejudice.txt",
        help="same UTF-8 text file used by every server",
    )
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--output",
        default="/app/results/benchmark-results.json",
        help="JSON summary output path",
    )
    args = parser.parse_args()

    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")

    file_path = Path(args.file)
    try:
        text = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        parser.error(f"cannot read {file_path}: {exc}")

    words = re.findall(r"\b[\w']+\b", text.lower())
    word_counts = Counter(words)
    filename = file_path.name
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        futures = [
            executor.submit(
                send_request,
                args.endpoint,
                filename,
                KEYWORDS[index % len(KEYWORDS)],
                word_counts[KEYWORDS[index % len(KEYWORDS)]],
                args.timeout,
            )
            for index in range(args.requests)
        ]
        results = [future.result() for future in futures]
    elapsed_seconds = time.perf_counter() - started

    successful = [result for result in results if result["ok"]]
    latencies = [result["latency_ms"] for result in results]
    servers = Counter(result["server"] for result in successful)
    summary = {
        "endpoint": args.endpoint,
        "file": str(file_path),
        "requests": args.requests,
        "concurrency": args.concurrency,
        "successful_requests": len(successful),
        "failed_or_incorrect_requests": len(results) - len(successful),
        "average_latency_ms": round(statistics.mean(latencies), 2),
        "p99_latency_ms": round(percentile(latencies, 0.99), 2),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "requests_per_second": round(args.requests / elapsed_seconds, 2),
        "servers": dict(servers),
        "cache_hits": sum(result["cache_hit"] for result in successful),
        "cache_misses": sum(not result["cache_hit"] for result in successful),
        "errors": [result["error"] for result in results if not result["ok"]][:10],
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Full summary saved to {output_path}")
    return 0 if summary["failed_or_incorrect_requests"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
