import csv
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import rpyc

SERVER_HOST = "server"
SERVER_PORT = 18861
FILENAME = "sample.txt"

RATES = [10, 60, 80, 100, 150]

DURATION_SECONDS = 10
MAX_WORKERS = 50

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
    "relationship"
]

thread_local = threading.local()


def get_connection():
    if not hasattr(thread_local, "conn"):
        thread_local.conn = rpyc.connect(
            SERVER_HOST,
            SERVER_PORT
        )
    return thread_local.conn


def close_connection():
    if hasattr(thread_local, "conn"):
        try:
            thread_local.conn.close()
        except Exception:
            pass
        del thread_local.conn


def warmup_connection():
    conn = get_connection()
    conn.root.count_word("the", FILENAME)


def send_request(index):
    keyword = KEYWORDS[index % len(KEYWORDS)]

    conn = get_connection()

    start = time.perf_counter()

    count, cache_hit, server_name = conn.root.count_word(
        keyword,
        FILENAME
    )

    end = time.perf_counter()

    return {
        "keyword": keyword,
        "count": count,
        "cache_hit": cache_hit,
        "server": server_name,
        "latency_ms": (end - start) * 1000
    }


def percentile(values, p):
    values = sorted(values)

    if not values:
        return 0

    index = (len(values) - 1) * p

    lower = int(index)
    upper = min(lower + 1, len(values))

    weight = index - lower

    return values[lower] + (values[upper] - values[lower]) * weight


def run_experiment(rate):
    total_requests = rate * DURATION_SECONDS

    print()
    print(f"Running {rate} requests/second")
    print(f"Total requests: {total_requests}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        warmup_tasks = [
            executor.submit(warmup_connection)
            for _ in range(MAX_WORKERS)
        ]

        for task in warmup_tasks:
            task.result()

        futures = []

        interval = 1.0 / rate
        next_request = time.perf_counter()

        for i in range(total_requests):
            now = time.perf_counter()

            if now < next_request:
                time.sleep(next_request - now)

            futures.append(
                executor.submit(send_request, i)
            )

            next_request += interval

        results = []

        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Request failed: {e}")

    latencies = [
        result["latency_ms"]
        for result in results
    ]

    average = statistics.mean(latencies)
    p99 = percentile(latencies, 0.99)

    cache_hits = sum(
        1 for result in results
        if result["cache_hit"]
    )

    print(f"Successful requests: {len(results)}")
    print(f"Average latency: {average:.3f} ms")
    print(f"P99 latency: {p99:.3f} ms")
    print(f"Cache hits: {cache_hits}")

    return {
        "rate": rate,
        "requests": len(results),
        "average_latency_ms": average,
        "p99_latency_ms": p99,
        "cache_hits": cache_hits
    }


def main():
    results = []

    for rate in RATES:
        result = run_experiment(rate)
        results.append(result)

        time.sleep(3)

    with open(
        "/app/experiments/phase2_results.csv",
        "w",
        newline=""
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "rate",
                "requests",
                "average_latency_ms",
                "p99_latency_ms",
                "cache_hits"
            ]
        )

        writer.writeheader()
        writer.writerows(results)

    print()
    print("Experiment complete")
    print("Results saved to /app/experiments/phase2_results.csv")

    for worker in range(MAX_WORKERS):
        pass


if __name__ == "__main__":
    main()