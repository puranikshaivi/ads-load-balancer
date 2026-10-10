import argparse
import os
import time

import rpyc


def parse_endpoint(endpoint):
    host, separator, port = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError(f"expected HOST:PORT, received {endpoint!r}")
    return host, int(port)


def main():
    parser = argparse.ArgumentParser(
        description="Count occurrences of a word using the RPyC service."
    )
    parser.add_argument("keyword", help="word to count")
    parser.add_argument("filename", help="text filename on the server")
    parser.add_argument(
        "--server",
        default=os.getenv("LOAD_BALANCER_ENDPOINT", "localhost:18860"),
        help="HOST:PORT of the load balancer or a server",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    host, port = parse_endpoint(args.server)
    connection = rpyc.connect(
        host,
        port,
        config={"sync_request_timeout": args.timeout},
    )
    try:
        started = time.perf_counter()
        count, cache_hit, server_name = connection.root.count_word(
            args.keyword,
            args.filename,
        )
        latency_ms = (time.perf_counter() - started) * 1000
    finally:
        connection.close()

    print(f"Keyword: {args.keyword}")
    print(f"File: {args.filename}")
    print(f"Count: {count}")
    print(f"Cache: {'HIT' if cache_hit else 'MISS'}")
    print(f"Server: {server_name}")
    print(f"Client latency: {latency_ms:.2f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
