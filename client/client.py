import sys
import time

import rpyc


def main():
    if len(sys.argv) != 3:
        print("Usage: python client.py <keyword> <filename>")
        sys.exit(1)

    keyword = sys.argv[1]
    filename = sys.argv[2]

    conn = rpyc.connect("server", 18861)

    start = time.perf_counter()

    count, cache_hit, server_name = conn.root.count_word(
        keyword,
        filename
    )

    end = time.perf_counter()

    latency_ms = (end - start) * 1000

    print(f"Keyword: {keyword}")
    print(f"File: {filename}")
    print(f"Count: {count}")
    print(f"Cache hit: {cache_hit}")
    print(f"Server: {server_name}")
    print(f"Latency: {latency_ms:.3f} ms")

    conn.close()


if __name__ == "__main__":
    main()
    