import hashlib
import json
import os
import re
import time
from pathlib import Path

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
SERVER_NAME = os.getenv("SERVER_NAME", "server")
TEXT_DIR = Path("/app/server/texts")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

while True:
    try:
        redis_client.ping()
        break
    except redis.exceptions.ConnectionError:
        time.sleep(1)


def count_word_in_text(keyword, filepath):
    text = filepath.read_text(encoding="utf-8", errors="ignore")
    words = re.findall(r"\b[\w']+\b", text.lower())
    return sum(1 for word in words if word == keyword.lower())


class WordCountService(rpyc.Service):

    def exposed_count_words(self, text, filename):
        started = time.perf_counter()
        text = str(text)
        filename = Path(str(filename)).name
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        cache_key = f"wordcount:{filename}:{digest}"

        cached = redis_client.get(cache_key)
        cache_status = "HIT" if cached is not None else "MISS"
        word_count = int(cached) if cached is not None else len(text.split())

        if cached is None:
            redis_client.set(cache_key, word_count)

        response = {
            "word_count": word_count,
            "cache_status": cache_status,
            "server": SERVER_NAME,
            "server_processing_ms": round((time.perf_counter() - started) * 1000, 2),
        }
        return json.dumps(response)

    def exposed_count_word(self, keyword, filename):
        started = time.perf_counter()
        keyword = str(keyword).strip().lower()
        filename = Path(str(filename)).name

        if not keyword:
            raise ValueError("Keyword cannot be empty")

        filepath = TEXT_DIR / filename

        if not filepath.exists():
            raise FileNotFoundError(f"Text file not found: {filename}")

        cache_key = f"wordcount:{filename}:{keyword}"

        cached = redis_client.get(cache_key)

        if cached is not None:
            redis_client.incr(f"hot:{keyword}")
            print(
                f"[{SERVER_NAME}] CACHE HIT | "
                f"keyword={keyword} | file={filename} | count={cached} | "
                f"processing_ms={(time.perf_counter() - started) * 1000:.2f}",
                flush=True
            )
            return int(cached), True, SERVER_NAME

        count = count_word_in_text(keyword, filepath)

        redis_client.set(cache_key, count)
        redis_client.incr(f"hot:{keyword}")

        print(
            f"[{SERVER_NAME}] CACHE MISS | "
            f"keyword={keyword} | file={filename} | count={count} | "
            f"processing_ms={(time.perf_counter() - started) * 1000:.2f}",
            flush=True
        )

        return count, False, SERVER_NAME


server = ThreadedServer(
    WordCountService,
    hostname="0.0.0.0",
    port=18861
)

print(f"[{SERVER_NAME}] RPyC server listening on port 18861", flush=True)

server.start()