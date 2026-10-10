import asyncio
import logging
import os
import time
from dataclasses import dataclass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("rpyc-load-balancer")


def parse_backends(value):
    backends = []
    for endpoint in value.split(","):
        host, separator, port = endpoint.strip().rpartition(":")
        if not separator or not host:
            raise ValueError("LB_BACKENDS entries must use HOST:PORT")
        backends.append(Backend(host, int(port)))
    if not backends:
        raise ValueError("LB_BACKENDS must contain at least one backend")
    return backends


@dataclass
class Backend:
    host: str
    port: int
    active: int = 0
    estimated_response_ms: float = 1.0
    unavailable_until: float = 0.0

    @property
    def name(self):
        return f"{self.host}:{self.port}"


class BackendPool:
    def __init__(self, backends, algorithm):
        algorithm = algorithm.strip().upper().replace("-", "_")
        if algorithm not in {"LEAST_CONNECTIONS", "LEAST_RESPONSE_TIME"}:
            raise ValueError(
                "LB_ALGORITHM must be LEAST_CONNECTIONS or LEAST_RESPONSE_TIME"
            )
        self.backends = backends
        self.algorithm = algorithm
        self.lock = asyncio.Lock()
        self.next_tie_break = 0
        self.ema_alpha = 0.2
        self.failure_cooldown_seconds = 2.0

    async def select(self):
        async with self.lock:
            now = time.monotonic()
            eligible = [
                (index, backend)
                for index, backend in enumerate(self.backends)
                if backend.unavailable_until <= now
            ]
            if not eligible:
                raise ConnectionError("All backends are temporarily unavailable")

            start = self.next_tie_break % len(self.backends)

            def selection_key(item):
                index, backend = item
                if self.algorithm == "LEAST_CONNECTIONS":
                    load = backend.active
                else:
                    load = backend.estimated_response_ms * (backend.active + 1)
                return load, (index - start) % len(self.backends)

            _, selected = min(eligible, key=selection_key)
            selected.active += 1
            self.next_tie_break = (self.backends.index(selected) + 1) % len(
                self.backends
            )
            logger.info(
                "selected backend=%s algorithm=%s active=%s estimates_ms=%s",
                selected.name,
                self.algorithm,
                self.active_counts(),
                self.response_estimates(),
            )
            return selected

    async def mark_unavailable(self, backend):
        async with self.lock:
            backend.unavailable_until = (
                time.monotonic() + self.failure_cooldown_seconds
            )

    async def record_response(self, backend, elapsed_ms):
        async with self.lock:
            backend.estimated_response_ms += self.ema_alpha * (
                elapsed_ms - backend.estimated_response_ms
            )
            logger.info(
                "response backend=%s algorithm=%s response_ms=%.2f estimate_ms=%.2f active=%s",
                backend.name,
                self.algorithm,
                elapsed_ms,
                backend.estimated_response_ms,
                self.active_counts(),
            )

    async def release(self, backend):
        async with self.lock:
            backend.active -= 1
            logger.info(
                "released backend=%s algorithm=%s active=%s",
                backend.name,
                self.algorithm,
                self.active_counts(),
            )

    def active_counts(self):
        return {backend.name: backend.active for backend in self.backends}

    def response_estimates(self):
        return {
            backend.name: round(backend.estimated_response_ms, 2)
            for backend in self.backends
        }


async def copy_stream(reader, writer, on_data=None):
    while True:
        data = await reader.read(65536)
        if not data:
            break
        if on_data is not None:
            await on_data()
        writer.write(data)
        await writer.drain()
    if writer.can_write_eof():
        writer.write_eof()
        await writer.drain()


async def close_writer(writer):
    if writer is not None:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def handle_client(client_reader, client_writer, pool, response_timeout):
    client = client_writer.get_extra_info("peername")
    attempted = set()
    backend = None
    backend_writer = None
    response_recorded = False
    connected_at = None

    try:
        while len(attempted) < len(pool.backends):
            try:
                backend = await pool.select()
            except ConnectionError as exc:
                logger.error("client=%s connection failed: %s", client, exc)
                return

            attempted.add(backend.name)
            try:
                backend_reader, backend_writer = await asyncio.wait_for(
                    asyncio.open_connection(backend.host, backend.port),
                    timeout=response_timeout,
                )
                connected_at = time.perf_counter()
                break
            except (OSError, asyncio.TimeoutError) as exc:
                logger.error(
                    "backend connection failed backend=%s client=%s error=%s",
                    backend.name,
                    client,
                    exc,
                )
                await pool.mark_unavailable(backend)
                await pool.release(backend)
                backend = None
        else:
            logger.error(
                "client=%s connection failed: no reachable backend", client
            )
            return

        async def record_first_response():
            nonlocal response_recorded
            if response_recorded:
                return
            response_recorded = True
            elapsed_ms = (time.perf_counter() - connected_at) * 1000
            await pool.record_response(backend, elapsed_ms)

        await asyncio.wait_for(
            asyncio.gather(
                copy_stream(client_reader, backend_writer),
                copy_stream(backend_reader, client_writer, record_first_response),
            ),
            timeout=response_timeout,
        )
    except asyncio.TimeoutError:
        logger.error("connection timed out client=%s backend=%s", client, getattr(backend, "name", None))
    except (ConnectionError, OSError, asyncio.IncompleteReadError) as exc:
        logger.error(
            "proxy connection failed client=%s backend=%s error=%s",
            client,
            getattr(backend, "name", None),
            exc,
        )
    finally:
        await close_writer(backend_writer)
        await close_writer(client_writer)
        if backend is not None:
            if connected_at is not None and not response_recorded:
                logger.warning(
                    "backend closed without response backend=%s client=%s",
                    backend.name,
                    client,
                )
            await pool.release(backend)


async def main():
    algorithm = os.getenv("LB_ALGORITHM", "LEAST_CONNECTIONS")
    backends = parse_backends(
        os.getenv(
            "LB_BACKENDS",
            "server1:18861,server2:18861,server3:18861",
        )
    )
    pool = BackendPool(backends, algorithm)
    host = os.getenv("LB_HOST", "0.0.0.0")
    port = int(os.getenv("LB_PORT", "18860"))
    response_timeout = float(os.getenv("LB_RESPONSE_TIMEOUT", "60"))

    logger.info(
        "starting TCP proxy algorithm=%s listen=%s:%s backends=%s",
        pool.algorithm,
        host,
        port,
        [backend.name for backend in backends],
    )
    server = await asyncio.start_server(
        lambda reader, writer: handle_client(
            reader, writer, pool, response_timeout
        ),
        host,
        port,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (ValueError, OSError) as exc:
        logger.critical("load balancer failed to start: %s", exc)
        raise
