import asyncio
import logging
import os
import struct
import time
import zlib
from dataclasses import dataclass

from rpyc.core import brine, consts


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
    response_samples: int = 0

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
        if not backends:
            raise ValueError("At least one backend is required")

        self.backends = backends
        self.algorithm = algorithm
        self.lock = asyncio.Lock()
        self.next_tie_break = 0
        self.ema_alpha = 0.2

    async def select(self):
        """Choose and reserve the least-loaded backend for the selected policy."""
        async with self.lock:
            start = self.next_tie_break % len(self.backends)

            def score(backend):
                if self.algorithm == "LEAST_CONNECTIONS":
                    return backend.active
                return backend.estimated_response_ms * (backend.active + 1)

            scores = [score(backend) for backend in self.backends]
            lowest_score = min(scores)
            tied = [
                index
                for index, backend_score in enumerate(scores)
                if backend_score == lowest_score
            ]
            selected_index = min(
                tied,
                key=lambda index: (index - start) % len(self.backends),
            )
            selected = self.backends[selected_index]

            selected.active += 1
            self.next_tie_break = (selected_index + 1) % len(self.backends)

            logger.info(
                "selected backend=%s algorithm=%s active=%s estimates_ms=%s",
                selected.name,
                self.algorithm,
                self.active_counts(),
                self.response_estimates(),
            )
            return selected

    async def record_response(self, backend, elapsed_ms):
        async with self.lock:
            backend.estimated_response_ms += self.ema_alpha * (
                elapsed_ms - backend.estimated_response_ms
            )
            backend.response_samples += 1
            logger.info(
                "response backend=%s algorithm=%s response_ms=%.2f "
                "estimate_ms=%.2f samples=%s active=%s",
                backend.name,
                self.algorithm,
                elapsed_ms,
                backend.estimated_response_ms,
                backend.response_samples,
                self.active_counts(),
            )

    async def release(self, backend):
        async with self.lock:
            if backend.active <= 0:
                raise RuntimeError(
                    f"Cannot release an unreserved backend: {backend.name}"
                )
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


class RPyCFrameParser:
    """Observe complete RPyC frames without changing the forwarded stream."""

    header = struct.Struct("!LB")
    flusher = b"\n"

    def __init__(self):
        self._header = bytearray()
        self._frame_bytes_remaining = None
        self._compressed = False
        self._payload = bytearray()

    def feed(self, data):
        frames = []
        offset = 0
        while offset < len(data):
            if self._frame_bytes_remaining is None:
                needed = self.header.size - len(self._header)
                amount = min(needed, len(data) - offset)
                self._header.extend(data[offset:offset + amount])
                offset += amount
                if len(self._header) == self.header.size:
                    payload_length, compression_flag = self.header.unpack(
                        self._header
                    )
                    self._header.clear()
                    self._compressed = bool(compression_flag)
                    self._frame_bytes_remaining = payload_length + len(
                        self.flusher
                    )
                    self._payload.clear()
            else:
                amount = min(
                    self._frame_bytes_remaining,
                    len(data) - offset,
                )
                payload_bytes = data[offset:offset + amount]
                payload_length_remaining = (
                    self._frame_bytes_remaining - len(self.flusher)
                )
                payload_amount = min(amount, max(0, payload_length_remaining))
                self._payload.extend(payload_bytes[:payload_amount])
                self._frame_bytes_remaining -= amount
                offset += amount

                if self._frame_bytes_remaining == 0:
                    terminator_valid = (
                        bool(payload_bytes)
                        and payload_bytes[-1:] == self.flusher
                    )
                    frames.append((
                        self._compressed,
                        bytes(self._payload),
                        terminator_valid,
                    ))
                    self._frame_bytes_remaining = None
                    self._payload.clear()
        return frames


class RPyCResponseTracker:
    """Measure completed RPyC method calls by matching their sequence IDs."""

    def __init__(self, pool, backend):
        self.pool = pool
        self.backend = backend
        self.client_frames = RPyCFrameParser()
        self.backend_frames = RPyCFrameParser()
        self.pending_requests = {}

    @staticmethod
    def _decode_frame(frame):
        compressed, payload, terminator_valid = frame
        if not terminator_valid:
            raise ValueError("Invalid RPyC frame terminator")
        if compressed:
            payload = zlib.decompress(payload)
        message_type, = brine.I1.unpack(payload[:1])
        sequence, arguments = brine.load(payload[1:])
        return message_type, sequence, arguments

    async def observe_client_data(self, data):
        for frame in self.client_frames.feed(data):
            try:
                message_type, sequence, arguments = self._decode_frame(frame)
            except (EOFError, IndexError, ValueError, struct.error, zlib.error) as exc:
                logger.warning(
                    "could not inspect client RPyC frame backend=%s error=%s",
                    self.backend.name,
                    exc,
                )
                continue

            if message_type != consts.MSG_REQUEST:
                continue
            try:
                handler, _ = arguments
            except (TypeError, ValueError):
                logger.warning(
                    "could not inspect client RPyC request backend=%s",
                    self.backend.name,
                )
                continue
            if (
                isinstance(sequence, int)
                and handler in (consts.HANDLE_CALL, consts.HANDLE_CALLATTR)
            ):
                self.pending_requests[sequence] = time.perf_counter()

    async def observe_backend_data(self, data):
        for frame in self.backend_frames.feed(data):
            try:
                message_type, sequence, _ = self._decode_frame(frame)
            except (EOFError, IndexError, ValueError, struct.error, zlib.error) as exc:
                logger.warning(
                    "could not inspect backend RPyC frame backend=%s error=%s",
                    self.backend.name,
                    exc,
                )
                continue

            if message_type not in {consts.MSG_REPLY, consts.MSG_EXCEPTION}:
                continue
            if not isinstance(sequence, int):
                logger.warning(
                    "could not inspect backend RPyC sequence ID backend=%s",
                    self.backend.name,
                )
                continue
            started_at = self.pending_requests.pop(sequence, None)
            if started_at is None:
                continue
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            await self.pool.record_response(self.backend, elapsed_ms)


async def copy_stream(reader, writer, on_data=None):
    """Forward raw TCP bytes from one side of the proxy to the other."""
    while True:
        data = await reader.read(65536)
        if not data:
            break
        if on_data is not None:
            await on_data(data)
        writer.write(data)
        await writer.drain()

    if writer.can_write_eof():
        writer.write_eof()
        await writer.drain()


async def forward_bidirectional(
    client_reader,
    client_writer,
    backend_reader,
    backend_writer,
    response_tracker,
):
    tasks = [
        asyncio.create_task(copy_stream(
            client_reader,
            backend_writer,
            response_tracker.observe_client_data,
        )),
        asyncio.create_task(copy_stream(
            backend_reader,
            client_writer,
            response_tracker.observe_backend_data,
        )),
    ]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def close_writer(writer):
    if writer is None:
        return
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, OSError):
        pass


async def handle_client(client_reader, client_writer, pool, response_timeout):
    client = client_writer.get_extra_info("peername")
    backend = None
    backend_writer = None
    response_tracker = None
    reserved = False

    try:
        backend = await pool.select()
        reserved = True
        try:
            backend_reader, backend_writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=response_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            logger.error(
                "backend connection failed backend=%s client=%s error=%s",
                backend.name,
                client,
                exc,
            )
            return

        response_tracker = RPyCResponseTracker(pool, backend)
        await asyncio.wait_for(
            forward_bidirectional(
                client_reader,
                client_writer,
                backend_reader,
                backend_writer,
                response_tracker,
            ),
            timeout=response_timeout,
        )
    except asyncio.TimeoutError:
        logger.error(
            "proxy connection timed out client=%s backend=%s",
            client,
            getattr(backend, "name", None),
        )
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
        if reserved:
            if response_tracker is not None and response_tracker.pending_requests:
                logger.warning(
                    "backend closed with %s unanswered calls backend=%s client=%s",
                    len(response_tracker.pending_requests),
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
            reader,
            writer,
            pool,
            response_timeout,
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
