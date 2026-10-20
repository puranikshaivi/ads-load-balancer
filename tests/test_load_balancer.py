import asyncio
import socket
import struct
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

import rpyc
from rpyc.core import brine, consts
from rpyc.utils.server import ThreadedServer

from load_balancer.proxy import (
    Backend,
    BackendPool,
    RPyCFrameParser,
    RPyCResponseTracker,
    handle_client,
)


def rpyc_frame(message_type, sequence, arguments, compressed=0):
    payload = brine.I1.pack(message_type) + brine.dump((sequence, arguments))
    return struct.pack("!LB", len(payload), compressed) + payload + b"\n"


class FakeClientWriter:
    def __init__(self):
        self.closed = False

    def get_extra_info(self, name):
        return ("127.0.0.1", 12345) if name == "peername" else None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class RecordingPool:
    def __init__(self):
        self.responses = []

    async def record_response(self, backend, elapsed_ms):
        self.responses.append((backend, elapsed_ms))


class FrameParserTests(unittest.TestCase):
    def test_parses_split_and_coalesced_frames(self):
        parser = RPyCFrameParser()
        first = rpyc_frame(consts.MSG_REQUEST, 1, (consts.HANDLE_CALL, ()))
        second = rpyc_frame(consts.MSG_REPLY, 1, "ok")

        self.assertEqual(parser.feed(first[:3]), [])
        self.assertEqual(parser.feed(first[3:] + second), [
            (False, first[5:-1], True),
            (False, second[5:-1], True),
        ])


class ResponseTrackerTests(unittest.IsolatedAsyncioTestCase):
    async def test_matches_real_call_frames_by_sequence_id(self):
        pool = RecordingPool()
        backend = Backend("server1", 18861)
        tracker = RPyCResponseTracker(pool, backend)
        first_call = rpyc_frame(
            consts.MSG_REQUEST,
            4,
            (consts.HANDLE_CALL, ()),
        )
        second_call = rpyc_frame(
            consts.MSG_REQUEST,
            9,
            (consts.HANDLE_CALLATTR, ()),
        )
        setup = rpyc_frame(
            consts.MSG_REQUEST,
            1,
            (consts.HANDLE_GETROOT, ()),
        )

        await tracker.observe_client_data(setup + first_call[:4])
        await tracker.observe_client_data(first_call[4:] + second_call)
        await asyncio.sleep(0.01)
        await tracker.observe_backend_data(
            rpyc_frame(consts.MSG_REPLY, 9, "second")
            + rpyc_frame(consts.MSG_EXCEPTION, 4, "first")
        )

        self.assertEqual(len(pool.responses), 2)
        self.assertTrue(all(item[0] is backend for item in pool.responses))
        self.assertTrue(all(elapsed_ms > 0 for _, elapsed_ms in pool.responses))


class BackendPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_least_connections_tracks_load_and_rotates_exact_ties(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")

        first = await pool.select()
        second = await pool.select()

        self.assertIsNot(first, second)
        self.assertEqual(pool.active_counts(), {
            "server1:18861": 1,
            "server2:18861": 1,
        })
        await pool.release(first)
        third = await pool.select()
        self.assertIs(third, first)
        await pool.release(second)
        await pool.release(third)
        self.assertEqual(pool.active_counts(), {
            "server1:18861": 0,
            "server2:18861": 0,
        })

    async def test_least_connections_prefers_fewer_active_connections(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        backends[0].active = 2
        pool = BackendPool(backends, "LEAST_CONNECTIONS")

        selected = await pool.select()

        self.assertIs(selected, backends[1])
        await pool.release(selected)

    async def test_failed_backend_connection_fails_over_to_next_backend(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")
        writer = FakeClientWriter()
        backend_writer = FakeClientWriter()
        open_connection = AsyncMock(
            side_effect=[
                OSError("backend unavailable"),
                (None, backend_writer),
            ]
        )

        with patch(
            "load_balancer.proxy.asyncio.open_connection",
            open_connection,
        ), patch(
            "load_balancer.proxy.forward_bidirectional",
            new_callable=AsyncMock,
        ):
            await handle_client(None, writer, pool, 0.1)

        self.assertEqual(open_connection.await_count, 2)
        self.assertTrue(writer.closed)
        self.assertTrue(backend_writer.closed)
        self.assertGreater(backends[0].unavailable_until, time.monotonic())
        self.assertEqual(pool.active_counts(), {
            "server1:18861": 0,
            "server2:18861": 0,
        })

    async def test_selection_skips_unavailable_backends(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")
        await pool.mark_unavailable(backends[0])

        selected = await pool.select()

        self.assertIs(selected, backends[1])
        await pool.release(selected)

    async def test_health_check_marks_failed_backend_unavailable(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")
        healthy_writer = FakeClientWriter()
        open_connection = AsyncMock(
            side_effect=[
                OSError("backend unavailable"),
                (None, healthy_writer),
            ]
        )

        with patch(
            "load_balancer.proxy.asyncio.open_connection",
            open_connection,
        ):
            await pool.health_check()

        self.assertGreater(backends[0].unavailable_until, time.monotonic())
        self.assertEqual(backends[1].unavailable_until, 0.0)
        self.assertTrue(healthy_writer.closed)

    async def test_health_check_restores_recovered_backend(self):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")
        await pool.mark_unavailable(backends[0])
        writers = [FakeClientWriter(), FakeClientWriter()]

        with patch(
            "load_balancer.proxy.asyncio.open_connection",
            new_callable=AsyncMock,
            side_effect=[(None, writers[0]), (None, writers[1])],
        ):
            await pool.health_check()

        self.assertEqual(backends[0].unavailable_until, 0.0)
        self.assertTrue(all(writer.closed for writer in writers))

    async def test_all_unavailable_backends_close_client_without_leaking_reservations(
        self,
    ):
        backends = [Backend("server1", 18861), Backend("server2", 18861)]
        pool = BackendPool(backends, "LEAST_CONNECTIONS")
        for backend in backends:
            await pool.mark_unavailable(backend)
        writer = FakeClientWriter()
        open_connection = AsyncMock()

        with patch(
            "load_balancer.proxy.asyncio.open_connection",
            open_connection,
        ):
            await handle_client(None, writer, pool, 0.1)

        open_connection.assert_not_awaited()
        self.assertTrue(writer.closed)
        self.assertEqual(pool.active_counts(), {
            "server1:18861": 0,
            "server2:18861": 0,
        })

    async def test_release_cannot_make_active_count_negative(self):
        pool = BackendPool([Backend("server1", 18861)], "LEAST_CONNECTIONS")

        with self.assertRaises(RuntimeError):
            await pool.release(pool.backends[0])

    async def test_lrt_uses_exact_lowest_score_without_tolerance(self):
        backends = [
            Backend("server1", 18861, estimated_response_ms=10.0),
            Backend("server2", 18861, estimated_response_ms=9.5),
            Backend("server3", 18861, estimated_response_ms=30.0),
        ]
        pool = BackendPool(backends, "LEAST_RESPONSE_TIME")

        selected = await pool.select()

        self.assertIs(selected, backends[1])
        await pool.release(selected)

    async def test_lrt_combines_response_estimate_and_active_connections(self):
        backends = [
            Backend("server1", 18861, active=1, estimated_response_ms=5.0),
            Backend("server2", 18861, estimated_response_ms=8.0),
            Backend("server3", 18861, estimated_response_ms=20.0),
        ]
        pool = BackendPool(backends, "LEAST_RESPONSE_TIME")

        selected = await pool.select()

        self.assertIs(selected, backends[1])
        await pool.release(selected)

    async def test_lrt_adapts_after_actual_response_measurements(self):
        backends = [
            Backend("server1", 18861, estimated_response_ms=10.0),
            Backend("server2", 18861, estimated_response_ms=10.0),
        ]
        pool = BackendPool(backends, "LEAST_RESPONSE_TIME")

        first = await pool.select()
        self.assertIs(first, backends[0])
        await pool.record_response(first, 100.0)
        await pool.release(first)

        selected = await pool.select()

        self.assertIs(selected, backends[1])
        self.assertEqual(backends[0].response_samples, 1)
        self.assertAlmostEqual(backends[0].estimated_response_ms, 28.0)
        await pool.release(selected)

    async def test_lrt_rotates_exact_equal_scores(self):
        backends = [
            Backend("server1", 18861, estimated_response_ms=10.0),
            Backend("server2", 18861, estimated_response_ms=10.0),
            Backend("server3", 18861, estimated_response_ms=30.0),
        ]
        pool = BackendPool(backends, "LEAST_RESPONSE_TIME")

        selected = [await pool.select() for _ in range(2)]

        self.assertEqual(selected, backends[:2])
        for backend in selected:
            await pool.release(backend)


class RPyCProxyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend_server = ThreadedServer(
            IntegrationService,
            hostname="127.0.0.1",
            port=0,
        )
        self.backend_thread = threading.Thread(
            target=self.backend_server.start,
            daemon=True,
        )
        self.backend_thread.start()
        deadline = time.monotonic() + 5
        while not self.backend_server.active and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        self.assertTrue(self.backend_server.active)

        self.pool = BackendPool(
            [Backend("127.0.0.1", self.backend_server.port)],
            "LEAST_CONNECTIONS",
        )
        self.proxy_server = await asyncio.start_server(
            lambda reader, writer: handle_client(
                reader,
                writer,
                self.pool,
                5.0,
            ),
            "127.0.0.1",
            0,
        )
        self.proxy_port = self.proxy_server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.proxy_server.close()
        await self.proxy_server.wait_closed()
        self.backend_server.close()
        await asyncio.to_thread(self.backend_thread.join, 2)

    async def test_real_rpyc_calls_succeed_through_tcp_proxy(self):
        def make_calls():
            connection = rpyc.connect(
                "127.0.0.1",
                self.proxy_port,
                config={"sync_request_timeout": 5},
            )
            try:
                first = connection.root.add(19, 23)
                second = connection.root.add(1, 2)
                return first, second
            finally:
                connection.close()

        results = await asyncio.to_thread(make_calls)
        deadline = time.monotonic() + 2
        while (
            self.pool.backends[0].active
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(0.01)

        self.assertEqual(results, (42, 3))
        self.assertEqual(self.pool.backends[0].response_samples, 2)
        self.assertGreater(self.pool.backends[0].estimated_response_ms, 0)
        self.assertEqual(self.pool.active_counts()["127.0.0.1:%s" % self.backend_server.port], 0)

    async def test_real_rpyc_call_fails_over_when_first_backend_is_down(self):
        with socket.socket() as unused_socket:
            unused_socket.bind(("127.0.0.1", 0))
            unavailable_port = unused_socket.getsockname()[1]

        healthy_backend = Backend("127.0.0.1", self.backend_server.port)
        unavailable_backend = Backend("127.0.0.1", unavailable_port)
        self.pool = BackendPool(
            [unavailable_backend, healthy_backend],
            "LEAST_CONNECTIONS",
        )

        def make_call():
            connection = rpyc.connect(
                "127.0.0.1",
                self.proxy_port,
                config={"sync_request_timeout": 5},
            )
            try:
                return connection.root.add(19, 23)
            finally:
                connection.close()

        result = await asyncio.to_thread(make_call)
        deadline = time.monotonic() + 2
        while healthy_backend.active and time.monotonic() < deadline:
            await asyncio.sleep(0.01)

        self.assertEqual(result, 42)
        self.assertGreater(unavailable_backend.unavailable_until, time.monotonic())
        self.assertEqual(healthy_backend.response_samples, 1)
        self.assertEqual(self.pool.active_counts(), {
            unavailable_backend.name: 0,
            healthy_backend.name: 0,
        })


class IntegrationService(rpyc.Service):
    def exposed_add(self, left, right):
        return left + right


if __name__ == "__main__":
    unittest.main()
