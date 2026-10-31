"""Retry decisions and budgets tested against controlled real network failures."""

import asyncio
import socket
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from curl_cffi.requests import AsyncSession, RetryStrategy, SendState, Session
from curl_cffi.requests.exceptions import HTTPError, RequestException, Timeout
import contextlib


@pytest.fixture
def retry_server():
    counts = {}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            counts[self.path] = counts.get(self.path, 0) + 1
            if self.path == "/drop":
                self.close_connection = True
                return
            if self.path == "/slow":
                time.sleep(0.3)
            self.send_response(503 if self.path == "/status" else 200)
            if self.path.startswith("/stream"):
                self.send_header("Transfer-Encoding", "chunked")
                if self.path == "/stream-compressed":
                    self.send_header("Content-Encoding", "gzip")
                self.end_headers()
                if self.path == "/stream-delayed":
                    time.sleep(0.15)
                try:
                    if self.path == "/stream-compressed":
                        compressor = zlib.compressobj(wbits=31)
                        first = compressor.compress(b":x\n") + compressor.flush(
                            zlib.Z_SYNC_FLUSH
                        )
                        self.wfile.write(
                            f"{len(first):x}\r\n".encode() + first + b"\r\n"
                        )
                        self.wfile.flush()
                        for _ in range(8):
                            # Empty, byte-aligned deflate blocks produce no decoded
                            # output, but are genuine response-body network ingress.
                            self.wfile.write(b"5\r\n\x00\x00\x00\xff\xff\r\n")
                            self.wfile.flush()
                            time.sleep(0.04)
                        last = compressor.flush(zlib.Z_FINISH)
                        self.wfile.write(
                            f"{len(last):x}\r\n".encode() + last + b"\r\n0\r\n\r\n"
                        )
                        self.wfile.flush()
                        return
                    for _ in range(8 if self.path == "/stream-heartbeat" else 1):
                        self.wfile.write(b"3\r\n:x\n\r\n")
                        self.wfile.flush()
                        time.sleep(0.04)
                    time.sleep(0.3)
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            self.send_header("Content-Length", "2")
            self.end_headers()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(b"ok")

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            counts.setdefault("uploaded", []).append(body)
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if len(counts["uploaded"]) == 1:
                self.wfile.write(body[:1])
                self.close_connection = True
            else:
                self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", counts
    server.shutdown()
    server.server_close()
    thread.join()


def test_before_send_retry_count_and_metadata():
    attempts = []
    with socket.socket() as sock, Session(trust_env=False) as session:
        sock.bind(("127.0.0.1", 0))
        with pytest.raises(RequestException) as caught:
            session.get(
                f"http://127.0.0.1:{sock.getsockname()[1]}",
                retry=RetryStrategy(count=2),
                on_attempt=attempts.append,
            )
    error = caught.value
    assert error.retry_info.attempts == 3
    assert error.retry_info.reason == "attempts_exhausted"
    assert error.send_state is SendState.NOT_STARTED
    assert error.__cause__.code == 7
    assert [a.attempt for a in attempts] == [1, 2, 3]
    assert all(a.error is not None and a.response is None for a in attempts)


@pytest.mark.parametrize("allow_replay,expected", [(False, 1), (True, 3)])
def test_sent_request_requires_replay_permission(retry_server, allow_replay, expected):
    url, counts = retry_server
    attempts = []
    with (
        Session(trust_env=False) as session,
        pytest.raises(RequestException) as caught,
    ):
        session.get(
            url + "/drop",
            retry=2,
            allow_replay=allow_replay,
            on_attempt=attempts.append,
        )
    assert len(attempts) == expected
    assert caught.value.retry_info.reason == (
        "attempts_exhausted" if allow_replay else "replay_forbidden"
    )
    # libcurl is allowed to internally recover independently of Python attempts.
    assert counts["/drop"] >= expected


@pytest.mark.parametrize("check_status", [False, True])
def test_http_status_is_never_retried(retry_server, check_status):
    url, counts = retry_server
    with Session(trust_env=False, retry=4, raise_for_status=check_status) as session:
        if check_status:
            with pytest.raises(HTTPError) as caught:
                session.get(url + "/status", allow_replay=True)
            assert caught.value.retry_info.reason == "deterministic_error"
        else:
            assert session.get(url + "/status").status_code == 503
    assert counts["/status"] == 1


def test_invalid_url_is_not_retried():
    attempts = []
    with (
        Session(trust_env=False, retry=3) as session,
        pytest.raises(RequestException) as caught,
    ):
        session.get("http://[broken", on_attempt=attempts.append)
    assert len(attempts) == 1
    assert caught.value.retry_info.reason == "deterministic_error"


@pytest.mark.asyncio
async def test_budget_covers_pool_wait_without_network_attempt(retry_server):
    url, counts = retry_server
    async with AsyncSession(max_clients=1, trust_env=False) as session:
        stream = await session.get(url + "/stream", stream=True)
        started = time.monotonic()
        with pytest.raises(Timeout) as caught:
            await session.get(
                url + "/ok",
                retry=RetryStrategy(count=3, total_timeout=0.07, attempt_timeout=1),
            )
        assert 0.05 <= time.monotonic() - started < 0.3
        assert caught.value.retry_info.reason == "budget_exhausted"
        assert caught.value.retry_info.attempts == 0
        assert caught.value.send_state is SendState.NOT_STARTED
        assert "/ok" not in counts
        await stream.aclose()
        assert (await session.get(url + "/ok")).status_code == 200


@pytest.mark.asyncio
async def test_total_budget_includes_all_attempts_and_backoff(retry_server):
    url, _ = retry_server
    async with AsyncSession(trust_env=False) as session:
        start = time.monotonic()
        with pytest.raises(Timeout) as caught:
            await session.get(
                url + "/slow",
                allow_replay=True,
                retry=RetryStrategy(
                    count=10, total_timeout=0.15, attempt_timeout=0.055, delay=0.01
                ),
            )
        assert time.monotonic() - start < 0.3
        assert caught.value.retry_info.reason == "budget_exhausted"
        assert 1 <= caught.value.retry_info.attempts <= 3


@pytest.mark.asyncio
async def test_request_override_does_not_mutate_shared_session(retry_server):
    url, _ = retry_server
    base = RetryStrategy(count=0)
    events = [[], []]
    async with AsyncSession(trust_env=False, retry=base) as session:
        await asyncio.gather(
            *[
                session.get(
                    url + "/drop",
                    retry=RetryStrategy(count=count),
                    allow_replay=True,
                    on_attempt=events[index].append,
                )
                for index, count in enumerate((0, 2))
            ],
            return_exceptions=True,
        )
        assert session.retry is base
    assert [len(e) for e in events] == [1, 3]


@pytest.mark.asyncio
async def test_open_stream_outlives_establishment_budget_and_heartbeats(retry_server):
    url, _ = retry_server
    async with AsyncSession(trust_env=False) as session:
        rsp = await session.get(
            url + "/stream-heartbeat",
            stream=True,
            retry=RetryStrategy(total_timeout=0.06),
            idle_timeout=0.09,
        )
        # Consumer is paused, but network heartbeats keep reception alive.
        await asyncio.sleep(0.24)
        assert not rsp.astream_task.done()
        await rsp.aclose()
        assert rsp.astream_task.done()


@pytest.mark.asyncio
async def test_idle_timeout_measures_receiver_while_consumer_paused(retry_server):
    url, _ = retry_server
    async with AsyncSession(trust_env=False) as session:
        rsp = await session.get(url + "/stream", stream=True, idle_timeout=0.07)
        await asyncio.sleep(0.15)
        assert rsp.astream_task.done()
        with pytest.raises(Timeout, match="idle timeout"):
            await rsp.acontent()
        assert (await session.get(url + "/ok")).status_code == 200


def test_sync_stream_budget_and_idle_close(retry_server):
    url, _ = retry_server
    with Session(trust_env=False) as session:
        rsp = session.get(
            url + "/stream-heartbeat",
            stream=True,
            retry=RetryStrategy(total_timeout=0.06),
            idle_timeout=0.08,
        )
        time.sleep(0.2)
        assert not rsp.stream_task.done()
        start = time.monotonic()
        rsp.close()
        assert time.monotonic() - start < 0.2
        rsp = session.get(url + "/stream", stream=True, idle_timeout=0.06)
        time.sleep(0.14)
        with pytest.raises(Timeout, match="idle timeout"):
            list(rsp.iter_content())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_final_headers_open_stream_before_delayed_body(retry_server, asynchronous):
    url, counts = retry_server
    policy = RetryStrategy(count=2, attempt_timeout=0.06, total_timeout=0.08)

    async def run():
        async with AsyncSession(trust_env=False) as session:
            start = time.monotonic()
            rsp = await session.get(
                url + "/stream-delayed", stream=True, retry=policy, idle_timeout=0.5
            )
            assert time.monotonic() - start < 0.1
            content = rsp.aiter_content()
            assert await anext(content) == b":x\n"
            await rsp.aclose()

    if asynchronous:
        asyncio.run(run())
    else:
        with Session(trust_env=False) as session:
            start = time.monotonic()
            rsp = session.get(
                url + "/stream-delayed", stream=True, retry=policy, idle_timeout=0.5
            )
            assert time.monotonic() - start < 0.1
            assert next(rsp.iter_content()) == b":x\n"
            rsp.close()
    assert counts["/stream-delayed"] == 1


@pytest.mark.asyncio
async def test_header_only_stream_starts_idle_timer(retry_server):
    url, counts = retry_server
    async with AsyncSession(trust_env=False) as session:
        rsp = await session.get(
            url + "/stream-delayed",
            stream=True,
            retry=RetryStrategy(count=3, attempt_timeout=0.03),
            idle_timeout=0.06,
        )
        with pytest.raises(Timeout, match="idle timeout"):
            await rsp.acontent()
    assert counts["/stream-delayed"] == 1


@pytest.mark.asyncio
async def test_compressed_ingress_refreshes_idle_without_decoded_chunks(retry_server):
    url, _ = retry_server
    async with AsyncSession(trust_env=False) as session:
        rsp = await session.get(
            url + "/stream-compressed", stream=True, idle_timeout=0.09
        )
        iterator = rsp.aiter_content()
        assert await anext(iterator) == b":x\n"
        await asyncio.sleep(0.24)
        assert not rsp.astream_task.done()
        assert rsp.queue.empty()
        assert b"".join([chunk async for chunk in iterator]) == b""


@pytest.mark.parametrize("asynchronous", [False, True])
def test_nonrewindable_body_forbids_additional_attempt(asynchronous):
    async def run(port):
        async def content():
            yield b"operation"

        async with AsyncSession(trust_env=False, retry=3) as session:
            with pytest.raises(RequestException) as caught:
                await session.post(f"http://127.0.0.1:{port}", content=content())
            assert caught.value.retry_info.reason == "body_not_replayable"
            assert caught.value.retry_info.attempts == 1
            assert caught.value.__cause__.code == 7

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        if asynchronous:
            asyncio.run(run(port))
        else:
            with Session(trust_env=False, retry=3) as session:
                with pytest.raises(RequestException) as caught:
                    session.post(
                        f"http://127.0.0.1:{port}", content=iter([b"operation"])
                    )
                assert caught.value.retry_info.reason == "body_not_replayable"
                assert caught.value.retry_info.attempts == 1
                assert caught.value.__cause__.code == 7


def test_attempt_observer_failure_does_not_change_result(retry_server):
    url, _ = retry_server

    def observer(info):
        raise ValueError("broken log sink")

    with (
        Session(trust_env=False, on_attempt=observer) as session,
        pytest.warns(UserWarning, match="on_attempt callback failed"),
    ):
        assert session.get(url + "/ok").status_code == 200


@pytest.mark.parametrize("asynchronous", [False, True])
def test_seekable_upload_rewinds_after_partial_network_response(
    retry_server, asynchronous
):
    from io import BytesIO

    url, counts = retry_server

    async def run():
        async with AsyncSession(trust_env=False, retry=1) as session:
            rsp = await session.post(
                url + "/upload", content=BytesIO(b"operation"), allow_replay=True
            )
            assert rsp.content == b"operation"

    if asynchronous:
        asyncio.run(run())
    else:
        with Session(trust_env=False, retry=1) as session:
            rsp = session.post(
                url + "/upload", content=BytesIO(b"operation"), allow_replay=True
            )
            assert rsp.content == b"operation"
    assert counts["uploaded"] == [b"operation", b"operation"]
