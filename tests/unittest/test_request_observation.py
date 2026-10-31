"""Real loopback regressions for request observation and native callback ownership."""

import asyncio
from contextlib import closing
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from curl_cffi import Curl, CurlError, CurlOpt, SendState
from curl_cffi.requests import AsyncSession, Session
from curl_cffi.requests.exceptions import RequestException


@pytest.fixture
def observation_server():
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            if self.path == "/disconnect":
                self.close_connection = True
                return
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/disconnect")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


def test_public_prereq_binding_and_reset(observation_server):
    calls = []
    with closing(Curl()) as curl:
        curl.setopt(CurlOpt.URL, observation_server)
        curl.setopt(CurlOpt.WRITEFUNCTION, lambda data: len(data))
        curl.setopt(CurlOpt.PREREQFUNCTION, lambda *args: calls.append(args) or 0)
        curl.perform()
        assert len(calls) == 1
        assert calls[0][0] == b"127.0.0.1"
        assert curl.send_state is SendState.POSSIBLY_SENT
        curl.reset()
        assert curl.send_state is SendState.NOT_STARTED


def test_error_snapshot_survives_reset_and_redirect(observation_server):
    with Session(trust_env=False) as session:
        for path in ("/disconnect", "/redirect"):
            with pytest.raises(RequestException) as caught:
                session.get(observation_server + path)
            assert caught.value.send_state is SendState.POSSIBLY_SENT
            assert caught.value.__cause__.send_state is SendState.POSSIBLY_SENT
            assert session.curl.send_state is SendState.NOT_STARTED
        assert session.get(observation_server).status_code == 200


def test_tcp_failure_is_before_target_request():
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        with Session(trust_env=False) as session:
            with pytest.raises(RequestException) as caught:
                session.get(f"http://127.0.0.1:{port}", timeout=0.2)
            assert caught.value.send_state is SendState.NOT_STARTED


def test_native_prereq_abort_is_supported(observation_server):
    with closing(Curl()) as curl:
        curl.setopt(CurlOpt.URL, observation_server)
        curl.setopt(CurlOpt.PREREQFUNCTION, lambda *args: 1)
        with pytest.raises(CurlError) as caught:
            curl.perform()
        assert caught.value.code == 42


@pytest.mark.asyncio
async def test_concurrent_observation_isolation(observation_server):
    async with AsyncSession(trust_env=False, max_clients=2) as session:
        responses = await asyncio.gather(
            session.get(observation_server),
            session.get(observation_server + "/disconnect"),
            return_exceptions=True,
        )
        assert responses[0].status_code == 200
        assert responses[1].send_state is SendState.POSSIBLY_SENT
        assert (await session.get(observation_server)).status_code == 200


def test_debug_cancel_callback_lifetime_subprocess():
    # A dangling CFFI handle aborts the interpreter, so isolate the regression.
    script = r"""
import asyncio
import resource
from curl_cffi import CurlOpt
from curl_cffi.requests import AsyncSession
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
async def main():
    received = asyncio.Event()
    workers = []
    async def handle(reader, writer):
        workers.append(asyncio.current_task())
        try:
            await reader.readuntil(b'\r\n\r\n')
            received.set()
            await reader.read()
        finally:
            writer.close()
    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    for stream in (False, True):
        received.clear()
        async with AsyncSession(trust_env=False, curl_options={
            CurlOpt.VERBOSE: 1, CurlOpt.DEBUGFUNCTION: lambda *args: None,
        }) as session:
            task = asyncio.create_task(session.get(
                f'http://127.0.0.1:{port}/', stream=stream, timeout=3))
            await asyncio.wait_for(received.wait(), 1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    server.close()
    await server.wait_closed()
    await asyncio.gather(*workers)
    print('cancelled cleanly')
asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, timeout=10
    )
    assert result.returncode == 0, result.stderr.decode()
    assert b"cancelled cleanly" in result.stdout


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [False, True])
async def test_local_dns_failure_before_send(answer):
    class DNS(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            if answer:
                self.transport.sendto(
                    data[:2] + b"\x81\x83" + data[4:6] + b"\0" * 6 + data[12:], addr
                )

    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        DNS, local_addr=("127.0.0.1", 0)
    )
    port = transport.get_extra_info("sockname")[1]
    try:
        async with AsyncSession(trust_env=False) as session:
            with pytest.raises(RequestException) as caught:
                await session.get(
                    "http://observation.test/", timeout=0.08, dns=f"127.0.0.1:{port}"
                )
            assert caught.value.send_state is SendState.NOT_STARTED
            assert caught.value.code == (6 if answer else 28)
    finally:
        transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["tls", "proxy-stall", "proxy-deny", "proxy-tls"])
async def test_tls_and_proxy_connect_failure_before_target_send(mode):
    writers = []
    received = []

    async def handle(reader, writer):
        writers.append(writer)
        try:
            if mode == "tls":
                received.append(await reader.read(4096))
            else:
                received.append(await reader.readuntil(b"\r\n\r\n"))
                if mode == "proxy-deny":
                    writer.write(
                        b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                        b"Content-Length: 0\r\n\r\n"
                    )
                    await writer.drain()
                    return
                if mode == "proxy-tls":
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await writer.drain()
                    await reader.read(4096)
            await reader.read()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        options = {} if mode == "tls" else {"proxy": f"http://127.0.0.1:{port}"}
        url = f"https://127.0.0.1:{port}/" if mode == "tls" else "https://target.test/"
        async with AsyncSession(trust_env=False, verify=False) as session:
            with pytest.raises(RequestException) as caught:
                await session.get(url, timeout=0.08, **options)
            assert caught.value.send_state is SendState.NOT_STARTED
        assert received
        if mode != "tls":
            assert received[0].startswith(b"CONNECT target.test:443 ")
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_http2_reuse_preserves_target_observation():
    import ssl

    import h2.config
    import h2.connection
    import h2.events
    import trustme

    from curl_cffi import CurlHttpVersion

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    trustme.CA().issue_cert("localhost").configure_cert(context)
    context.set_alpn_protocols(["h2"])
    writers = []
    requests = []

    async def handle(reader, writer):
        writers.append(writer)
        connection = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False, header_encoding="utf8")
        )
        connection.initiate_connection()
        writer.write(connection.data_to_send())
        try:
            while data := await reader.read(65536):
                for event in connection.receive_data(data):
                    if isinstance(event, h2.events.RequestReceived):
                        path = dict(event.headers)[":path"]
                        requests.append(path)
                        if path == "/ok":
                            connection.send_headers(
                                event.stream_id,
                                [(":status", "200"), ("content-length", "2")],
                            )
                            connection.send_data(
                                event.stream_id, b"ok", end_stream=True
                            )
                writer.write(connection.data_to_send())
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context)
    url = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        async with AsyncSession(
            trust_env=False, verify=False, http_version=CurlHttpVersion.V2_0
        ) as session:
            assert (await session.get(url + "/ok")).content == b"ok"
            with pytest.raises(RequestException) as caught:
                await session.get(url + "/wait", timeout=0.06)
            assert caught.value.send_state is SendState.POSSIBLY_SENT
        assert requests == ["/ok", "/wait"]
        assert len(writers) == 1
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
            await writer.wait_closed()


def test_connect_to_public_slist_keeps_logical_target(observation_server):
    port = observation_server.rsplit(":", 1)[1]
    with Session(
        trust_env=False,
        curl_options={CurlOpt.CONNECT_TO: [f"unresolved.invalid:80:127.0.0.1:{port}"]},
    ) as session:
        for _ in range(2):
            rsp = session.get("http://unresolved.invalid/")
            assert rsp.content == b"ok"
            assert rsp.url == "http://unresolved.invalid/"
