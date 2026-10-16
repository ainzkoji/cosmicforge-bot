import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from websockets.exceptions import InvalidHandshake

from app.exchange.platform_adapter import stream_transport as T
from app.exchange.platform_adapter.user_stream import HEARTBEAT, StreamClosed


def test_denied_handshake_never_opens_socket(monkeypatch):
    connector = Mock()
    monkeypatch.setattr(T, "connect", connector)
    transport = T.WebSocketTransport(admit=lambda: False)
    with pytest.raises(StreamClosed, match="STREAM_CONNECTION_BUDGET_REFUSED"):
        transport.connect(SimpleNamespace(url="wss://example.test/private"))
    connector.assert_not_called()


def test_accounts_share_a_persistent_handshake_window(tmp_path):
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    from shared_lib.persistence.db import DB
    db = DB(str(tmp_path / "connections.db"))
    ensure_cati_platform_schema(db)
    now = [1_800_000_000_000]
    first = T.connection_admission(db, "bybit", "shared-egress", "one", lambda: now[0])
    second = T.connection_admission(db, "bybit", "shared-egress", "two", lambda: now[0])
    assert sum(first() for _ in range(500)) == 320  # 500 x 80% utilisation x 80% read ceiling
    assert second() is False
    now[0] += 300_000
    assert second() is True


def test_frames_pongs_timeout_and_shutdown(monkeypatch):
    socket = Mock()
    connector = Mock(return_value=socket)
    monkeypatch.setattr(T, "connect", connector)
    transport = T.WebSocketTransport()
    transport.connect(SimpleNamespace(url="wss://example.test/ws/private-secret"))
    transport.send({"op": "auth", "args": ["secret"]})
    assert json.loads(socket.send.call_args.args[0])["op"] == "auth"
    socket.recv.side_effect = [TimeoutError(), '{"event":"fill"}', b"gzip-frame"]
    assert transport.receive() is None
    assert transport.receive() == '{"event":"fill"}'
    assert transport.receive() == b"gzip-frame"
    socket.recv.assert_called_with(timeout=0)
    pong = threading.Event()
    socket.ping.return_value = pong
    transport.ping()
    transport.ping()
    assert socket.ping.call_count == 1
    pong.set()
    assert transport.receive() is HEARTBEAT
    transport.close()
    transport.close()
    assert socket.close.call_count == 1
    assert connector.call_args.kwargs["max_queue"] == 16
    assert connector.call_args.kwargs["logger"].disabled


def test_tls_and_errors_never_expose_stream_credentials(monkeypatch):
    transport = T.WebSocketTransport()
    with pytest.raises(StreamClosed, match="STREAM_TLS_REQUIRED"):
        transport.connect(SimpleNamespace(url="ws://example.test/secret"))
    monkeypatch.setattr(T, "connect", Mock(side_effect=InvalidHandshake("private-secret")))
    with pytest.raises(StreamClosed) as error:
        transport.connect(SimpleNamespace(url="wss://example.test/private-secret"))
    assert "private-secret" not in str(error.value)
    with pytest.raises(StreamClosed, match="STREAM_NOT_CONNECTED"):
        transport.receive()


def test_real_tls_websocket_roundtrip(tmp_path, monkeypatch):
    """Exercise actual TLS and WebSocket framing against a local server, no venue credentials."""
    import datetime
    import ssl
    import time
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from websockets.sync.client import connect as real_connect
    from websockets.sync.server import serve

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(key, hashes.SHA256()))
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()))
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(cert_path, key_path)
    client_tls = ssl.create_default_context(cafile=str(cert_path))
    monkeypatch.setattr(T, "connect", lambda url, **kw: real_connect(url, ssl=client_tls, proxy=None, **kw))
    def echo(socket):
        for message in socket:
            socket.send(message)
    with serve(echo, "127.0.0.1", 0, ssl=server_tls) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        transport = T.WebSocketTransport()
        try:
            port = server.socket.getsockname()[1]
            transport.connect(SimpleNamespace(url=f"wss://localhost:{port}/private-test"))
            transport.send({"op": "auth", "args": ["synthetic"]})
            deadline = time.monotonic() + 3
            received = None
            while received is None and time.monotonic() < deadline:
                received = transport.receive()
                time.sleep(0.005)
            assert json.loads(received) == {"op": "auth", "args": ["synthetic"]}
            transport.ping()
            heartbeat = None
            while heartbeat is None and time.monotonic() < deadline:
                heartbeat = transport.receive()
                time.sleep(0.005)
            assert heartbeat is HEARTBEAT
        finally:
            transport.close()
            server.shutdown()
            thread.join(timeout=3)
        assert not thread.is_alive()
