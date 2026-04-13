import threading
import time
from types import SimpleNamespace

import pytest

from oxia.internal.sessions import Session, SessionManager, _resolve_heartbeat_interval_ms


class FakeStub:
    def __init__(self):
        self.created = []
        self.keepalives = []
        self.closed = []
        self.fail_keepalive = False
        self._next_session_id = 1

    def create_session(self, request):
        session_id = self._next_session_id
        self._next_session_id += 1
        self.created.append((request.shard, request.session_timeout_ms, request.client_identity, session_id))
        return SimpleNamespace(session_id=session_id)

    def keep_alive(self, request):
        self.keepalives.append((request.shard, request.session_id))
        if self.fail_keepalive:
            raise RuntimeError("keepalive failed")
        return SimpleNamespace()

    def close_session(self, request):
        self.closed.append((request.shard, request.session_id))
        return SimpleNamespace()


class FakeDiscovery:
    def __init__(self, stub):
        self._stub = stub

    def get_stub(self, shard):
        return self._stub


def wait_until(predicate, timeout=1.0, interval=0.01):
    deadline = time.monotonic() + timeout
    waiter = threading.Event()
    while time.monotonic() < deadline:
        if predicate():
            return
        waiter.wait(interval)

    assert predicate(), "timed out waiting for condition"


def test_session_sends_keepalives():
    stub = FakeStub()
    session = Session(
        7,
        "client-1",
        FakeDiscovery(stub),
        session_timeout_ms=120,
        heartbeat_interval_ms=20,
    )

    try:
        wait_until(lambda: len(stub.keepalives) >= 2)
        assert stub.created == [(7, 120, "client-1", session.session_id())]
        assert stub.keepalives[0] == (7, session.session_id())
        assert not session.is_closed()
    finally:
        session.close()

    assert stub.closed[-1] == (7, session.session_id())


def test_small_session_timeout_uses_capped_default_heartbeat_interval():
    stub = FakeStub()
    session = Session(7, "client-1", FakeDiscovery(stub), session_timeout_ms=120)

    try:
        assert session._heartbeat_interval_ms == 60
        wait_until(lambda: len(stub.keepalives) >= 1, timeout=0.5)
        assert not session.is_closed()
    finally:
        session.close()


def test_rejects_invalid_session_timing_configuration():
    with pytest.raises(ValueError, match="session_timeout_ms must be at least 2"):
        _resolve_heartbeat_interval_ms(1, None)

    with pytest.raises(ValueError, match="heartbeat_interval_ms must be greater than zero"):
        _resolve_heartbeat_interval_ms(10, 0)

    with pytest.raises(ValueError, match="heartbeat_interval_ms must be smaller than session_timeout_ms"):
        _resolve_heartbeat_interval_ms(10, 10)


def test_session_manager_recreates_closed_sessions():
    stub = FakeStub()
    manager = SessionManager(
        FakeDiscovery(stub),
        session_timeout_ms=120,
        client_identifier="client-1",
        heartbeat_interval_ms=20,
    )

    try:
        session_1 = manager.get_session(7)
        session_1.close()

        session_2 = manager.get_session(7)
        assert session_2.session_id() != session_1.session_id()

        stub.fail_keepalive = True
        wait_until(session_2.is_closed)
        wait_until(lambda: 7 not in manager.sessions_by_shard)

        session_3 = manager.get_session(7)
        assert session_3.session_id() != session_2.session_id()
    finally:
        manager.close()
