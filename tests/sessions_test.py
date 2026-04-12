import time
from types import SimpleNamespace

from oxia.internal.sessions import Session, SessionManager


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


def test_session_sends_keepalives():
    stub = FakeStub()
    session = Session(7, "client-1", FakeDiscovery(stub), session_timeout_ms=2_200)

    try:
        time.sleep(4.5)
        assert stub.created == [(7, 2_200, "client-1", session.session_id())]
        assert len(stub.keepalives) >= 2
        assert stub.keepalives[0] == (7, session.session_id())
        assert not session.is_closed()
    finally:
        session.close()

    assert stub.closed[-1] == (7, session.session_id())


def test_session_manager_recreates_closed_sessions():
    stub = FakeStub()
    manager = SessionManager(FakeDiscovery(stub), session_timeout_ms=2_200, client_identifier="client-1")

    try:
        session_1 = manager.get_session(7)
        session_1.close()

        session_2 = manager.get_session(7)
        assert session_2.session_id() != session_1.session_id()

        stub.fail_keepalive = True
        time.sleep(4.8)
        assert session_2.is_closed()
        assert 7 not in manager.sessions_by_shard

        session_3 = manager.get_session(7)
        assert session_3.session_id() != session_2.session_id()
    finally:
        manager.close()
