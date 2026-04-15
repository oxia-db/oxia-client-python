# Copyright 2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
import time
from types import SimpleNamespace

import pytest

from oxia.internal.sessions import (
    Session,
    SessionManager,
    _resolve_heartbeat_interval_ms,
)


class FakeStub:
    def __init__(self):
        self._lock = threading.Lock()
        self.created = []
        self.keepalives = []
        self.closed = []
        self.fail_keepalive = False
        self._next_session_id = 1

    def create_session(self, request):
        with self._lock:
            session_id = self._next_session_id
            self._next_session_id += 1
            self.created.append(
                (request.shard, request.session_timeout_ms,
                 request.client_identity, session_id))
        return SimpleNamespace(session_id=session_id)

    def keep_alive(self, request):
        with self._lock:
            self.keepalives.append((request.shard, request.session_id))
            fail = self.fail_keepalive
        if fail:
            raise RuntimeError("keepalive failed")
        return SimpleNamespace()

    def close_session(self, request):
        with self._lock:
            self.closed.append((request.shard, request.session_id))
        return SimpleNamespace()


class FakeDiscovery:
    def __init__(self, stub):
        self._stub = stub

    def get_stub(self, shard):
        return self._stub


def wait_until(predicate, timeout=2.0, interval=0.005):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    assert predicate(), "timed out waiting for condition"


def test_resolve_heartbeat_interval_default_aims_for_tenth_of_timeout():
    assert _resolve_heartbeat_interval_ms(30_000, None) == 3_000
    assert _resolve_heartbeat_interval_ms(60_000, None) == 6_000


def test_resolve_heartbeat_interval_default_is_floored_at_two_seconds():
    # timeout // 10 = 1000, below the 2s floor, so we get 2000
    assert _resolve_heartbeat_interval_ms(10_000, None) == 2_000


def test_resolve_heartbeat_interval_caps_below_session_timeout():
    # At the floor boundary, the default (2000) collides with the timeout and
    # must be clamped strictly below it.
    assert _resolve_heartbeat_interval_ms(2_000, None) == 1_999


def test_resolve_heartbeat_interval_accepts_explicit_values():
    assert _resolve_heartbeat_interval_ms(1_000, 100) == 100
    assert _resolve_heartbeat_interval_ms(30_000, 500) == 500


def test_resolve_heartbeat_interval_allows_small_timeouts_with_explicit_interval():
    # The 2000ms default floor only applies to the default path. When the
    # caller provides an explicit interval the floor must not get in the way,
    # so that tests and advanced users can drive tight timing deterministically.
    assert _resolve_heartbeat_interval_ms(100, 10) == 10
    assert _resolve_heartbeat_interval_ms(500, 50) == 50
    assert _resolve_heartbeat_interval_ms(1_000, 999) == 999


def test_resolve_heartbeat_interval_rejects_invalid_inputs():
    # Too-small timeout is only rejected when we're asked to pick the default.
    with pytest.raises(ValueError, match=r"session_timeout_ms must be at least 2000ms"):
        _resolve_heartbeat_interval_ms(1_999, None)
    with pytest.raises(ValueError, match=r"session_timeout_ms must be at least 2000ms"):
        _resolve_heartbeat_interval_ms(1, None)
    with pytest.raises(ValueError, match="heartbeat_interval_ms must be greater than zero"):
        _resolve_heartbeat_interval_ms(100, 0)
    with pytest.raises(ValueError, match="heartbeat_interval_ms must be greater than zero"):
        _resolve_heartbeat_interval_ms(100, -1)
    with pytest.raises(ValueError, match="heartbeat_interval_ms must be smaller than session_timeout_ms"):
        _resolve_heartbeat_interval_ms(100, 100)
    with pytest.raises(ValueError, match="heartbeat_interval_ms must be smaller than session_timeout_ms"):
        _resolve_heartbeat_interval_ms(100, 150)


def test_session_sends_keepalives():
    stub = FakeStub()
    session = Session(
        7,
        "client-1",
        FakeDiscovery(stub),
        session_timeout_ms=1_000,
        heartbeat_interval_ms=20,
    )

    try:
        wait_until(lambda: len(stub.keepalives) >= 3)
        assert stub.created == [(7, 1_000, "client-1", session.session_id())]
        for shard, session_id in stub.keepalives:
            assert shard == 7
            assert session_id == session.session_id()
        assert not session.is_closed()
    finally:
        session.close()

    assert stub.closed[-1] == (7, session.session_id())
    assert session.is_closed()


def test_session_close_is_idempotent():
    stub = FakeStub()
    session = Session(
        7, "client-1", FakeDiscovery(stub),
        session_timeout_ms=1_000, heartbeat_interval_ms=20,
    )

    session.close()
    session.close()
    session.close()

    assert len(stub.closed) == 1


def test_session_self_expires_when_keepalives_keep_failing():
    stub = FakeStub()
    stub.fail_keepalive = True

    on_close_hits = []

    def on_close(s):
        on_close_hits.append(s)

    session = Session(
        7, "client-1", FakeDiscovery(stub),
        session_timeout_ms=100,
        heartbeat_interval_ms=10,
        on_close=on_close,
    )

    wait_until(session.is_closed, timeout=3.0)
    assert on_close_hits == [session]
    # Local expiry still triggers the close_session RPC
    assert stub.closed == [(7, session.session_id())]


def test_session_manager_recreates_closed_sessions():
    stub = FakeStub()
    manager = SessionManager(
        FakeDiscovery(stub),
        session_timeout_ms=1_000,
        client_identifier="client-1",
        heartbeat_interval_ms=20,
    )

    try:
        session_1 = manager.get_session(7)
        session_1.close()

        session_2 = manager.get_session(7)
        assert session_2 is not session_1
        assert session_2.session_id() != session_1.session_id()
    finally:
        manager.close()


def test_session_manager_recreates_after_keepalive_driven_expiry():
    stub = FakeStub()
    stub.fail_keepalive = True
    manager = SessionManager(
        FakeDiscovery(stub),
        session_timeout_ms=100,
        client_identifier="client-1",
        heartbeat_interval_ms=10,
    )

    try:
        session_1 = manager.get_session(7)
        wait_until(session_1.is_closed, timeout=3.0)
        wait_until(lambda: 7 not in manager.sessions_by_shard, timeout=1.0)

        stub.fail_keepalive = False
        session_2 = manager.get_session(7)
        assert session_2.session_id() != session_1.session_id()
    finally:
        manager.close()


def test_session_manager_late_on_close_does_not_evict_replacement():
    """Race fix: a late on_close from an old session must not evict a replacement."""
    stub = FakeStub()
    manager = SessionManager(
        FakeDiscovery(stub),
        session_timeout_ms=1_000,
        client_identifier="client-1",
        heartbeat_interval_ms=20,
    )

    try:
        session_1 = manager.get_session(7)
        session_1.close()  # Removes itself from the manager

        session_2 = manager.get_session(7)  # New session installed under the same key
        assert session_2 is not session_1

        # Simulate a stale callback from session_1 arriving after session_2 took its place.
        manager.on_session_closed(session_1)

        assert manager.sessions_by_shard.get(7) is session_2
    finally:
        manager.close()


def test_session_manager_close_shuts_down_all_sessions():
    stub = FakeStub()
    manager = SessionManager(
        FakeDiscovery(stub),
        session_timeout_ms=1_000,
        client_identifier="client-1",
        heartbeat_interval_ms=20,
    )

    s1 = manager.get_session(3)
    s2 = manager.get_session(5)

    manager.close()

    assert s1.is_closed()
    assert s2.is_closed()
    assert {entry[0] for entry in stub.closed} == {3, 5}
    assert manager.sessions_by_shard == {}
