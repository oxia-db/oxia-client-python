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

import logging
import threading
import time
import uuid
from typing import Callable, Optional

from oxia.internal.service_discovery import ServiceDiscovery
from oxia.internal.proto.io.streamnative.oxia import proto as pb

log = logging.getLogger(__name__)

# Default floor for the heartbeat cadence when the caller does not provide one.
# Matches the Oxia Java client's default (oxia-client-java Session.java).
_DEFAULT_HEARTBEAT_FLOOR_MS = 2_000


def _resolve_heartbeat_interval_ms(session_timeout_ms: int,
                                   heartbeat_interval_ms: Optional[int]) -> int:
    """Pick a safe heartbeat cadence for the given session timeout.

    The returned interval is always strictly smaller than ``session_timeout_ms``
    so that the keepalive loop always gets a chance to run before the local
    expiry check fires.
    """
    if heartbeat_interval_ms is not None:
        if heartbeat_interval_ms <= 0:
            raise ValueError("heartbeat_interval_ms must be greater than zero")
        if heartbeat_interval_ms >= session_timeout_ms:
            raise ValueError(
                "heartbeat_interval_ms must be smaller than session_timeout_ms")
        return heartbeat_interval_ms

    # The default cadence uses a 2s floor, so the session timeout has to be at
    # least that long for the default to fit at all. Callers who want tighter
    # timeouts can still get them by passing an explicit heartbeat_interval_ms.
    if session_timeout_ms < _DEFAULT_HEARTBEAT_FLOOR_MS:
        raise ValueError(
            f"session_timeout_ms must be at least {_DEFAULT_HEARTBEAT_FLOOR_MS}ms "
            f"when heartbeat_interval_ms is not provided (got {session_timeout_ms})")

    # Aim for roughly 10 heartbeats per timeout window, floored so we never
    # hammer the server on very large timeouts, and capped strictly below the
    # session timeout to cover the edge case where the floor meets the timeout.
    default = max(session_timeout_ms // 10, _DEFAULT_HEARTBEAT_FLOOR_MS)
    return min(default, session_timeout_ms - 1)


class Session:
    def __init__(self, shard: int, client_identifier: str,
                 service_discovery: ServiceDiscovery,
                 session_timeout_ms: int,
                 on_close: Optional[Callable[["Session"], None]] = None,
                 heartbeat_interval_ms: Optional[int] = None):
        self._shard = shard
        self._client_identifier = client_identifier
        self._service_discovery = service_discovery
        self._on_close = on_close
        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = _resolve_heartbeat_interval_ms(
            session_timeout_ms, heartbeat_interval_ms)
        self._closed = False
        self._close_lock = threading.Lock()
        self._stop_event = threading.Event()

        res = service_discovery.get_stub(shard).create_session(pb.CreateSessionRequest(
            shard=shard,
            session_timeout_ms=session_timeout_ms,
            client_identity=client_identifier,
        ))
        self._session_id = res.session_id
        self._last_successful_response = time.monotonic()

        self._heartbeat_thread = threading.Thread(
            target=self._run_keepalive_loop,
            daemon=True,
            name=f"oxia-session-{shard}",
        )
        self._heartbeat_thread.start()

    def shard_id(self):
        return self._shard

    def session_id(self):
        return self._session_id

    def client_identifier(self):
        return self._client_identifier

    def is_closed(self):
        return self._closed

    def _run_keepalive_loop(self):
        interval_s = self._heartbeat_interval_ms / 1000.0
        while not self._stop_event.wait(interval_s):
            if self._closed:
                return

            elapsed_ms = (time.monotonic() - self._last_successful_response) * 1000
            if elapsed_ms > self._session_timeout_ms:
                log.warning(
                    "Oxia session expired locally due to missing keepalive responses",
                    extra={"shard": self._shard, "session_id": self._session_id},
                )
                self.close()
                return

            try:
                self._service_discovery.get_stub(self._shard).keep_alive(pb.SessionHeartbeat(
                    shard=self._shard,
                    session_id=self._session_id,
                ))
                self._last_successful_response = time.monotonic()
            except Exception:
                log.warning(
                    "Failed to send Oxia session keepalive",
                    exc_info=True,
                    extra={"shard": self._shard, "session_id": self._session_id},
                )

    def close(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._stop_event.set()

        # Notify the manager before the (potentially slow) close_session RPC
        # so that concurrent get_session() callers don't race with us and so
        # that a late callback can never evict a replacement session.
        if self._on_close is not None:
            try:
                self._on_close(self)
            except Exception:
                log.warning(
                    "Oxia session on_close callback raised",
                    exc_info=True,
                    extra={"shard": self._shard, "session_id": self._session_id},
                )

        try:
            self._service_discovery.get_stub(self._shard).close_session(pb.CloseSessionRequest(
                shard=self._shard,
                session_id=self._session_id,
            ))
        except Exception:
            log.warning(
                "Failed to close Oxia session cleanly",
                exc_info=True,
                extra={"shard": self._shard, "session_id": self._session_id},
            )

        if threading.current_thread() is not self._heartbeat_thread:
            self._heartbeat_thread.join(
                timeout=self._heartbeat_interval_ms / 1000.0 + 1.0)


class SessionManager:
    def __init__(self, service_discovery: ServiceDiscovery, session_timeout_ms: int,
                 client_identifier: str,
                 heartbeat_interval_ms: Optional[int] = None):
        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._client_identifier = client_identifier or uuid.uuid4().hex
        self._service_discovery = service_discovery
        self.sessions_by_shard = {}
        self.lock = threading.Lock()

    def get_session(self, shard: int) -> Session:
        with self.lock:
            s = self.sessions_by_shard.get(shard)
            if s is None or s.is_closed():
                s = Session(
                    shard,
                    self._client_identifier,
                    self._service_discovery,
                    self._session_timeout_ms,
                    on_close=self.on_session_closed,
                    heartbeat_interval_ms=self._heartbeat_interval_ms,
                )
                self.sessions_by_shard[shard] = s
            return s

    def on_session_closed(self, session: Session):
        with self.lock:
            # Only evict when the tracked session is still the one that closed:
            # otherwise a late callback from an already-replaced session would
            # drop a live replacement and leak its heartbeat thread.
            if self.sessions_by_shard.get(session.shard_id()) is session:
                del self.sessions_by_shard[session.shard_id()]

    def close(self):
        with self.lock:
            sessions = list(self.sessions_by_shard.values())
            self.sessions_by_shard.clear()
        for s in sessions:
            s.close()
