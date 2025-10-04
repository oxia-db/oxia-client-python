# Copyright 2025 StreamNative, Inc.
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

import threading, uuid
from oxia.internal.service_discovery import ServiceDiscovery
from oxia.internal.proto.io.streamnative import oxia as pb


class Session:
    def __init__(self, shard: int, client_identifier: str, service_discovery: ServiceDiscovery,
                 session_timeout_ms: int):
        self._shard = shard
        self._client_identifier = client_identifier
        self._service_discovery = service_discovery
        res = service_discovery.get_stub(shard).create_session(pb.CreateSessionRequest(
            shard=shard,
            session_timeout_ms=session_timeout_ms,
            client_identity=client_identifier,
        ))
        self._session_id = res.session_id

    def shard_id(self):
        return self._shard

    def session_id(self):
        return self._session_id

    def client_identifier(self):
        return self._client_identifier

    def close(self):
        self._service_discovery.get_stub(self._shard).close_session(pb.CloseSessionRequest(
            shard=self._shard,
            session_id=self._session_id,
        ))


class SessionManager:
    def __init__(self, service_discovery: ServiceDiscovery, session_timeout_ms: int, client_identifier: str):
        self._session_timeout_ms = session_timeout_ms
        self._client_identifier = client_identifier or uuid.uuid4().hex
        self._service_discovery = service_discovery
        self.sessions_by_shard = {}
        self.lock = threading.Lock()

    def get_session(self, shard: int) -> Session:
        with self.lock:
            s = self.sessions_by_shard.get(shard)
            if s is None:
                s = Session(shard, self._client_identifier, self._service_discovery,  self._session_timeout_ms)
                self.sessions_by_shard[shard] = s
            return s

    def on_session_closed(self, session: Session):
        with self.lock:
            del self.sessions_by_shard[session.shard_id()]

    def close(self):
        with self.lock:
            for s in self.sessions_by_shard.values():
                s.close()
            self.sessions_by_shard.clear()
