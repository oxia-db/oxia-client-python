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
from typing import Optional

import grpc
from oxia.internal.interceptors import RequestTimeoutInterceptor
from oxia.internal.proto.io.streamnative.oxia.proto import OxiaClientStub


class ConnectionPool:

    def __init__(self, request_timeout_ms: Optional[int] = None):
        self._lock = threading.Lock()
        self.connections = {}
        self._interceptors = []
        if request_timeout_ms is not None:
            self._interceptors.append(
                RequestTimeoutInterceptor(request_timeout_ms / 1000.0))

    def get(self, address) -> OxiaClientStub:
        with self._lock:
            x = self.connections.get(address)
            if x is None:
                channel = grpc.insecure_channel(address)
                if self._interceptors:
                    channel = grpc.intercept_channel(channel, *self._interceptors)
                stub = OxiaClientStub(channel)
                x = (channel, stub)
                self.connections[address] = x
            return x[1]

    def close(self):
        with self._lock:
            for c in self.connections.values():
                c[0].close()
            self.connections.clear()
