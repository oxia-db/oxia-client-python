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

"""gRPC client interceptors used by the Oxia client."""

import grpc

# Server streaming RPCs that are long-lived and should NOT carry a
# request timeout (they stay open for the lifetime of the client
# subscription).
_LONG_LIVED_STREAMS = frozenset({
    "/io.streamnative.oxia.proto.OxiaClient/GetShardAssignments",
    "/io.streamnative.oxia.proto.OxiaClient/GetNotifications",
    "/io.streamnative.oxia.proto.OxiaClient/GetSequenceUpdates",
    "/io.streamnative.oxia.proto.OxiaClient/RangeScan",
})


class RequestTimeoutInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
):
    """Apply a default per-call deadline to every RPC that does not
    already carry one, except for the long-lived server-streaming RPCs
    listed in ``_LONG_LIVED_STREAMS``."""

    def __init__(self, timeout_s: float):
        self._timeout_s = timeout_s

    def _with_timeout(self, client_call_details):
        if client_call_details.method in _LONG_LIVED_STREAMS:
            return client_call_details
        if client_call_details.timeout is not None:
            return client_call_details
        return client_call_details._replace(timeout=self._timeout_s)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._with_timeout(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._with_timeout(client_call_details), request)
