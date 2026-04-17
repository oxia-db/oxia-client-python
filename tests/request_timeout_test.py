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

"""Unit tests for the RequestTimeoutInterceptor."""

from types import SimpleNamespace

from oxia.internal.interceptors import RequestTimeoutInterceptor, _LONG_LIVED_STREAMS


def _make_details(method, timeout=None):
    """Minimal stand-in for grpc.ClientCallDetails."""
    return SimpleNamespace(
        method=method,
        timeout=timeout,
        metadata=None,
        credentials=None,
        wait_for_ready=None,
        compression=None,
    )


# SimpleNamespace doesn't have _replace — create a real namedtuple-backed thing
from collections import namedtuple

CallDetails = namedtuple("CallDetails",
                         ["method", "timeout", "metadata", "credentials",
                          "wait_for_ready", "compression"])


def _details(method, timeout=None):
    return CallDetails(method, timeout, None, None, None, None)


class _Recorder:
    """Captures the CallDetails passed to a continuation."""
    def __init__(self):
        self.last_details = None

    def __call__(self, details, request):
        self.last_details = details
        return None


def test_interceptor_adds_timeout_to_unary_unary():
    interceptor = RequestTimeoutInterceptor(timeout_s=5.0)
    rec = _Recorder()
    details = _details("/io.streamnative.oxia.proto.OxiaClient/Write")

    interceptor.intercept_unary_unary(rec, details, object())
    assert rec.last_details.timeout == 5.0


def test_interceptor_respects_explicit_timeout():
    """If a caller already set a timeout, the interceptor must not overwrite it."""
    interceptor = RequestTimeoutInterceptor(timeout_s=5.0)
    rec = _Recorder()
    details = _details("/io.streamnative.oxia.proto.OxiaClient/Write", timeout=1.0)

    interceptor.intercept_unary_unary(rec, details, object())
    assert rec.last_details.timeout == 1.0


def test_interceptor_skips_long_lived_streams():
    """Long-lived server-streaming RPCs must not carry a timeout."""
    interceptor = RequestTimeoutInterceptor(timeout_s=5.0)
    for method in _LONG_LIVED_STREAMS:
        rec = _Recorder()
        details = _details(method)
        interceptor.intercept_unary_stream(rec, details, object())
        assert rec.last_details.timeout is None, \
            f"long-lived stream {method} must not receive a timeout"


def test_interceptor_adds_timeout_to_finite_streams():
    """Finite server-streaming RPCs (read, list) should receive the timeout."""
    interceptor = RequestTimeoutInterceptor(timeout_s=5.0)
    for method in [
        "/io.streamnative.oxia.proto.OxiaClient/Read",
        "/io.streamnative.oxia.proto.OxiaClient/List",
    ]:
        rec = _Recorder()
        details = _details(method)
        interceptor.intercept_unary_stream(rec, details, object())
        assert rec.last_details.timeout == 5.0, \
            f"finite stream {method} should receive the default timeout"


def test_connection_pool_wires_interceptor():
    """ConnectionPool created with request_timeout_ms must install the
    interceptor."""
    from oxia.internal.connection_pool import ConnectionPool

    pool = ConnectionPool(request_timeout_ms=2_000)
    assert len(pool._interceptors) == 1
    assert isinstance(pool._interceptors[0], RequestTimeoutInterceptor)
    assert pool._interceptors[0]._timeout_s == 2.0


def test_connection_pool_no_interceptor_by_default():
    from oxia.internal.connection_pool import ConnectionPool

    pool = ConnectionPool()
    assert pool._interceptors == []
