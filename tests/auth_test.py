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

"""Unit tests for the authentication subsystem."""

from collections import namedtuple

from oxia.auth import TokenAuthentication
from oxia.defs import Authentication
from oxia.internal.interceptors import AuthenticationInterceptor


CallDetails = namedtuple("CallDetails",
                         ["method", "timeout", "metadata", "credentials",
                          "wait_for_ready", "compression"])


def _details(method="/io.streamnative.oxia.proto.OxiaClient/Write",
             metadata=None):
    return CallDetails(method, None, metadata, None, None, None)


class _Recorder:
    def __init__(self):
        self.last_details = None

    def __call__(self, details, request):
        self.last_details = details
        return None


# ---------------------------------------------------------------------------
# TokenAuthentication
# ---------------------------------------------------------------------------

def test_token_authentication_static_token():
    auth = TokenAuthentication("my-secret")
    creds = auth.generate_credentials()
    assert creds == {"authorization": "Bearer my-secret"}


def test_token_authentication_dynamic_supplier():
    counter = [0]

    def supplier():
        counter[0] += 1
        return f"token-{counter[0]}"

    auth = TokenAuthentication(supplier)
    assert auth.generate_credentials() == {"authorization": "Bearer token-1"}
    assert auth.generate_credentials() == {"authorization": "Bearer token-2"}


def test_token_authentication_is_an_authentication():
    assert isinstance(TokenAuthentication("x"), Authentication)


# ---------------------------------------------------------------------------
# AuthenticationInterceptor
# ---------------------------------------------------------------------------

def test_interceptor_adds_metadata_on_unary_unary():
    auth = TokenAuthentication("tok")
    interceptor = AuthenticationInterceptor(auth)
    rec = _Recorder()
    interceptor.intercept_unary_unary(rec, _details(), object())
    assert ("authorization", "Bearer tok") in rec.last_details.metadata


def test_interceptor_adds_metadata_on_unary_stream():
    auth = TokenAuthentication("tok")
    interceptor = AuthenticationInterceptor(auth)
    rec = _Recorder()
    interceptor.intercept_unary_stream(rec, _details(), object())
    assert ("authorization", "Bearer tok") in rec.last_details.metadata


def test_interceptor_preserves_existing_metadata():
    auth = TokenAuthentication("tok")
    interceptor = AuthenticationInterceptor(auth)
    rec = _Recorder()
    existing = [("x-trace-id", "abc123")]
    interceptor.intercept_unary_unary(rec, _details(metadata=existing), object())
    assert ("x-trace-id", "abc123") in rec.last_details.metadata
    assert ("authorization", "Bearer tok") in rec.last_details.metadata


def test_interceptor_lowercases_header_keys():
    """gRPC metadata keys must be lowercase."""
    class UppercaseAuth(Authentication):
        def generate_credentials(self):
            return {"Authorization": "Bearer x", "X-Custom-Header": "v"}

    rec = _Recorder()
    AuthenticationInterceptor(UppercaseAuth()).intercept_unary_unary(
        rec, _details(), object())
    keys = [k for k, _v in rec.last_details.metadata]
    assert all(k == k.lower() for k in keys), \
        f"metadata keys must be lowercase, got {keys}"


def test_interceptor_skips_empty_credentials():
    class NoAuth(Authentication):
        def generate_credentials(self):
            return {}

    rec = _Recorder()
    original = _details()
    AuthenticationInterceptor(NoAuth()).intercept_unary_unary(rec, original, object())
    # No change to details
    assert rec.last_details is original


# ---------------------------------------------------------------------------
# ConnectionPool wiring
# ---------------------------------------------------------------------------

def test_connection_pool_installs_auth_interceptor():
    from oxia.internal.connection_pool import ConnectionPool

    auth = TokenAuthentication("tok")
    pool = ConnectionPool(authentication=auth)
    assert any(isinstance(i, AuthenticationInterceptor)
               for i in pool._interceptors)


def test_connection_pool_no_auth_interceptor_by_default():
    from oxia.internal.connection_pool import ConnectionPool

    pool = ConnectionPool()
    assert not any(isinstance(i, AuthenticationInterceptor)
                   for i in pool._interceptors)
