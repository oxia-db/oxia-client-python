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

"""Unit tests for TLS channel creation."""

from unittest.mock import patch, MagicMock

from oxia.internal.connection_pool import ConnectionPool


def test_insecure_channel_by_default():
    pool = ConnectionPool()
    with patch("oxia.internal.connection_pool.grpc.insecure_channel") as insecure, \
         patch("oxia.internal.connection_pool.grpc.secure_channel") as secure:
        insecure.return_value = MagicMock()
        pool.get("localhost:6648")
        insecure.assert_called_once_with("localhost:6648")
        secure.assert_not_called()


def test_secure_channel_when_tls_true():
    pool = ConnectionPool(tls=True)
    with patch("oxia.internal.connection_pool.grpc.insecure_channel") as insecure, \
         patch("oxia.internal.connection_pool.grpc.secure_channel") as secure, \
         patch("oxia.internal.connection_pool.grpc.ssl_channel_credentials") as creds:
        secure.return_value = MagicMock()
        creds.return_value = "fake-creds"
        pool.get("oxia.example.com:6648")

        creds.assert_called_once()
        secure.assert_called_once_with("oxia.example.com:6648", "fake-creds")
        insecure.assert_not_called()


def test_tls_url_prefix_enables_tls():
    """service_address starting with tls:// must enable TLS and strip
    the scheme, without needing an explicit tls=True."""
    # We can't actually construct a Client without a server, so test
    # the prefix-handling logic at the boundary: ConnectionPool receives
    # the stripped address and tls=True.
    import oxia

    real_init = ConnectionPool.__init__
    init_calls = []

    def capture_init(self, *args, **kwargs):
        init_calls.append(kwargs)
        real_init(self, *args, **kwargs)

    real_sd_init = None  # will be patched to avoid starting a real thread

    with patch.object(ConnectionPool, "__init__", capture_init), \
         patch("oxia.client.ServiceDiscovery") as mock_sd, \
         patch("oxia.client.SessionManager"):
        mock_sd.return_value = MagicMock()
        oxia.Client("tls://example.com:6648")

    assert len(init_calls) == 1
    assert init_calls[0].get("tls") is True
    # And the address passed to ServiceDiscovery must have the scheme stripped
    _pos, sd_kwargs = mock_sd.call_args[0], mock_sd.call_args[1]
    passed_address = mock_sd.call_args.args[0]
    assert passed_address == "example.com:6648"


def test_explicit_tls_parameter_enables_tls():
    import oxia

    real_init = ConnectionPool.__init__
    init_calls = []

    def capture_init(self, *args, **kwargs):
        init_calls.append(kwargs)
        real_init(self, *args, **kwargs)

    with patch.object(ConnectionPool, "__init__", capture_init), \
         patch("oxia.client.ServiceDiscovery"), \
         patch("oxia.client.SessionManager"):
        oxia.Client("example.com:6648", tls=True)

    assert init_calls[0].get("tls") is True
