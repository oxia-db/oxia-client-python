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

"""Unit tests for oxia.internal.service_discovery.ServiceDiscovery."""

import pytest

from oxia.internal.service_discovery import ServiceDiscovery


class FailingStub:
    """A stub whose get_shard_assignments always raises."""

    def get_shard_assignments(self, request):
        raise ConnectionError("simulated connection failure")


class FailingConnectionPool:
    """Returns a FailingStub for any address."""

    def get(self, address):
        return FailingStub()


def test_init_raises_on_connection_failure():
    """When the initial shard-assignments stream cannot be established
    within the timeout, the constructor must raise a clear RuntimeError
    rather than propagating a BrokenBarrierError."""
    with pytest.raises(RuntimeError, match="Timed out.*shard assignments"):
        ServiceDiscovery("localhost:99999", FailingConnectionPool(), "default",
                         init_timeout=1)
