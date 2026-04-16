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

"""Unit tests for oxia.internal.connection_pool.ConnectionPool."""

import threading
from unittest.mock import patch, MagicMock

from oxia.internal.connection_pool import ConnectionPool


def test_concurrent_get_creates_only_one_channel():
    """Many threads calling get() with the same address concurrently must
    all receive the same stub and only one gRPC channel should be created.

    Without locking, some threads would see a cache miss simultaneously,
    each create their own channel, and all but one would leak."""

    pool = ConnectionPool()
    n_threads = 20
    stubs = [None] * n_threads

    with patch("oxia.internal.connection_pool.grpc.insecure_channel") as mock_ch:
        mock_ch.return_value = MagicMock()

        # Use a barrier so all threads call pool.get() at roughly the
        # same instant, maximising contention.
        go = threading.Barrier(n_threads)

        def worker(idx):
            go.wait()
            stubs[idx] = pool.get("localhost:9999")

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

    # All threads must have gotten the same stub.
    assert all(s is stubs[0] for s in stubs), \
        "all threads must get the same stub instance"
    # Exactly one channel should have been created.
    assert mock_ch.call_count == 1, \
        f"expected exactly 1 channel creation, got {mock_ch.call_count}"


def test_close_clears_pool():
    """After close(), the pool should be empty so that a subsequent get()
    does not return a stale (closed) stub."""

    pool = ConnectionPool()
    with patch("oxia.internal.connection_pool.grpc.insecure_channel") as mock_ch:
        mock_channel = MagicMock()
        mock_ch.return_value = mock_channel
        pool.get("localhost:9999")

    pool.close()
    assert len(pool.connections) == 0, \
        "pool should be empty after close()"
