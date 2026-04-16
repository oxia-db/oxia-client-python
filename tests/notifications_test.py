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

"""Unit tests for oxia.internal.notifications.Notifications.

Uses fakes instead of a real Oxia server so we can simulate stream
failures, verify reconnect behaviour, and detect deadlocks.
"""

import threading
import time

import grpc
import pytest

from oxia.defs import NotificationType
from oxia.internal.notifications import Notifications
from oxia.internal.proto.io.streamnative.oxia import proto as pb


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

def _make_batch(shard, offset, key, ntype=pb.NotificationType.KEY_CREATED,
                version_id=1):
    """Build a minimal NotificationBatch for testing."""
    return pb.NotificationBatch(
        shard=shard,
        offset=offset,
        timestamp=1000,
        notifications={
            key: pb.Notification(type=ntype, version_id=version_id),
        },
    )


class FakeStream:
    """A fake gRPC server-streaming iterator.

    Yields *batches* in order, then optionally raises *fail_with* to
    simulate a stream error.  After either exhaustion or failure the
    iterator blocks forever (mimicking a long-lived stream) unless
    *cancel()* has been called.
    """

    def __init__(self, batches, fail_with=None):
        self._batches = list(batches)
        self._fail_with = fail_with
        self._index = 0
        self._cancelled = threading.Event()

    def __iter__(self):
        return self

    def __next__(self):
        if self._cancelled.is_set():
            raise grpc.RpcError()
        if self._index < len(self._batches):
            b = self._batches[self._index]
            self._index += 1
            return b
        if self._fail_with is not None:
            exc = self._fail_with
            self._fail_with = None  # only raise once
            raise exc
        # Block until cancelled (simulates an idle long-lived stream).
        self._cancelled.wait()
        raise grpc.RpcError()

    def cancel(self):
        self._cancelled.set()


class FakeStub:
    """A fake OxiaClient stub that returns FakeStreams from
    ``get_notifications()``.  Streams are consumed from a list so
    successive calls (reconnects) get different streams."""

    def __init__(self, streams_per_shard):
        # {shard: [stream1, stream2, ...]}
        self._streams = dict(streams_per_shard)
        self._lock = threading.Lock()

    def get_notifications(self, request):
        with self._lock:
            shard = request.shard
            streams = self._streams.get(shard, [])
            if not streams:
                raise RuntimeError(f"no more fake streams for shard {shard}")
            return streams.pop(0)


class FakeServiceDiscovery:
    """Minimal fake that returns a fixed set of (shard, stub) pairs."""

    def __init__(self, shards_and_stubs):
        self._shards_and_stubs = list(shards_and_stubs)

    def get_all_shards(self):
        return self._shards_and_stubs


def _drain(notifications, count, timeout=5.0):
    """Collect *count* notifications, failing if *timeout* is exceeded."""
    result = []
    deadline = time.monotonic() + timeout
    for _ in range(count):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        # Use a polling loop — Notifications.__next__ blocks on queue.get
        # and we can't easily inject a timeout there without modifying the
        # class under test.
        got = [None]

        def _get():
            got[0] = next(notifications)

        t = threading.Thread(target=_get, daemon=True)
        t.start()
        t.join(timeout=remaining)
        if t.is_alive():
            break
        result.append(got[0])
    return result


# ---------------------------------------------------------------------------
# Bug #1 — reconnect after stream failure is dead code
# ---------------------------------------------------------------------------

def test_notifications_reconnect_after_stream_failure():
    """When a per-shard notification stream fails, the Notifications object
    must reconnect and continue delivering notifications.

    Currently FAILS because the reconnect path is dead code:
    ``failed_condition.notify_all()`` is called without the lock held
    (RuntimeError), and even if that were fixed nobody waits on the
    condition — so the worker thread just dies silently."""

    shard = 0
    # First stream: delivers 1 batch, then fails.
    stream1 = FakeStream(
        batches=[_make_batch(shard, offset=1, key="/k1")],
        fail_with=RuntimeError("simulated stream failure"),
    )
    # Second stream (after reconnect): delivers 1 more batch.
    stream2 = FakeStream(
        batches=[_make_batch(shard, offset=2, key="/k2")],
    )

    stub = FakeStub(streams_per_shard={shard: [stream1, stream2]})
    sd = FakeServiceDiscovery(shards_and_stubs=[(shard, stub)])

    notifications = Notifications(sd)
    try:
        results = _drain(notifications, count=2, timeout=5.0)
        assert len(results) == 2, (
            f"expected 2 notifications (one from each stream), got {len(results)}: "
            f"{[r.key() for r in results]}"
        )
        assert results[0].key() == "/k1"
        assert results[0].notification_type() == NotificationType.KEY_CREATED
        assert results[1].key() == "/k2"
    finally:
        notifications.close()


#---------------------------------------------------------------------------
# Bug #6 — close() joins threads while holding the lock → deadlock
# ---------------------------------------------------------------------------

class SlowLockStream:
    """A fake stream that delivers batches continuously.

    After each batch, it waits for a brief period before yielding the
    next one.  The goal: the worker thread spends most of its time
    cycling between ``with self._lock:`` and ``stream.__next__()``,
    maximising the chance that ``close()`` tries to ``join()`` while
    the worker is holding the lock.
    """

    def __init__(self, shard, batch_count=200):
        self._shard = shard
        self._batch_count = batch_count
        self._index = 0
        self._cancelled = threading.Event()

    def __iter__(self):
        return self

    def __next__(self):
        if self._cancelled.is_set():
            raise grpc.RpcError()
        if self._index >= self._batch_count:
            self._cancelled.wait()
            raise grpc.RpcError()
        self._index += 1
        # Tiny yield so the worker isn't starved — but fast enough that
        # we cycle through the lock hundreds of times.
        time.sleep(0.001)
        return _make_batch(self._shard, offset=self._index,
                           key=f"/k{self._index}")

    def cancel(self):
        self._cancelled.set()


def test_notifications_close_does_not_deadlock():
    """Notifications.close() must complete promptly even while worker
    threads are actively processing batches (cycling through the lock).

    The underlying bug: close() holds self._lock while calling join()
    on worker threads, but workers acquire self._lock to update
    _last_notification after every batch.  If close() catches the
    worker mid-lock, both sides wait on each other."""

    shard = 0
    stream = SlowLockStream(shard, batch_count=500)
    stub = FakeStub(streams_per_shard={shard: [stream]})
    sd = FakeServiceDiscovery(shards_and_stubs=[(shard, stub)])

    notifications = Notifications(sd)

    # Drain a few notifications so the worker is well into its
    # batch-processing loop (past the barrier, cycling through the lock).
    results = _drain(notifications, count=5, timeout=3.0)
    assert len(results) >= 5

    # close() must return within a reasonable time.
    close_done = threading.Event()

    def _do_close():
        notifications.close()
        close_done.set()

    t = threading.Thread(target=_do_close, daemon=True)
    t.start()
    finished = close_done.wait(timeout=5.0)
    assert finished, "Notifications.close() did not complete within 5s — likely deadlocked"
