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

from oxia.internal.service_discovery import ServiceDiscovery
from oxia.internal.backoff import Backoff
import grpc
import threading
import queue
import logging

from oxia.internal.proto.io.streamnative.oxia import proto as pb
from oxia.defs import Notification, NotificationType

log = logging.getLogger(__name__)


class Notifications:
    def __init__(self, service_discovery: ServiceDiscovery):
        self._lock = threading.Lock()
        self._service_discovery = service_discovery
        self._closed = False
        self._notifications = queue.Queue(100)
        self._last_notification = {}
        self._threads = []
        self._streams = []

        self._ready = threading.Event()
        self._coordinator = threading.Thread(
            target=self._coordinator_loop, daemon=True,
            name="oxia-notifications-coordinator")
        self._coordinator.start()

        if not self._ready.wait(timeout=30):
            raise RuntimeError(
                "Timed out waiting for initial notification streams")

    def close(self):
        self._closed = True
        self._teardown_workers()
        self._coordinator.join(timeout=5.0)
        self._notifications.shutdown()

    def _teardown_workers(self):
        """Cancel all streams then join all worker threads.

        Cancels under the lock so no new batches arrive, then joins
        OUTSIDE the lock so workers can finish their lock-holding
        updates without deadlocking.
        """
        with self._lock:
            streams = list(self._streams)
            self._streams = []
        for stream in streams:
            stream.cancel()
        threads = self._threads
        self._threads = []
        for thread in threads:
            thread.join()

    def _coordinator_loop(self):
        backoff = Backoff()
        while not self._closed:
            failed_event = threading.Event()
            try:
                all_shards = self._service_discovery.get_all_shards()
                barrier = threading.Barrier(len(all_shards) + 1)

                threads = []
                streams = []
                for shard, stub in all_shards:
                    stream = stub.get_notifications(pb.NotificationsRequest(
                        shard=shard,
                        start_offset_exclusive=self._last_notification.get(shard),
                    ))
                    t = threading.Thread(
                        target=self._worker,
                        args=(stream, barrier, failed_event),
                        daemon=True,
                        name=f"oxia-notifications-{shard}",
                    )
                    threads.append(t)
                    streams.append(stream)
                    t.start()

                with self._lock:
                    self._threads = threads
                    self._streams = streams

                barrier.wait()
                self._ready.set()
                backoff.reset()

                # Block until a worker exits (stream failure or end).
                failed_event.wait()
                if self._closed:
                    return
                self._teardown_workers()
                backoff.wait_next()

            except threading.BrokenBarrierError:
                # A worker failed before delivering its first batch.
                if self._closed:
                    return
                self._teardown_workers()
                backoff.wait_next()
            except Exception:
                if self._closed:
                    return
                log.exception('Failed to set up notification streams')
                self._teardown_workers()
                backoff.wait_next()

    def _worker(self, stream, barrier, failed_event):
        is_first = True
        try:
            for batch in stream:
                for k, n in batch.notifications.items():
                    notification = Notification()
                    notification._key = k
                    notification._type = NotificationType(n.type)
                    notification._version_id = n.version_id if n.version_id else 0
                    notification._key_range_end = n.key_range_last
                    self._notifications.put(notification)

                with self._lock:
                    self._last_notification[batch.shard] = batch.offset

                if is_first:
                    barrier.wait()
                    is_first = False
        except threading.BrokenBarrierError:
            return
        except Exception as e:
            if self._closed:
                return
            if not (isinstance(e, grpc.RpcError)
                    and e.code() == grpc.StatusCode.CANCELLED):
                log.exception('Notification worker failed: %s', e)
        finally:
            if is_first:
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass
            failed_event.set()

    def __iter__(self):
        return self

    def __next__(self):
        i = self._notifications.get()
        self._notifications.task_done()
        return i
