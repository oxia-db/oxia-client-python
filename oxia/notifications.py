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

from oxia.service_discovery import ServiceDiscovery
from oxia.backoff import Backoff
import threading, queue, logging
import oxia.proto.io.streamnative.oxia.proto as pb
from oxia.api import Notification, NotificationType

class Notifications:
    def __init__(self, service_discovery : ServiceDiscovery):
        self._lock = threading.Lock()
        self._service_discovery = service_discovery
        self._closed = False
        self._notifications = queue.Queue(100)
        self._last_notification = {}
        self._threads = []
        self._streams = []
        self._get_notifications_with_retries()

    def close(self):
        self._closed = True
        with self._lock:
            for stream in self._streams:
                stream.cancel()

            for thread in self._threads:
                thread.join()
        self._notifications.shutdown()


    def _get_notifications_with_retries(self):
        backoff = Backoff()

        while not self._closed:
            failed_condition = threading.Condition(self._lock)


            try:
                self._lock.acquire()

                all_shards = self._service_discovery.get_all_shards()
                first_notification_barrier = threading.Barrier(len(all_shards) + 1)
                self._lock.release()

                for shard, stub in all_shards:
                    stream = stub.get_notifications(pb.NotificationsRequest(
                        shard=shard,
                        start_offset_exclusive=self._last_notification.get(shard)
                    ))
                    thread = threading.Thread(target=self._fetch_notifications_in_thread,
                                      args=(stream, first_notification_barrier, failed_condition),
                                      daemon=True)
                    self._threads.append(thread)
                    self._streams.append(stream)
                    thread.start()

                first_notification_barrier.wait()
                return

            except Exception as e:
                logging.exception('Failed to get notifications on shard', e)
                for stream in self._streams:
                    stream.cancel()
                for thread in self._threads:
                    thread.join()
                backoff.wait_next()

    def _fetch_notifications_in_thread(self, stream, first_notification_barrier, failed_condition):
        try:
            is_first = True
            for batch in stream:
                # print(
                #     f"Got notification batch shard={batch.shard} offset={batch.offset} ts={batch.timestamp} notifications_count={len(batch.notifications)}")
                for k, n in batch.notifications.items():
                    # print(
                    #     f"Notification: {k} - type:{n.type} version_id:{n.version_id} - key_range_last:{n.key_range_last}")
                    notification = Notification()
                    notification._key = k
                    notification._type = NotificationType(n.type)
                    notification._version_id = n.version_id if n.version_id else 0
                    notification._key_range_end = n.key_range_last
                    self._notifications.put(notification)

                with self._lock:
                    self._last_notification[batch.shard] = batch.offset

                if is_first:
                    first_notification_barrier.wait()
                    is_first = False
        except Exception as e:
            if not self._closed:
                logging.exception('Failed to get notifications', e)
                failed_condition.notify_all()


    def __iter__(self):
        return self

    def __next__(self):
        i = self._notifications.get()
        self._notifications.task_done()
        return i

