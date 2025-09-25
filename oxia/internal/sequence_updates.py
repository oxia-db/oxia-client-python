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

import threading, queue
import grpc

from oxia.internal.service_discovery import ServiceDiscovery
import oxia.proto.io.streamnative.oxia.proto as pb

class SequenceUpdatesImpl:
    def __init__(self, service_discovery: ServiceDiscovery, prefix_key: str, partition_key: str, is_client_closed):
        self._service_discovery = service_discovery
        self._queue = queue.Queue(10)
        self._closed = False
        self._lock = threading.RLock()
        self._prefix_key = prefix_key
        self._partition_key = partition_key
        self._stream = None
        self._is_client_closed = is_client_closed
        self._thread = threading.Thread(target=self._internal_thread, daemon=True)
        self._thread.start()

    def _internal_thread(self):
        while not self._closed:
            try:
                with self._lock:
                    if self._closed: return
                    elif self._is_client_closed():
                        self.close()
                        return

                    shard, stub = self._service_discovery.get_leader(self._prefix_key, self._partition_key)
                    self._stream = stub.get_sequence_updates(pb.GetSequenceUpdatesRequest(
                        shard=shard,
                        key=self._prefix_key,
                    ))

                for i in self._stream:
                    self._queue.put(i.highest_sequence_key)
            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.CANCELLED:
                    print(f'Exception while getting sequence updates: {e}')

    def __iter__(self):
        return self

    def __next__(self):
        try:
            i = self._queue.get()
            self._queue.task_done()
            return i
        except queue.ShutDown:
            raise StopIteration

    def close(self):
        with self._lock:
            self._closed = True
            if self._stream:
                self._stream.cancel()

            self._queue.shutdown()
            if self._thread != threading.current_thread():
                self._thread.join()