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

from oxia.proto.client_pb2 import ShardAssignmentsRequest, Int32HashRange, ShardKeyRouter
from oxia.proto.client_pb2_grpc import OxiaClientStub

from backoff import  Backoff

import threading
import xxhash

class HashRange:
    def __init__(self, r: Int32HashRange):
        self.min_included = r.min_hash_inclusive
        self.max_included = r.max_hash_inclusive

    def contains(self, key_hash: int) -> bool:
        return self.min_included <= key_hash <= self.max_included

    def __str__(self):
        return f'{self.min_included:#010x}-{self.max_included:#010x}'

class Shard:
    def __init__(self, shard: int, leader: str, hash_range: HashRange):
        self.shard = shard
        self.leader = leader
        self.hash_range = hash_range

    def contains(self, key_hash: int) -> bool:
        return self.hash_range.contains(key_hash)

    def __str__(self):
        return f'Shard(shard={self.shard}, leader={self.leader}, hash_range={self.hash_range})'

class ServiceDiscovery(object):

    def __init__(self, service_address, connection_pool, namespace):
        self._assignments = {}
        self._service_address = service_address
        self._connection_pool = connection_pool
        self._namespace = namespace
        self._closed = False
        self._lock = threading.Lock()

        self._thread = threading.Thread(target=self.get_assignments, daemon=True)
        self._thread.start()

        # Wait until we have the first assignment map
        self._init_barrier = threading.Barrier(2)
        self._init_barrier.wait(30)

    def get_assignments(self):
        stub = self._connection_pool.get(self._service_address)
        backoff = Backoff()

        while not self._closed:
            try:
                for sa in stub.GetShardAssignments(ShardAssignmentsRequest(namespace=self._namespace)):
                    self._parse_assignments(sa.namespaces[self._namespace])
                    self._init_barrier.wait()
                    backoff.reset()
            except Exception as e:
                if not self._closed:
                    print("Failed to get assignments", e)
                    backoff.wait_next()

    def _parse_assignments(self, assignments):
        if assignments.shard_key_router != ShardKeyRouter.XXHASH3:
            raise Exception('Invalid ShardKeyRouter')

        self._lock.acquire()
        self._assignments = {}
        try:
            for s in assignments.assignments:
                a = Shard(s.shard, s.leader, HashRange(s.int32_hash_range))
                self._assignments[a.shard] = a
                # print("New assignment", a)
        finally:
            self._lock.release()

    def get_shard(self, key: str) -> Shard:
        h = xxhash.xxh64(key).intdigest() & 0x00000000FFFFFFFF
        # print(f"Hash for key {key} is {h:#010x}")

        with self._lock:
            for s in self._assignments.values():
                if s.contains(h):
                    return s
        raise Exception(f'No shard found for key {key}')

    def get_stub(self, shard: int) -> OxiaClientStub:
        with self._lock:
            for s in self._assignments.values():
                if s.shard == shard:
                    return self._connection_pool.get(s.leader)
        raise Exception(f'No stub found for shard {shard}')

    def get_leader(self, key: str, partition_key: str):
        hashing_key = partition_key if partition_key else key
        s = self.get_shard(hashing_key)
        return s.shard, self._connection_pool.get(s.leader)

    def get_all_shards(self):
        self._lock.acquire()
        try:
            return [(s.shard, self._connection_pool.get(s.leader))
                    for s in self._assignments.values()]
        finally:
            self._lock.release()

    def close(self):
        self._closed = True
        self._thread.join()