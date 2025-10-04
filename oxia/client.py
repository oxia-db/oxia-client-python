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
import functools
import heapq
from typing import Iterator

from oxia.internal.compare import compare_tuple_with_slash, compare_with_slash
from oxia.internal.connection_pool import ConnectionPool
from oxia.internal.notifications import Notifications
from oxia.internal.sequence_updates import SequenceUpdatesImpl
from oxia.internal.sessions import SessionManager
from oxia.internal.service_discovery import ServiceDiscovery
from oxia.internal.proto.io.streamnative import oxia as pb
import oxia.ex

import datetime

def _check_status(status: pb.Status):
    if status == pb.Status.OK:
        pass
    elif status == pb.Status.KEY_NOT_FOUND:
        raise oxia.ex.KeyNotFound()
    elif status == pb.Status.UNEXPECTED_VERSION_ID:
        raise oxia.ex.UnexpectedVersionId()
    elif status == pb.Status.SESSION_DOES_NOT_EXIST:
        raise oxia.ex.SessionNotFound()


def _get_version(pbv : pb.Version):
    v = oxia.defs.Version()
    v._version_id = pbv.version_id
    v._modifications_count = pbv.modifications_count
    v._created_timestamp = datetime.datetime.fromtimestamp(pbv.created_timestamp/1000.0)
    v._modified_timestamp = datetime.datetime.fromtimestamp(pbv.modified_timestamp/1000.0)
    v._session_id = pbv.session_id
    v._client_identity = pbv.client_identity
    return v

class Client:
    def __init__(self, service_address: str,
                 namespace: str = "default",
                 session_timeout_ms: int = 30_000,
                 client_identifier: str = None,
                 ):
        self._closed = False
        self._connections = ConnectionPool()
        self._service_discovery = ServiceDiscovery(service_address, self._connections, namespace)
        self._session_manager = SessionManager(self._service_discovery, session_timeout_ms, client_identifier)

    def put(self, key: str, value: object,
            partition_key: str = None,
            expected_version_id: int = None,
            ephemeral: bool = False,
            sequence_keys_deltas: list[int] = None,
            secondary_indexes: dict[str, str] = None,
            ) -> (str, oxia.defs.Version):
        """
            Put Associates a value with a key

          There are few options that can be passed to the Put operation:
           - The Put operation can be made conditional on that the record hasn't changed from
             a specific existing version by passing the [ExpectedVersionId] option.
           - Client can assert that the record does not exist by passing [ExpectedRecordNotExists]
           - Client can create an ephemeral record with [Ephemeral]

          Returns the actual key of the inserted record
          Returns a [Version] object that contains information about the newly updated record
          Throw [ErrorUnexpectedVersionId] if the expected version id does not match the
          current version id of the record

        :param key:
        :param value:
        :param partition_key:
        :param expected_version_id:
        :param ephemeral:
        :return:
        """
        shard, stub = self._service_discovery.get_leader(key, partition_key)

        if sequence_keys_deltas:
            if not partition_key:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas can only be used with partition_key")
            if expected_version_id is not None:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas cannot be used with expected_version_id")

        if type(value) is str:
            value = bytes(str(value), encoding='utf-8')

        pr = pb.PutRequest(key=key, value=value,
                           partition_key=partition_key,
                           expected_version_id=expected_version_id,
                           sequence_key_delta=sequence_keys_deltas,
                           )
        if ephemeral:
            session = self._session_manager.get_session(shard)
            pr.session_id = session.session_id()
            pr.client_identity = session.client_identifier()

        if secondary_indexes:
            indexes = []
            for k, v in secondary_indexes.items():
                indexes.append(pb.SecondaryIndex(k, v))
            pr.secondary_indexes = indexes

        res = stub.write(pb.WriteRequest(shard=shard, puts=[pr]))

        put_res = res.puts[0]  # We only have 1 request
        _check_status(put_res.status)

        if put_res.key:
            key = put_res.key
        return key, _get_version(put_res.version)

    def delete(self, key: str,
               partition_key: str = None,
               expected_version_id: int = None, ) -> bool:
        # // Delete removes the key and its associated value from the data store.
        # //
        # // The Delete operation can be made conditional on that the record hasn't changed from
        # // a specific existing version by passing the [ExpectedVersionId] option.
        # // Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
        # // current version id of the record
        shard, stub = self._service_discovery.get_leader(key, partition_key)

        dr = pb.DeleteRequest(key=key,
                              expected_version_id=expected_version_id)
        res = stub.write(pb.WriteRequest(shard=shard, deletes=[dr]))

        status = res.deletes[0].status # We only have 1 request
        if status == pb.Status.KEY_NOT_FOUND:
            return False
        else:
            _check_status(status)
            return True

    def delete_range(self,  min_key_inclusive: str, max_key_exclusive: str, partition_key: str = None):
        # // DeleteRange deletes any records with keys within the specified range.
        # // Note: Oxia uses a custom sorting order that treats `/` characters in special way.
        # // Refer to this documentation for the specifics:
        # // https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md
        if partition_key is None:
            for shard, stub in self._service_discovery.get_all_shards():
                self._delete_range_single_shard(min_key_inclusive, max_key_exclusive, shard, stub)
        else:
            shard, stub = self._service_discovery.get_leader(partition_key, partition_key)
            self._delete_range_single_shard(min_key_inclusive, max_key_exclusive, shard, stub)

    @staticmethod
    def _delete_range_single_shard(min_key_inclusive: str, max_key_exclusive: str, shard: int, stub):
        dr = pb.DeleteRangeRequest(start_inclusive=min_key_inclusive,
                              end_exclusive=max_key_exclusive)
        res = stub.write(pb.WriteRequest(shard=shard, delete_ranges=[dr]))

        status = res.delete_ranges[0].status  # We only have 1 request
        _check_status(status)

    def get(self, key: str,
            partition_key: str = None,
            comparison_type: oxia.defs.ComparisonType = oxia.defs.ComparisonType.EQUAL,
            include_value: bool = True,
            use_index: str = None,
            ) -> (str, str, oxia.defs.Version):
        # // Get returns the value associated with the specified key.
        # // In addition to the value, a version object is also returned, with information
        # // about the record state.
        # // Returns ErrorKeyNotFound if the record does not exist
        if partition_key is None and \
                (comparison_type != oxia.defs.ComparisonType.EQUAL
                     or use_index is not None):

            results = []
            for shard, stub in self._service_discovery.get_all_shards():
                try:
                    k, val, version =  self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)
                    results.append((k, val, version))
                except oxia.ex.KeyNotFound:
                    pass
            if not results: raise oxia.ex.KeyNotFound
            results.sort(key=functools.cmp_to_key(compare_tuple_with_slash))

            if comparison_type == oxia.defs.ComparisonType.EQUAL or \
                comparison_type == oxia.defs.ComparisonType.CEILING or \
                comparison_type == oxia.defs.ComparisonType.HIGHER:
                return results[0]
            elif comparison_type == oxia.defs.ComparisonType.FLOOR or \
                    comparison_type == oxia.defs.ComparisonType.LOWER:
                return results[-1]

        else:
            # Single shard get operation
            shard, stub = self._service_discovery.get_leader(key, partition_key)
            return self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)

    @staticmethod
    def _get_single_shard(shard: int, stub,
                          key: str,
                          comparison_type: oxia.defs.ComparisonType,
                          include_value: bool,
                          use_index: str) -> (str, str, oxia.defs.Version):
        gr = pb.GetRequest(key=key,
                           comparison_type=comparison_type,
                           include_value=include_value,
                           secondary_index_name=use_index)
        res = next(stub.read(pb.ReadRequest(shard=shard, gets=[gr])))
        get_res = res.gets[0] # We only have 1 request
        _check_status(get_res.status)

        res_key = get_res.key if get_res.key is not None else key
        return res_key, get_res.value, _get_version(get_res.version)

    def list(self, min_key_inclusive: str, max_key_exclusive: str,
             partition_key: str = None,
             use_index: str = None,
             ):
        # // List any existing keys within the specified range.
        # // Note: Oxia uses a custom sorting order that treats `/` characters in special way.
        # // Refer to this documentation for the specifics:
        # // https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md
        if partition_key is None:
            all_res = []
            for shard, stub in self._service_discovery.get_all_shards():
                all_res.extend(self._list_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index))
            all_res.sort(key=functools.cmp_to_key(compare_with_slash))
            return all_res
        else:
            shard, stub = self._service_discovery.get_leader(partition_key, partition_key)
            return self._list_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index)

    @staticmethod
    def _list_single_shard(shard, stub, min_key_inclusive: str, max_key_exclusive: str, use_index: str) -> list:
        res = stub.list(pb.ListRequest(shard=shard,
                                       start_inclusive=min_key_inclusive,
                                       end_exclusive=max_key_exclusive,
                                       secondary_index_name=use_index))

        keys = []
        for i in res:
            keys.extend(i.keys)
        return keys

    def range_scan(self,
                   min_key_inclusive: str,
                   max_key_exclusive: str,
                   partition_key: str = None,
                   use_index: str = None,
                   ) -> Iterator[tuple[str, str, oxia.defs.Version]]:
        # // RangeScan perform a scan for existing records with any keys within the specified range.
        # // Ordering in results channel is respected only if a [PartitionKey] option is passed (and the keys were
        # // inserted with that partition key).
        if partition_key is None:
            its = []
            for shard, stub in self._service_discovery.get_all_shards():
                its.append(self._range_scan_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index))
            return heapq.merge(*its, key=functools.cmp_to_key(compare_tuple_with_slash))
        else:
            shard, stub = self._service_discovery.get_leader(partition_key, partition_key)
            return self._range_scan_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index)

    @staticmethod
    def _range_scan_single_shard(shard, stub, min_key_inclusive: str, max_key_exclusive: str, use_index: str) \
                            -> Iterator[tuple[str, str, oxia.defs.Version]]:
        it = stub.range_scan(pb.RangeScanRequest(shard=shard,
                            start_inclusive=min_key_inclusive,
                            end_exclusive=max_key_exclusive,
                            secondary_index_name=use_index))

        for res in it:
            for x in res.records:
                yield x.key, x.value, _get_version(x.version)

    def get_sequence_updates(self, prefix_key: str, partition_key: str = None) -> oxia.defs.SequenceUpdates:
        # // GetSequenceUpdates allows to subscribe to the updates happening on a sequential key
        # // The channel will report the current latest sequence for a given key.
        # // Multiple updates can be collapsed into one single event with the
        # // highest sequence.
        if partition_key is None:
            raise oxia.ex.InvalidOptions("get_sequence_updates requires a partition_key")
        return SequenceUpdatesImpl(self._service_discovery, prefix_key, partition_key, lambda : self._closed)

    def get_notifications(self):
        """GetNotifications creates a new subscription to receive the notifications
           from Oxia for any change that is applied to the database"""
        return Notifications(self._service_discovery)

    def close(self):
        self._closed = True
        self._session_manager.close()
        self._connections.close()
        self._service_discovery.close()
