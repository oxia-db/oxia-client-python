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
from abc import ABC
from typing import Iterator

from oxia.internal.compare import compare_tuple_with_slash, compare_with_slash
from oxia.internal.connection_pool import ConnectionPool
from oxia.internal.notifications import Notifications
from oxia.internal.sequence_updates import SequenceUpdatesImpl
from oxia.internal.sessions import SessionManager
from oxia.internal.service_discovery import ServiceDiscovery
import oxia.proto.io.streamnative.oxia.proto as pb

import datetime
from enum import IntEnum

def _check_status(status: pb.Status):
    if status == pb.Status.OK:
        pass
    elif status == pb.Status.KEY_NOT_FOUND:
        raise KeyNotFound()
    elif status == pb.Status.UNEXPECTED_VERSION_ID:
        raise UnexpectedVersionId()
    elif status == pb.Status.SESSION_DOES_NOT_EXIST:
        raise SessionNotFound()


def _get_version(pbv : pb.Version):
    v = Version()
    v._version_id = pbv.version_id
    v._modifications_count = pbv.modifications_count
    v._created_timestamp = datetime.datetime.fromtimestamp(pbv.created_timestamp/1000.0)
    v._modified_timestamp = datetime.datetime.fromtimestamp(pbv.modified_timestamp/1000.0)
    v._session_id = pbv.session_id
    v._client_identity = pbv.client_identity
    return v

class ComparisonType(IntEnum):

    """Equal sets the Get() operation to compare the stored key for equality."""
    EQUAL = pb.KeyComparisonType.EQUAL

    """Floor option will make the get operation to search for the record whose key is the 
       highest key <= to the supplied key."""
    FLOOR = pb.KeyComparisonType.FLOOR

    """Ceiling option will make the get operation to search for the record whose key is the 
       lowest key >= to the supplied key."""
    CEILING = pb.KeyComparisonType.CEILING

    """Lower option will make the get operation to search for the record whose key is strictly < to
       the supplied key."""
    LOWER = pb.KeyComparisonType.LOWER

    """Higher option will make the get operation to search for the record whose key is 
       strictly > to the supplied key."""
    HIGHER = pb.KeyComparisonType.HIGHER


class Version:
    """
    Version includes some information regarding the state of a record.
    """

    def version_id(self) -> int:
        """
        Retrieve the version ID.

        This method returns the version ID, which is an integer value representing
        the current version. It does not accept any parameters and simply outputs
        the version ID as an integer.

        :return: The version ID.
        :rtype: int
        """
        return self._version_id

    def created_timestamp(self) -> datetime.datetime:
        """
        The time when the record was last created
        (If the record gets deleted and recreated, it will have a new CreatedTimestamp value)
        """
        return self._created_timestamp

    def modified_timestamp(self) -> datetime.datetime:
        """
        Get the time when the record was last modified.

        :return: The last modification timestamp.
        :rtype: int
        """
        return self._modified_timestamp

    def modifications_count(self) -> int:
        """
        Get the number of modifications to the record since it was last created.
        If the record gets deleted and recreated, the ModificationsCount will restart at 0.

        :return: The number of modifications.
        :rtype: int
        """
        return self._modifications_count

    def is_ephemeral(self) -> bool:
        """
        Check if the record is ephemeral.

        :return: True if the record is ephemeral, False otherwise.
        :rtype: bool
        """
        return self._session_id is not None

    def session_id(self) -> int:
        """
        Get the session identifier for ephemeral records.
        For ephemeral records, this is the identifier of the session to which this record lifecycle
        is attached to. Non-ephemeral records will always report 0.

        :return: The session ID.
        :rtype: int
        """
        return self._session_id

    def client_identity(self) -> str:
        """
        Get the client identity for ephemeral records.
        For ephemeral records, this is the unique identity of the Oxia client that did last modify it.
        It will be empty for all non-ephemeral records.

        :return: The client identity.
        :rtype: str
        """
        return self._client_identity

    def __str__(self):
        return f"Version(version_id: {self.version_id()}, session_id: {self.session_id()}, modifications_count: {self.modifications_count()}, created_timestamp: {self.created_timestamp()}, modified_timestamp: {self.modified_timestamp()}, client_identity: {self.client_identity()})"

EXPECTED_RECORD_DOES_NOT_EXIST = -1


class SequenceUpdates(Iterator[str], ABC):
    def close(self):
        pass


class Client:
    def __init__(self, service_address: str,
                 namespace: str = "default",
                 session_timeout_ms: int = 30_000,
                 client_identifier: str = None,
                 ):
        self._closed = False
        self.connections = ConnectionPool()
        self.service_discovery = ServiceDiscovery(service_address, self.connections, namespace)
        self.session_manager = SessionManager(self.service_discovery, session_timeout_ms, client_identifier)

    def put(self, key: str, value: object,
            partition_key: str = None,
            expected_version_id: int = None,
            ephemeral: bool = False,
            sequence_keys_deltas: list[int] = None,
            secondary_indexes: dict[str, str] = None,
            ) -> (str, Version):
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
        shard, stub = self.service_discovery.get_leader(key, partition_key)

        if sequence_keys_deltas:
            if not partition_key:
                raise InvalidOptions("sequence_keys_deltas can only be used with partition_key")
            if expected_version_id is not None:
                raise InvalidOptions("sequence_keys_deltas cannot be used with expected_version_id")

        if type(value) is str:
            value = bytes(str(value), encoding='utf-8')

        pr = pb.PutRequest(key=key, value=value,
                           partition_key=partition_key,
                           expected_version_id=expected_version_id,
                           sequence_key_delta=sequence_keys_deltas,
                           )
        if ephemeral:
            session = self.session_manager.get_session(shard)
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
        shard, stub = self.service_discovery.get_leader(key, partition_key)

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
            for shard, stub in self.service_discovery.get_all_shards():
                self._delete_range_single_shard(min_key_inclusive, max_key_exclusive, shard, stub)
        else:
            shard, stub = self.service_discovery.get_leader(partition_key, partition_key)
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
            comparison_type: ComparisonType = ComparisonType.EQUAL,
            include_value: bool = True,
            use_index: str = None,
            ) -> (str, str, Version):
        # // Get returns the value associated with the specified key.
        # // In addition to the value, a version object is also returned, with information
        # // about the record state.
        # // Returns ErrorKeyNotFound if the record does not exist
        if partition_key is None and \
                (comparison_type != ComparisonType.EQUAL
                     or use_index is not None):

            results = []
            for shard, stub in self.service_discovery.get_all_shards():
                try:
                    k, val, version =  self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)
                    results.append((k, val, version))
                except KeyNotFound:
                    pass
            if not results: raise KeyNotFound
            results.sort(key=functools.cmp_to_key(compare_tuple_with_slash))

            if comparison_type == ComparisonType.EQUAL or \
                comparison_type == ComparisonType.CEILING or \
                comparison_type == ComparisonType.HIGHER:
                return results[0]
            elif comparison_type == ComparisonType.FLOOR or \
                    comparison_type == ComparisonType.LOWER:
                return results[-1]

        else:
            # Single shard get operation
            shard, stub = self.service_discovery.get_leader(key, partition_key)
            return self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)

    @staticmethod
    def _get_single_shard(shard: int, stub,
                          key: str,
                          comparison_type: ComparisonType,
                          include_value: bool,
                          use_index: str) -> (str, str, Version):
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
            for shard, stub in self.service_discovery.get_all_shards():
                all_res.extend(self._list_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index))
            all_res.sort(key=functools.cmp_to_key(compare_with_slash))
            return all_res
        else:
            shard, stub = self.service_discovery.get_leader(partition_key, partition_key)
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
                   ) -> Iterator[tuple[str, str, Version]]:
        # // RangeScan perform a scan for existing records with any keys within the specified range.
        # // Ordering in results channel is respected only if a [PartitionKey] option is passed (and the keys were
        # // inserted with that partition key).
        if partition_key is None:
            its = []
            for shard, stub in self.service_discovery.get_all_shards():
                its.append(self._range_scan_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index))
            return heapq.merge(*its, key=functools.cmp_to_key(compare_tuple_with_slash))
        else:
            shard, stub = self.service_discovery.get_leader(partition_key, partition_key)
            return self._range_scan_single_shard(shard, stub, min_key_inclusive, max_key_exclusive, use_index)

    @staticmethod
    def _range_scan_single_shard(shard, stub, min_key_inclusive: str, max_key_exclusive: str, use_index: str) \
                            -> Iterator[tuple[str, str, Version]]:
        it = stub.range_scan(pb.RangeScanRequest(shard=shard,
                            start_inclusive=min_key_inclusive,
                            end_exclusive=max_key_exclusive,
                            secondary_index_name=use_index))

        for res in it:
            for x in res.records:
                yield x.key, x.value, _get_version(x.version)

    def get_sequence_updates(self, prefix_key: str, partition_key: str = None) -> SequenceUpdates:
        # // GetSequenceUpdates allows to subscribe to the updates happening on a sequential key
        # // The channel will report the current latest sequence for a given key.
        # // Multiple updates can be collapsed into one single event with the
        # // highest sequence.
        if partition_key is None:
            raise InvalidOptions("get_sequence_updates requires a partition_key")
        return SequenceUpdatesImpl(self.service_discovery, prefix_key, partition_key, lambda : self._closed)

    def get_notifications(self):
        """GetNotifications creates a new subscription to receive the notifications
           from Oxia for any change that is applied to the database"""
        return Notifications(self.service_discovery)

    def close(self):
        self._closed = True
        self.session_manager.close()
        self.connections.close()
        self.service_discovery.close()


class OxiaException(Exception):
    pass

class InvalidOptions(OxiaException):
    pass

class KeyNotFound(OxiaException):
    pass

class UnexpectedVersionId(OxiaException):
    pass

class SessionNotFound(OxiaException):
    pass
