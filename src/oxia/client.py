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
import functools
import heapq

from oxia.internal.compare import compare_tuple_with_slash, compare_with_slash
from oxia.internal.connection_pool import ConnectionPool
from oxia.internal.notifications import Notifications
from oxia.internal.sequence_updates import SequenceUpdatesImpl
from oxia.internal.sessions import SessionManager
from oxia.internal.service_discovery import ServiceDiscovery
from oxia.internal.proto.io.streamnative.oxia import proto as pb
import oxia.ex
import oxia.defs

import datetime
import enum
from typing import Iterator


def _coerce_value(value) -> bytes:
    """Convert a user-supplied value to ``bytes``.

    @param value: the value to convert.
    @return: the value as bytes.
    @raises TypeError: if *value* is not ``str`` or ``bytes``.
    """
    if isinstance(value, str):
        return value.encode('utf-8')
    elif isinstance(value, bytes):
        return value
    else:
        raise TypeError(
            f"value must be str or bytes, got {type(value).__name__}")


def _check_status(status: pb.Status):
    """Translate a protobuf response status into a Python exception.

    @param status: the status from an Oxia RPC response.
    @raises oxia.ex.KeyNotFound: if the key was not found.
    @raises oxia.ex.UnexpectedVersionId: if the version did not match.
    @raises oxia.ex.SessionNotFound: if the session does not exist.
    @raises oxia.ex.OxiaException: for any unrecognised status.
    """
    if status == pb.Status.OK:
        pass
    elif status == pb.Status.KEY_NOT_FOUND:
        raise oxia.ex.KeyNotFound()
    elif status == pb.Status.UNEXPECTED_VERSION_ID:
        raise oxia.ex.UnexpectedVersionId()
    elif status == pb.Status.SESSION_DOES_NOT_EXIST:
        raise oxia.ex.SessionNotFound()
    else:
        raise oxia.ex.OxiaException(f"unknown status: {status}")


class ComparisonType(enum.IntEnum):
    """Key comparison mode for the L{Client.get} operation."""

    EQUAL = int(pb.KeyComparisonType.EQUAL)      #: Exact match on the key.
    FLOOR = int(pb.KeyComparisonType.FLOOR)      #: Highest key <= the supplied key.
    CEILING = int(pb.KeyComparisonType.CEILING)  #: Lowest key >= the supplied key.
    LOWER = int(pb.KeyComparisonType.LOWER)      #: Highest key strictly < the supplied key.
    HIGHER = int(pb.KeyComparisonType.HIGHER)    #: Lowest key strictly > the supplied key.


def _get_version(pbv: pb.Version):
    v = Version()
    v._version_id = pbv.version_id
    v._modifications_count = pbv.modifications_count
    v._created_timestamp = datetime.datetime.fromtimestamp(pbv.created_timestamp / 1000.0)
    v._modified_timestamp = datetime.datetime.fromtimestamp(pbv.modified_timestamp / 1000.0)
    v._session_id = pbv.session_id
    v._client_identity = pbv.client_identity
    return v


class Version:
    """Metadata about the state of an Oxia record."""

    def version_id(self) -> int:
        """The monotonically increasing version identifier of the record."""
        return self._version_id

    def created_timestamp(self) -> datetime.datetime:
        """When the record was created. Resets if the record is deleted and
        re-created."""
        return self._created_timestamp

    def modified_timestamp(self) -> datetime.datetime:
        """When the record was last modified."""
        return self._modified_timestamp

    def modifications_count(self) -> int:
        """Number of modifications since the record was last created.
        Resets to 0 if the record is deleted and re-created."""
        return self._modifications_count

    def is_ephemeral(self) -> bool:
        """Whether the record is ephemeral (tied to a client session).
        Returns ``True`` if the record has an associated session ID,
        ``False`` otherwise."""
        return self._session_id is not None

    def session_id(self) -> int:
        """The session ID for ephemeral records, or ``None`` for
        non-ephemeral records."""
        return self._session_id

    def client_identity(self) -> str:
        """The identity of the client that last modified this ephemeral
        record, or ``None`` for non-ephemeral records."""
        return self._client_identity

    def __str__(self):
        return (f"Version(version_id={self.version_id()}, "
                f"session_id={self.session_id()}, "
                f"modifications_count={self.modifications_count()}, "
                f"created_timestamp={self.created_timestamp()}, "
                f"modified_timestamp={self.modified_timestamp()}, "
                f"client_identity={self.client_identity()!r})")


EXPECTED_RECORD_DOES_NOT_EXIST = -1
"""Pass as ``expected_version_id`` to L{Client.put} to assert the record
does not already exist."""


class Client:
    """Synchronous client for the Oxia service.

    Can be used as a context manager::

        with oxia.Client('localhost:6648') as client:
            client.put('/key', b'value')
    """

    def __init__(self, service_address: str,
                 namespace: str = "default",
                 session_timeout_ms: int = 30_000,
                 client_identifier: str = None,
                 tls: bool = False,
                 ):
        """Create a new Oxia client.

        @param service_address: Oxia service address (``host:port`` or
            ``tls://host:port``). Prefixing with ``tls://`` is
            equivalent to passing ``tls=True``.
        @param namespace: Oxia namespace. Default is ``"default"``.
        @param session_timeout_ms: Session timeout in milliseconds for
            ephemeral records. Default is 30 000 ms.
        @param client_identifier: Optional client identity string. If
            ``None``, a random UUID is generated.
        @param tls: If ``True``, use a TLS-encrypted gRPC channel. The
            system trust store is used to verify server certificates.
            Default is ``False`` (insecure channel).
        """
        self._closed = False
        if service_address.startswith("tls://"):
            service_address = service_address[len("tls://"):]
            tls = True
        self._connections = ConnectionPool(tls=tls)
        self._service_discovery = ServiceDiscovery(service_address, self._connections, namespace)
        self._session_manager = SessionManager(self._service_discovery, session_timeout_ms, client_identifier)

    def put(self, key: str, value: str | bytes,
            partition_key: str = None,
            expected_version_id: int = None,
            ephemeral: bool = False,
            sequence_keys_deltas: list[int] = None,
            secondary_indexes: dict[str, str] = None,
            ) -> tuple[str, Version]:
        """Associate a value with a key.

        @param key: The key to write.
        @param value: The value (``str`` is encoded to UTF-8).
        @param partition_key: Override shard routing with this key instead
            of the record key. Records sharing a partition key are
            co-located on the same shard.
        @param expected_version_id: If set, the put is conditional: it
            succeeds only if the current version matches. Pass
            L{EXPECTED_RECORD_DOES_NOT_EXIST} to assert the key is new.
        @param ephemeral: If ``True``, the record is tied to the client
            session and automatically deleted when the session ends.
        @param sequence_keys_deltas: Server-assigned sequential key
            suffixes. Requires ``partition_key``.
        @param secondary_indexes: Additional index entries
            (``{index_name: secondary_key}``).
        @return: ``(actual_key, version)``
        @raises oxia.ex.InvalidOptions: if options are incompatible.
        @raises oxia.ex.UnexpectedVersionId: if the version check fails.
        @raises oxia.ex.SessionNotFound: if the session for an ephemeral
            put no longer exists on the server.
        """
        shard, stub = self._service_discovery.get_leader(key, partition_key)

        if sequence_keys_deltas:
            if not partition_key:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas can only be used with partition_key")
            if expected_version_id is not None:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas cannot be used with expected_version_id")

        value = _coerce_value(value)

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
        """Delete a key and its value.

        @param key: The key to delete.
        @param partition_key: Override shard routing.
        @param expected_version_id: If set, the delete is conditional.
        @return: ``True`` if the record existed and was deleted,
            ``False`` if the key was not found.
        @raises oxia.ex.UnexpectedVersionId: if the version check fails.
        """
        shard, stub = self._service_discovery.get_leader(key, partition_key)

        dr = pb.DeleteRequest(key=key,
                              expected_version_id=expected_version_id)
        res = stub.write(pb.WriteRequest(shard=shard, deletes=[dr]))

        status = res.deletes[0].status  # We only have 1 request
        if status == pb.Status.KEY_NOT_FOUND:
            return False
        else:
            _check_status(status)
            return True

    def delete_range(self, min_key_inclusive: str, max_key_exclusive: str, partition_key: str = None):
        """Delete all records whose keys fall within ``[min, max)``.

        Keys are compared using Oxia's hierarchical sort order.
        See: U{https://oxia-db.github.io/docs/features/oxia-key-sorting}

        @param min_key_inclusive: Start of the range (inclusive).
        @param max_key_exclusive: End of the range (exclusive).
        @param partition_key: If set, only the shard owning this
            partition key is affected.
        """
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
            comparison_type: ComparisonType = ComparisonType.EQUAL,
            include_value: bool = True,
            use_index: str = None,
            ) -> tuple[str, bytes, Version]:
        """Retrieve the record for a key.

        @param key: The key (or secondary-index key when *use_index* is
            set) to look up.
        @param partition_key: Override shard routing.
        @param comparison_type: Key comparison mode. Default is
            L{ComparisonType.EQUAL}. Non-equal modes query across all
            shards when *partition_key* is not set.
        @param include_value: If ``False``, the returned value is
            ``None`` (useful for metadata-only reads).
        @param use_index: Name of a secondary index to query.
        @return: ``(key, value, version)`` where *key* is the primary
            key of the matched record and *value* is ``bytes`` (or
            ``None`` if *include_value* is ``False``).
        @raises oxia.ex.KeyNotFound: if no matching record exists.
        """
        if partition_key is None and \
                (comparison_type != ComparisonType.EQUAL
                 or use_index is not None):

            results = []
            for shard, stub in self._service_discovery.get_all_shards():
                try:
                    k, val, version = self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)
                    results.append((k, val, version))
                except oxia.ex.KeyNotFound:
                    pass
            if not results:
                raise oxia.ex.KeyNotFound()
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
            shard, stub = self._service_discovery.get_leader(key, partition_key)
            return self._get_single_shard(shard, stub, key, comparison_type, include_value, use_index)

    @staticmethod
    def _get_single_shard(shard: int, stub,
                          key: str,
                          comparison_type: ComparisonType,
                          include_value: bool,
                          use_index: str) -> tuple[str, bytes, Version]:
        gr = pb.GetRequest(key=key,
                           comparison_type=comparison_type,
                           include_value=include_value,
                           secondary_index_name=use_index)
        res = next(stub.read(pb.ReadRequest(shard=shard, gets=[gr])))
        get_res = res.gets[0]  # We only have 1 request
        _check_status(get_res.status)

        res_key = get_res.key if get_res.key is not None else key
        return res_key, get_res.value, _get_version(get_res.version)

    def list(self, min_key_inclusive: str, max_key_exclusive: str,
             partition_key: str = None,
             use_index: str = None,
             ) -> list[str]:
        """List keys in the range ``[min, max)``.

        Keys are returned in Oxia's hierarchical sort order.
        See: U{https://oxia-db.github.io/docs/features/oxia-key-sorting}

        @param min_key_inclusive: Start of the range (inclusive).
        @param max_key_exclusive: End of the range (exclusive).
        @param partition_key: If set, only query the shard owning this
            partition key.
        @param use_index: Name of a secondary index to query.
        @return: Sorted list of keys in the range, or ``[]`` if none.
        """
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
                   ) -> Iterator[tuple[str, bytes, Version]]:
        """Scan records in the range ``[min, max)``.

        Returns both keys and values, unlike L{list} which returns keys
        only.

        Keys are returned in Oxia's hierarchical sort order.
        See: U{https://oxia-db.github.io/docs/features/oxia-key-sorting}

        @param min_key_inclusive: Start of the range (inclusive).
        @param max_key_exclusive: End of the range (exclusive).
        @param partition_key: If set, only scan the shard owning this
            partition key.
        @param use_index: Name of a secondary index to query.
        @return: An iterator of ``(key, value, version)`` tuples.
        """
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
                            -> Iterator[tuple[str, bytes, Version]]:
        it = stub.range_scan(pb.RangeScanRequest(shard=shard,
                            start_inclusive=min_key_inclusive,
                            end_exclusive=max_key_exclusive,
                            secondary_index_name=use_index))

        for res in it:
            for x in res.records:
                yield x.key, x.value, _get_version(x.version)

    def get_sequence_updates(self, prefix_key: str, partition_key: str = None) -> oxia.defs.SequenceUpdates:
        """Subscribe to updates on a sequential key.

        Returns an iterator that yields the latest key each time the
        sequence advances. Call ``.close()`` on the returned object when
        done.

        @param prefix_key: The key prefix for the sequence.
        @param partition_key: Required. The partition key for shard
            routing.
        @return: A L{SequenceUpdates} iterator.
        @raises oxia.ex.InvalidOptions: if *partition_key* is not set.
        """
        if partition_key is None:
            raise oxia.ex.InvalidOptions("get_sequence_updates requires a partition_key")
        return SequenceUpdatesImpl(self._service_discovery, prefix_key, partition_key, lambda: self._closed)

    def get_notifications(self) -> Iterator[oxia.defs.Notification]:
        """Subscribe to change notifications for the entire database.

        Returns an iterator that yields L{Notification} objects for
        every create, modify, delete, or range-delete event. Multiple
        subscriptions can be active simultaneously.

        @return: An iterator of L{Notification} objects.
        """
        return Notifications(self._service_discovery)

    def close(self):
        """Close the client and release all underlying connections."""
        self._closed = True
        self._session_manager.close()
        self._connections.close()
        self._service_discovery.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
