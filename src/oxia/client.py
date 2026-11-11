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

"""
The public API, over the native client in `oxia._oxia`.

The native module does the work — connections, sharding, sessions, batching,
retries — and speaks in plain tuples. This module is what users import: it
holds the documented signatures, the option checks, and the classes those
tuples are turned back into.
"""

from abc import ABC
import datetime
import enum
from typing import Iterator

from oxia import _oxia
from oxia.defs import Notification, NotificationType
import oxia.ex


class ComparisonType(enum.IntEnum):
    """ComparisonType is an enumeration of the possible comparison types for the `get()` operation."""

    EQUAL = 0 #: Equal sets the Get() operation to compare the stored key for equality.
    FLOOR = 1 #: Floor option will make the get operation to search for the record whose key is the highest key <= to the supplied key.
    CEILING = 2 #: Ceiling option will make the get operation to search for the record whose key is the lowest key >= to the supplied key.
    LOWER = 3 #: Lower option will make the get operation to search for the record whose key is strictly < to the supplied key.
    HIGHER = 4 #: Higher option will make the get operation to search for the record whose key is strictly > to the supplied key.


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

        @return: The version ID.
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

        @return: The last modification timestamp.
        """
        return self._modified_timestamp

    def modifications_count(self) -> int:
        """
        Get the number of modifications to the record since it was last created.
        If the record gets deleted and recreated, the ModificationsCount will restart at 0.

        @return: The number of modifications.
        """
        return self._modifications_count

    def is_ephemeral(self) -> bool:
        """
        Check if the record is ephemeral.

        @return: True if the record is ephemeral, False otherwise.
        """
        return self._session_id is not None

    def session_id(self) -> int:
        """
        Get the session identifier for ephemeral records.
        For ephemeral records, this is the identifier of the session to which this record lifecycle
        is attached to. Non-ephemeral records will always report 0.

        @return: The session ID.
        """
        return self._session_id

    def client_identity(self) -> str:
        """
        Get the client identity for ephemeral records.
        For ephemeral records, this is the unique identity of the Oxia client that did last modify it.
        It will be empty for all non-ephemeral records.

        @return: The client identity.
        """
        return self._client_identity

    def __str__(self):
        return f"Version(version_id: {self.version_id()}, session_id: {self.session_id()}, modifications_count: {self.modifications_count()}, created_timestamp: {self.created_timestamp()}, modified_timestamp: {self.modified_timestamp()}, client_identity: {self.client_identity()})"


def _version(v) -> Version:
    """Rebuild a `Version` from the tuple the native client returns."""
    version = Version()
    (version._version_id,
     version._modifications_count,
     created_timestamp,
     modified_timestamp,
     version._session_id,
     version._client_identity) = v
    version._created_timestamp = datetime.datetime.fromtimestamp(created_timestamp / 1000.0)
    version._modified_timestamp = datetime.datetime.fromtimestamp(modified_timestamp / 1000.0)
    return version


def _record(r) -> tuple[str, bytes, Version]:
    key, value, version = r
    return key, value, _version(version)


def _notification(n) -> Notification:
    notification = Notification()
    (notification_type, notification._key,
     notification._version_id, notification._key_range_end) = n
    notification._type = NotificationType(notification_type)
    return notification


EXPECTED_RECORD_DOES_NOT_EXIST = -1
"""When doing a `put()`, this value can be used to indicate that the record should not exist."""


class SequenceUpdates(Iterator[str], ABC):
    """
    Represents an iterable sequence of key updates that can be closed when the caller is done.
    """

    def close(self):
        """
        Close the iterator and release any resources.
        """
        pass


class _Mapped:
    """An iterator over a native one, converting each item as it comes."""

    def __init__(self, inner, convert):
        self._inner = inner
        self._convert = convert

    def __iter__(self):
        return self

    def __next__(self):
        return self._convert(next(self._inner))

    def close(self):
        """Close the iterator and release any resources."""
        self._inner.close()


class Client:
    """Client is the main entry point to the Oxia Python client"""

    def __init__(self, service_address: str,
                 namespace: str = "default",
                 session_timeout_ms: int = 30_000,
                 client_identifier: str = None,
                 ):
        """
        Create a new Oxia client instance.

        Connects to the service, discovers the shards of the namespace and
        starts the thread the client runs on. The client is thread-safe:
        operations submitted from several threads run concurrently and are
        batched together.

        @param service_address: The Oxia service address to connect to.
        @param namespace: The namespace to which this client belongs. Default is "default".
        @param session_timeout_ms: Duration of the session timeout in milliseconds. Default is 30,000 ms.
        @param client_identifier: A unique identifier for the client. If None, a default identifier will
        be generated internally.
        """
        self._client = _oxia.Client(service_address, namespace, session_timeout_ms,
                                    client_identifier)

    def put(self, key: str, value: object,
            partition_key: str = None,
            expected_version_id: int = None,
            ephemeral: bool = False,
            sequence_keys_deltas: list[int] = None,
            secondary_indexes: dict[str, str] = None,
            ) -> (str, Version):
        """
        Associates a value with a key

        There are few options that can be passed to the Put operation:
           - The Put operation can be made conditional on that the record hasn't changed from
             a specific existing version by passing the `expected_version_id` option.
           - Client can assert that the record does not exist by passing [oxia.EXPECTED_RECORD_DOES_NOT_EXIST]
           - Client can create an ephemeral record with `ephemeral=True`

        @param key: the key to associate with the value
        @param value: the value to associate with the key
        @type value: str | bytes
        @param sequence_keys_deltas: a list of sequence keys to be used as deltas for the key. Oxia will create
        new unique keys atomically adding the sequence key deltas to the key.
        @param secondary_indexes: a dictionary of secondary indexes to be created for the record.
        @param partition_key: the partition key to use (instead of the actual record key)
        @param expected_version_id: the expected version id of the record to insert. If not specified, the put operation is not conditional.
        @param ephemeral: whether the record should be created as ephemeral. Ephemeral records are deleted when the session expires.
        @return: (actual_key, version)
        @raises oxia.ex.InvalidOptions: if the sequence_keys_deltas option is used with partition_key
        @raises oxia.ex.UnexpectedVersionId: if the expected version id does not match the current version id of the record
        """
        if sequence_keys_deltas:
            if not partition_key:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas can only be used with partition_key")
            if expected_version_id is not None:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas cannot be used with expected_version_id")

        if type(value) is str:
            value = bytes(value, encoding='utf-8')

        key, version = self._client.put(key, value,
                                        partition_key=partition_key,
                                        expected_version_id=expected_version_id,
                                        ephemeral=ephemeral,
                                        sequence_keys_deltas=sequence_keys_deltas,
                                        secondary_indexes=list(secondary_indexes.items())
                                        if secondary_indexes else None)
        return key, _version(version)

    def delete(self, key: str,
               partition_key: str = None,
               expected_version_id: int = None, ) -> bool:
        """
        Removes the key and its associated value from the data store.

        The delete operation can be made conditional on that the record hasn't changed from
        a specific existing version by passing the [ExpectedVersionId] option.

        @param key: the key to delete
        @param partition_key: the partition key to use (instead of the actual record key)
        @param expected_version_id: the expected version id of the record to delete. If not specified, the delete operation is not conditional.
        @return: true if the record was deleted, false otherwise
        @raises oxia.ex.UnexpectedVersionId: if the expected version id does not match the current version id of the record
        """
        return self._client.delete(key,
                                   partition_key=partition_key,
                                   expected_version_id=expected_version_id)

    def delete_range(self, min_key_inclusive: str, max_key_exclusive: str, partition_key: str = None):
        """
        Deletes any records with keys within the specified range.

        Note: Oxia uses a custom sorting order that treats `/` characters in a special way.
        Refer to this documentation for the specifics:
        https://oxia-db.github.io/docs/features/oxia-key-sorting

        @param min_key_inclusive: the minimum key to delete from
        @param max_key_exclusive: the maximum key to delete to (exclusive)
        @param partition_key: if set, the delete range will be applied to the shard with the specified partition key.
        """
        self._client.delete_range(min_key_inclusive, max_key_exclusive,
                                  partition_key=partition_key)

    def get(self, key: str,
            partition_key: str = None,
            comparison_type: ComparisonType = ComparisonType.EQUAL,
            include_value: bool = True,
            use_index: str = None,
            ) -> (str, str, Version):
        """
        Returns the value associated with the specified key.

        In addition to the value, a version object is also returned, with information about the record state.

        @param key: the key to retrieve
        @param partition_key: the partition key to use (instead of the actual record key)
        @param comparison_type: the comparison type to use
        @param include_value: whether to include the value in the result
        @param use_index: the name of the secondary index to use
        @return: (key, value, version)
        @raises oxia.ex.KeyNotFound: if the record does not exist
        """
        return _record(self._client.get(key,
                                        partition_key=partition_key,
                                        comparison_type=int(comparison_type),
                                        include_value=include_value,
                                        use_index=use_index))

    def list(self, min_key_inclusive: str, max_key_exclusive: str,
             partition_key: str = None,
             use_index: str = None,
             ) -> list[str]:
        """
        List all the keys within the specified range.

        Note: Oxia uses a custom sorting order that treats `/` characters in special way.
        Refer to this documentation for the specifics:
        https://oxia-db.github.io/docs/features/oxia-key-sorting

        @param min_key_inclusive: the minimum key to list from (inclusive).
        @param max_key_exclusive: the maximum key to list to (exclusive).
        @param partition_key: if set, the list will be applied to the shard with the specified partition key.
        @param use_index: if set, the list will be applied to the secondary index with the specified name.
        @return: the list of keys within the specified range.
        """
        return self._client.list(min_key_inclusive, max_key_exclusive,
                                 partition_key=partition_key,
                                 use_index=use_index)

    def range_scan(self,
                   min_key_inclusive: str,
                   max_key_exclusive: str,
                   partition_key: str = None,
                   use_index: str = None,
                   ) -> Iterator[tuple[str, str, Version]]:
        """
        perform a scan for existing records with any keys within the specified range.

        Note: Oxia uses a custom sorting order that treats `/` characters in special way.
        Refer to this documentation for the specifics:
        https://oxia-db.github.io/docs/features/oxia-key-sorting

        @param min_key_inclusive: the minimum key to list from (inclusive).
        @param max_key_exclusive: the maximum key to list to (exclusive).
        @param partition_key: if set, the range-scan will be applied to the shard with the specified partition key.
        @param use_index: if set, the range-scan will be applied to the secondary index with the specified name.
        @return: an iterator over the records within the specified range. Each record is a tuple of (key, value, version).
        """
        return _Mapped(self._client.range_scan(min_key_inclusive, max_key_exclusive,
                                               partition_key=partition_key,
                                               use_index=use_index),
                       _record)

    def get_sequence_updates(self, prefix_key: str, partition_key: str = None) -> SequenceUpdates:
        """
        Subscribe to the updates happening on a sequential key.

        Multiple updates can be collapsed into one single event with the highest sequence.

        @param prefix_key: the prefix for the sequential key.
        @param partition_key: the partition key to use (this is required)
        @return: a SequenceUpdates object that can be used to iterate over the updates. Should be closed when done.
        """
        if partition_key is None:
            raise oxia.ex.InvalidOptions("get_sequence_updates requires a partition_key")
        return self._client.sequence_updates(prefix_key, partition_key)

    def get_notifications(self) -> Iterator[Notification]:
        """
        Creates a new subscription to receive the notifications
        from Oxia for any change that is applied to the database

        @return: an iterator over the notifications. Each notification is a Notification object.
        Should be closed when done.
        @rtype: Iterator[oxia.Notification]
        """
        return _Mapped(self._client.notifications(), _notification)

    def close(self):
        """Close closes the client and all underlying connections"""
        self._client.close()
