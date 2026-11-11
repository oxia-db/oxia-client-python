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

"""The public API, over the native client in ``oxia._oxia``.

The native module does the work — connections, shard routing, sessions,
batching, retries — and speaks in plain tuples. This module is what users
import: it holds the documented signatures, the option checks, and the
classes those tuples are turned back into.
"""

# Annotations are not evaluated at runtime. `Client.list` shadows the builtin
# inside the class body, so a `list[int]` annotation on a sibling method would
# otherwise resolve to the method under PEP 649 and break `inspect.signature`.
from __future__ import annotations

import datetime
import enum
from typing import Iterator

from oxia import _oxia
from oxia.defs import Notification, NotificationType, SequenceUpdates
import oxia.ex

# `get_sequence_updates` returns the native iterator directly. Registering it
# makes it a virtual subclass of the documented ABC, so the type contract
# holds without a wrapper in front of every item.
SequenceUpdates.register(_oxia.SequenceUpdatesIterator)


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


class ComparisonType(enum.IntEnum):
    """Key comparison mode for the L{Client.get} operation."""

    EQUAL = 0    #: Exact match on the key.
    FLOOR = 1    #: Highest key <= the supplied key.
    CEILING = 2  #: Lowest key >= the supplied key.
    LOWER = 3    #: Highest key strictly < the supplied key.
    HIGHER = 4   #: Lowest key strictly > the supplied key.


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


def _version(v) -> Version:
    """Rebuild a L{Version} from the tuple the native client returns."""
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
"""Pass as ``expected_version_id`` to L{Client.put} to assert the record
does not already exist."""


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
        """Close the subscription and release resources."""
        self._inner.close()


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
                 request_timeout_ms: int = 30_000,
                 ):
        """Create a new Oxia client.

        Connects to the service, discovers the shards of the namespace and
        starts the thread the client runs on. The client is thread-safe:
        operations submitted from several threads run concurrently and are
        batched together.

        @param service_address: Oxia service address (``host:port``).
        @param namespace: Oxia namespace. Default is ``"default"``.
        @param session_timeout_ms: Session timeout in milliseconds for
            ephemeral records. Default is 30 000 ms.
        @param client_identifier: Optional client identity string. If
            ``None``, a random UUID is generated.
        @param request_timeout_ms: Deadline for each finite operation
            (``put``, ``delete``, ``get``, ``list``, ``range_scan``,
            session management), including the retries the client makes
            internally. Long-lived subscriptions (notifications, sequence
            updates, shard assignments) are not bounded. Default is
            30 000 ms. A timeout is raised as L{oxia.ex.OxiaException}.
        """
        self._client = _oxia.Client(service_address, namespace,
                                    session_timeout_ms, request_timeout_ms,
                                    client_identifier)

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
        if sequence_keys_deltas:
            if not partition_key:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas can only be used with partition_key")
            if expected_version_id is not None:
                raise oxia.ex.InvalidOptions("sequence_keys_deltas cannot be used with expected_version_id")

        key, version = self._client.put(key, _coerce_value(value),
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
        """Delete a key and its value.

        @param key: The key to delete.
        @param partition_key: Override shard routing.
        @param expected_version_id: If set, the delete is conditional.
        @return: ``True`` if the record existed and was deleted,
            ``False`` if the key was not found.
        @raises oxia.ex.UnexpectedVersionId: if the version check fails.
        """
        return self._client.delete(key,
                                   partition_key=partition_key,
                                   expected_version_id=expected_version_id)

    def delete_range(self, min_key_inclusive: str, max_key_exclusive: str, partition_key: str = None):
        """Delete all records whose keys fall within ``[min, max)``.

        Keys are compared using Oxia's hierarchical sort order.
        See: U{https://oxia-db.github.io/docs/features/oxia-key-sorting}

        @param min_key_inclusive: Start of the range (inclusive).
        @param max_key_exclusive: End of the range (exclusive).
        @param partition_key: If set, only the shard owning this
            partition key is affected.
        """
        self._client.delete_range(min_key_inclusive, max_key_exclusive,
                                  partition_key=partition_key)

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
        return _record(self._client.get(key,
                                        partition_key=partition_key,
                                        comparison_type=int(comparison_type),
                                        include_value=include_value,
                                        use_index=use_index))

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
        return self._client.list(min_key_inclusive, max_key_exclusive,
                                 partition_key=partition_key,
                                 use_index=use_index)

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
        return _Mapped(self._client.range_scan(min_key_inclusive, max_key_exclusive,
                                               partition_key=partition_key,
                                               use_index=use_index),
                       _record)

    def get_sequence_updates(self, prefix_key: str, partition_key: str = None) -> SequenceUpdates:
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
        return self._client.sequence_updates(prefix_key, partition_key)

    def get_notifications(self) -> Iterator[Notification]:
        """Subscribe to change notifications for the entire database.

        Returns an iterator that yields L{Notification} objects for
        every create, modify, delete, or range-delete event. Multiple
        subscriptions can be active simultaneously. Call ``.close()`` on
        the returned object when done.

        @return: An iterator of L{Notification} objects.
        """
        return _Mapped(self._client.notifications(), _notification)

    def close(self):
        """Close the client and release all underlying connections."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
