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

from enum import Enum, IntEnum
from abc import ABC
from oxia.internal.proto.io.streamnative import oxia as pb
import datetime
from typing import Iterator

class NotificationType(Enum):
    """NotificationType represents the type of the notification event."""

    """KeyCreated A record that didn't exist was created."""
    KEY_CREATED = 0

    """KeyModified An existing record was modified."""
    KEY_MODIFIED = 1

    """KeyDeleted A record was deleted."""
    KEY_DELETED = 2

    """KeyRangeDeleted A range of keys was deleted."""
    KEY_RANGE_DELETED = 3


class Notification:
    """Notification represents one change in the Oxia database."""

    def notification_type(self) -> NotificationType:
        """The type of the modification"""
        return self._type

    def key(self) -> str:
        """The Key of the record to which the notification is referring"""
        return self._key

    def version_id(self) -> int:
        """The current VersionId of the record, or -1 for a KeyDeleted event"""
        return self._version_id

    def key_range_end(self) -> str:
        """In case of a KeyRangeRangeDeleted notification, this would represent
	      the end (excluded) of the range of keys"""
        return self._key_range_end

    def __str__(self):
        return f"Notification(key: {self.key()}, type: {self.notification_type()}, version_id: {self.version_id()}, key_range_end: {self.key_range_end()})"

class ComparisonType(IntEnum):
    """ComparisonType is an enumeration of the possible comparison types for the `get()` operation."""

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
