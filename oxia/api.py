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

from enum import Enum

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
