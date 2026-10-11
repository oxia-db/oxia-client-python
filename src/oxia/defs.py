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

from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterator


class NotificationType(Enum):
    """The type of a notification event."""

    KEY_CREATED = 0  #: A record that didn't exist was created.
    KEY_MODIFIED = 1  #: An existing record was modified.
    KEY_DELETED = 2  #: A record was deleted.
    KEY_RANGE_DELETED = 3  #: A range of keys was deleted.


class Notification:
    """A single change event in the Oxia database."""

    def notification_type(self) -> NotificationType:
        """The type of the modification."""
        return self._type

    def key(self) -> str:
        """The key of the record that was changed."""
        return self._key

    def version_id(self) -> int:
        """The current version ID of the record, or ``0`` for a delete event."""
        return self._version_id

    def key_range_end(self) -> str:
        """For a L{KEY_RANGE_DELETED} notification, the end (exclusive) of
        the deleted key range. ``None`` for other notification types."""
        return self._key_range_end

    def __str__(self):
        return (f"Notification(key={self.key()!r}, type={self.notification_type()}, "
                f"version_id={self.version_id()}, key_range_end={self.key_range_end()!r})")


class SequenceUpdates(Iterator[str], ABC):
    """An iterator over sequential key updates.

    Yields the latest key each time the sequence advances. Multiple
    updates may be collapsed into a single event with the highest
    sequence. Call L{close} when done to release server-side resources.
    """

    def close(self):
        """Close the subscription and release resources."""
        pass


class Authentication(ABC):
    """Pluggable authentication for Oxia client RPCs.

    Implementations return a dict of metadata entries that will be
    attached to every outgoing RPC (e.g. ``{"authorization": "Bearer <token>"}``).
    ``generate_credentials`` is called on every RPC, so dynamic
    token-refresh schemes are supported by returning a fresh value each call.
    """

    @abstractmethod
    def generate_credentials(self) -> dict[str, str]:
        """Return the gRPC metadata to attach to each outgoing RPC.

        Keys are interpreted as gRPC metadata header names (will be
        lowercased by the transport). Values are arbitrary strings.
        """
        ...
