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

"""Unit tests for the SequenceUpdates type contract."""

from oxia.defs import SequenceUpdates
from oxia.internal.sequence_updates import SequenceUpdatesImpl


def test_sequence_updates_impl_is_a_sequence_updates():
    """SequenceUpdatesImpl must be a subclass of the SequenceUpdates ABC.

    Currently FAILS because SequenceUpdatesImpl is an unrelated class
    that doesn't inherit from SequenceUpdates."""
    assert issubclass(SequenceUpdatesImpl, SequenceUpdates), \
        "SequenceUpdatesImpl should inherit from SequenceUpdates"
