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

from oxia import _oxia
from oxia.defs import SequenceUpdates


def test_sequence_updates_iterator_is_a_sequence_updates():
    """What get_sequence_updates() returns must satisfy the documented ABC.

    The iterator is the native type, which cannot inherit from a Python
    class, so oxia.client registers it as a virtual subclass instead."""
    assert issubclass(_oxia.SequenceUpdatesIterator, SequenceUpdates)
