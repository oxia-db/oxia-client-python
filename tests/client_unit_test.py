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

"""Unit tests for client-level value coercion (bug #7)."""

import pytest


def test_coerce_value_str():
    """str values should be encoded to UTF-8 bytes."""
    from oxia.client import _coerce_value
    assert _coerce_value("hello") == b"hello"


def test_coerce_value_bytes():
    """bytes values should pass through unchanged."""
    from oxia.client import _coerce_value
    assert _coerce_value(b"hello") == b"hello"


def test_coerce_value_rejects_int():
    """Non-str/non-bytes values must raise TypeError.

    Currently FAILS because put() passes them through to protobuf
    where they error deep in serialization."""
    from oxia.client import _coerce_value
    with pytest.raises(TypeError, match="str or bytes"):
        _coerce_value(42)


def test_coerce_value_rejects_none():
    from oxia.client import _coerce_value
    with pytest.raises(TypeError, match="str or bytes"):
        _coerce_value(None)
