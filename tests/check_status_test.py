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

"""Unit tests for oxia.client._check_status."""

import pytest

import oxia.ex
from oxia.client import _check_status
from oxia.internal.proto.io.streamnative.oxia import proto as pb


def test_ok_does_not_raise():
    _check_status(pb.Status.OK)


def test_key_not_found_raises():
    with pytest.raises(oxia.ex.KeyNotFound):
        _check_status(pb.Status.KEY_NOT_FOUND)


def test_unexpected_version_id_raises():
    with pytest.raises(oxia.ex.UnexpectedVersionId):
        _check_status(pb.Status.UNEXPECTED_VERSION_ID)


def test_session_does_not_exist_raises():
    with pytest.raises(oxia.ex.SessionNotFound):
        _check_status(pb.Status.SESSION_DOES_NOT_EXIST)


def test_unknown_status_raises():
    """An unrecognised status must raise OxiaException rather than being
    silently treated as success.

    Currently FAILS because _check_status falls through for unknown values."""
    with pytest.raises(oxia.ex.OxiaException):
        _check_status(999)
