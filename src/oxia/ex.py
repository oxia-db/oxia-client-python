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


"""
This module defines custom exception classes used within a system for
indicating various error conditions related to operations, options,
keys, versions, or sessions.

Custom exceptions in this module inherit from a base exception class
to provide a consistent hierarchy for error handling. They are used
to raise specific error cases that can be programmatically distinguished
and handled.

Classes:
    - OxiaException: The base class for all exceptions in this module.
    - InvalidOptions: Raised when invalid options are provided to a
      function or method.
    - KeyNotFound: Raised when an operation references a key that does
      not exist.
    - UnexpectedVersionId: Raised when an operation encounters an
      unexpected or incorrect version identifier.
    - SessionNotFound: Raised when an operation references a session
      that cannot be found.
"""

class OxiaException(Exception):
    """
    The base class for all the Oxia client exceptions.
    """
    pass

class InvalidOptions(OxiaException):
    """
    Raised when invalid options are provided to a function or method
    """
    pass

class KeyNotFound(OxiaException):
    """
    The key was not found during a get operation
    """
    pass

class UnexpectedVersionId(OxiaException):
    """
    When doing a put operation, the existing version does not match the
    expected version provided by the user.
    """
    pass

class SessionNotFound(OxiaException):
    """
    The session that the put request referred to is not alive.
    """
    pass
