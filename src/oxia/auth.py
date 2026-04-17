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

"""Authentication implementations for the Oxia client.

Usage::

    from oxia.auth import TokenAuthentication

    client = oxia.Client('oxia.example.com:6648', tls=True,
                         authentication=TokenAuthentication('my-token'))
"""

from typing import Callable, Union

from oxia.defs import Authentication


class TokenAuthentication(Authentication):
    """Bearer-token authentication.

    Emits an ``Authorization: Bearer <token>`` metadata header on every
    RPC. The token can be static or supplied by a callable for
    refresh-on-demand semantics.
    """

    _AUTHORIZATION_KEY = "authorization"
    _BEARER_PREFIX = "Bearer "

    def __init__(self, token: Union[str, Callable[[], str]]):
        """
        @param token: Either a static token string, or a zero-argument
            callable that returns the current token. The callable is
            invoked on every RPC.
        """
        if callable(token):
            self._token_supplier = token
        else:
            self._token_supplier = lambda: token

    def generate_credentials(self) -> dict[str, str]:
        return {self._AUTHORIZATION_KEY:
                self._BEARER_PREFIX + self._token_supplier()}
