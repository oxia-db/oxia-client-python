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
import functools
from opcode import cmp_op


def _cmp(a: str, b: str) -> int:
    if a < b:
        return -1
    elif a > b:
        return +1
    else:
        return 0

def compare_tuple_with_slash(a, b) -> int:
    return compare_with_slash(a[0], b[0])

def compare_with_slash(a : str, b : str) -> int:
    while a and b:
        idx_a = a.find('/')
        idx_b = b.find('/')
        if idx_a < 0 and idx_b < 0:
            return _cmp(a, b)
        elif idx_a < 0:
            return -1
        elif idx_b < 0:
            return +1


        # At this point, both slices have '/'
        span_a = a[:idx_a]
        span_b = b[:idx_b]
        span_cmp = _cmp(span_a, span_b)
        if span_cmp != 0:
            return span_cmp

        a = a[idx_a+1:]
        b = b[idx_b+1:]

    if len(a) < len(b):
        return -1
    elif len(a) > 0:
        return +1
    else:
        return 0