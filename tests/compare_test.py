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

import unittest
from oxia.internal.compare import compare_with_slash as cs


class CompareTest(unittest.TestCase):

    def test_compare(self):
        self.assertEqual(0, cs("aaaaa", "aaaaa"))
        self.assertEqual(-1, cs("aaaaa", "zzzzz"))
        self.assertEqual(+1, cs("bbbbb", "aaaaa"))

        self.assertEqual(+1, cs("aaaaa", ""))
        self.assertEqual(-1, cs("", "aaaaa"))
        self.assertEqual(0, cs("", ""))

        self.assertEqual(-1, cs("aaaaa", "aaaaaaaaaaa"))
        self.assertEqual(+1, cs("aaaaaaaaaaa", "aaa"))

        self.assertEqual(-1, cs("a", "/"))
        self.assertEqual(+1, cs("/", "a"))

        self.assertEqual(-1, cs("/aaaa", "/bbbbb"))
        self.assertEqual(-1, cs("/aaaa/a", "/aaaa/b"))

        self.assertEqual(+1, cs("/aaaa/a/a", "/bbbbbbbbbb"))
        self.assertEqual(+1, cs("/aaaa/a/a", "/aaaa/bbbbbbbbbb"))

        self.assertEqual(+1, cs("/a/b/a/a/a", "/a/b/a/b"))


if __name__ == '__main__':
    unittest.main()
