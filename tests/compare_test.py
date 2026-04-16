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

import unittest
from functools import cmp_to_key

from oxia.internal.compare import compare_with_slash as cs


def _natural_sign(a: str, b: str) -> int:
    """Sign of Python's default string comparison (codepoint-wise)."""
    return (a > b) - (a < b)


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

    def test_edge_bytes(self):
        # Null bytes sort below any ASCII letter inside a single segment.
        self.assertEqual(-1, cs("\x00", "a"))
        self.assertEqual(+1, cs("a", "\x00"))

        # Null byte as a segment prefix is still compared segment-wise.
        self.assertEqual(+1, cs("\x00/a", "/a"))
        self.assertEqual(-1, cs("/a", "\x00/a"))

        # Slash-only keys: fewer slashes sort before more slashes.
        self.assertEqual(-1, cs("/", "//"))
        self.assertEqual(-1, cs("//", "///"))
        self.assertEqual(+1, cs("///", "/"))

        # Empty segment between two slashes is meaningful: "a//b" != "a/b".
        self.assertEqual(+1, cs("a//b", "a/b"))
        self.assertEqual(-1, cs("a/b", "a//b"))

        # Keys that differ only after a long common prefix must still resolve
        # correctly (regression guard against buggy recursion bounds).
        long_prefix = "a" * 1000
        self.assertEqual(-1, cs(long_prefix + "/y", long_prefix + "/z"))
        self.assertEqual(+1, cs(long_prefix + "/z", long_prefix + "/y"))
        self.assertEqual(0, cs(long_prefix, long_prefix))

    def test_hierarchical_vs_natural(self):
        """Oxia's hierarchical sort deliberately differs from Python's codepoint-wise
        compare. Each case here is a regression guard that (1) the hierarchical
        rule gives the expected sign AND (2) natural ordering gives the opposite
        sign — so the test is proven to be exercising the divergence, not
        accidentally passing because the two orderings happen to agree."""
        divergent_cases = [
            # (a, b, expected hierarchical sign)
            ("a", "/", -1),                           # no-slash key sorts before slash key
            ("abcd", "a/c", -1),                      # same: no-slash < slash, even when 'b' > '/'
            ("b", "/", -1),
            ("/aaaa/a/a", "/bbbbbbbbbb", +1),         # deeper path with 'a' beats shallower 'b'
            ("/aaaa/a/a", "/aaaa/bbbbbbbbbb", +1),    # second segment: deeper 'a' beats shallower 'b'
            ("ab/c", "b", +1),                        # slash key sorts after no-slash key
        ]
        for a, b, expected_hier in divergent_cases:
            with self.subTest(a=a, b=b):
                hier = cs(a, b)
                self.assertEqual(expected_hier, hier,
                                 f"hierarchical cs({a!r}, {b!r}) = {hier}, expected {expected_hier}")
                nat = _natural_sign(a, b)
                self.assertNotEqual(hier, nat,
                                    f"this case is supposed to diverge from natural ordering, but "
                                    f"cs({a!r},{b!r})={hier} matches natural sign {nat}")

    def test_sorted_list(self):
        """Using `compare_with_slash` as a sort key must produce the correct
        hierarchical ordering over a list. The chosen keys intentionally produce
        a different order under Python's default `sorted()` — asserted at the
        end so a future refactor that makes cs() natural-equivalent would fail
        loudly here rather than silently pass."""
        keys = ["a", "/", "aa", "a/b", "ab", "ab/c", "b", "/a"]

        hierarchical = sorted(keys, key=cmp_to_key(cs))
        expected = ["a", "aa", "ab", "b", "/", "/a", "a/b", "ab/c"]
        self.assertEqual(expected, hierarchical)

        natural = sorted(keys)
        self.assertNotEqual(natural, hierarchical,
                            "natural and hierarchical orderings must differ for this key set; "
                            "otherwise the test is not exercising the hierarchical rule")


if __name__ == '__main__':
    unittest.main()
