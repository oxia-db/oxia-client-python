#!/usr/bin/env bash
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

# Writes the version being released into Cargo.toml, which is where the
# package takes its version from (pyproject.toml declares it dynamic).
#
# Done in Python rather than with `sed -i`, which differs between the GNU
# sed on the Linux runners and the BSD sed on the macOS ones, and fails
# quietly on one of them. Nothing here may fail quietly: a version that is
# not stamped produces artifacts that are wrong rather than missing, and a
# wrong version uploaded to PyPI cannot be taken back.

set -euo pipefail

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
    echo "usage: $0 <version>" >&2
    exit 1
fi

python3 - "$1" <<'PYTHON'
import re
import sys

version = sys.argv[1]
with open("Cargo.toml") as manifest:
    text = manifest.read()

# The first `version` key of the file, which is the package's own: the
# dependencies state theirs inline, after their `name`.
stamped, count = re.subn(
    r'(?m)^version = "[^"]*"$', f'version = "{version}"', text, count=1
)
if count != 1:
    sys.exit("could not find the package version in Cargo.toml")

with open("Cargo.toml", "w") as manifest:
    manifest.write(stamped)

print(f"stamped version {version} into Cargo.toml")
PYTHON

grep -m1 '^version = ' Cargo.toml
