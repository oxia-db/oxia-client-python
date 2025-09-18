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

import time

class Backoff:
    def __init__(self, start_delay=0.1):
        self.start_delay = start_delay
        self.delay = start_delay
        self.max_delay = 60.0

    def reset(self):
        self.delay = self.start_delay

    def wait_next(self):
        time.sleep(self.delay)
        self.delay = min(self.delay * 2, self.max_delay)
