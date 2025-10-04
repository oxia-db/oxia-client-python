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

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

class OxiaContainer(DockerContainer):
    def __init__(self, shards=1):
        super().__init__(
            image='oxia/oxia:latest',
            command=f"oxia standalone --shards={shards}",
            ports=[6648,]
        )
        self.delay = self.waiting_for(LogMessageWaitStrategy("Serving Prometheus metrics"))

    def service_url(self):
        return f"localhost:{self.get_exposed_port(6648)}"