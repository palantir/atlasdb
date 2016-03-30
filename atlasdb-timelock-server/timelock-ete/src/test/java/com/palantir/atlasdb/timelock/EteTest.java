/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock;

import java.util.function.Function;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;

public class EteTest {
    @ClassRule
    DockerComposition composition = DockerComposition.of("docker-compose.yml")
            .waitingForService("locktime_1", toBePingable())
            .waitingForService("locktime_2", toBePingable())
            .waitingForService("locktime_3", toBePingable())
            .build();

    private HealthCheck toBePingable() {
        return container -> container.portMappedInternallyTo(3828).isHttpResponding(onPingEndpoint());
    }

    private Function<DockerPort, String> onPingEndpoint() {
        return port -> "http://" + port.getIp() + ":" + port.getExternalPort();
    }

    @Test public void shouldBeAbleToGetTimestampsOffAClusterOfServices() {

    }
}
