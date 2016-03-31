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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.util.List;
import java.util.function.Function;

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.timestamp.TimestampService;

public class EteTest {
    public static final int TIMELOCK_SERVER_PORT = 3828;

    @ClassRule
    public static DockerComposition composition = DockerComposition.of("atlasdb-timelock-server/timelock-ete/docker-compose.yml")
            .waitingForService("timelock_1", toBePingable())
            .waitingForService("timelock_2", toBePingable())
            .waitingForService("timelock_3", toBePingable())
            .build();

    private static HealthCheck toBePingable() {
        return container -> container.portMappedInternallyTo(TIMELOCK_SERVER_PORT).isHttpResponding(onPingEndpoint());
    }

    private static Function<DockerPort, String> onPingEndpoint() {
        return port -> "http://" + port.getIp() + ":" + port.getExternalPort();
    }

    @Test public void shouldBeAbleToGetTimestampsOffAClusterOfServices() throws Exception {
        List<String> endpointUris = ImmutableList.of(onPingEndpoint().apply(composition.portOnContainerWithInternalMapping("timelock_1", TIMELOCK_SERVER_PORT)));
        TimestampService timestampService = AtlasDbHttpClients.createProxyWithFailover(Optional.absent(), endpointUris, TimestampService.class);

        long timestamp1 = timestampService.getFreshTimestamp();
        long timestamp2 = timestampService.getFreshTimestamp();

        assertThat(timestamp1, lessThan(timestamp2));
    }
}
