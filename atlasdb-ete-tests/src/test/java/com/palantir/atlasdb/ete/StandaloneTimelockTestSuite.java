/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.ete;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.timestamp.TimestampService;

public class StandaloneTimelockTestSuite {
    private static final String LOG_DIRECTORY = StandaloneTimelockTestSuite.class.getName();

    @ClassRule
    public static final DockerComposeRule DOCKER_COMPOSE_RULE = DockerComposeRule.builder()
            .file("docker-compose.timelock-solo.yml")
            .addClusterWait(new ClusterWait(
                    ClusterHealthCheck.nativeHealthChecks(),
                    Duration.standardSeconds(30)))
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(LOG_DIRECTORY))
            .build();

    @Test
    public void canGetTimestamps() {
        DockerPort port1 = DOCKER_COMPOSE_RULE.containers().container("timelock1").port(8421);
        DockerPort port2 = DOCKER_COMPOSE_RULE.containers().container("timelock1").port(8421);
        DockerPort port3 = DOCKER_COMPOSE_RULE.containers().container("timelock1").port(8421);

        List<String> uris = Stream.of(port1, port2, port3)
                .map(dockerPort -> String.format("http://%s:%s/%s", dockerPort.getIp(), dockerPort.getExternalPort(),
                        "test"))
                .collect(Collectors.toList());

        TimestampService timestampService = AtlasDbHttpClients.createProxyWithFailover(
                Optional.absent(),
                uris,
                TimestampService.class);


        System.out.println(timestampService.getFreshTimestamp());
    }
}
