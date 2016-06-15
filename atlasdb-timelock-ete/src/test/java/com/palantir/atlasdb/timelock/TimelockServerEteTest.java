/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock;

import static java.util.stream.Collectors.toList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.MultiServiceHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.timestamp.TimestampService;

public class TimelockServerEteTest {
    private static final int TIMELOCK_SERVER_PORT = 3828;
    private static final ImmutableList<String> TIMELOCK_NODES = ImmutableList.of("timelock1", "timelock2", "timelock3");

    public static DockerComposition dockerComposition = DockerComposition.of("docker-compose.yml")
            .waitingForServices(TIMELOCK_NODES, toHaveElectedALeader())
            .saveLogsTo("container-logs")
            .build();

    public static Gradle gradle = Gradle.ensureTaskHasRun(":atlasdb-timelock-server:prepareForEteTests");

    @ClassRule
    public static RuleChain rules = RuleChain
            .outerRule(gradle)
            .around(dockerComposition);

    @Test
    public void shouldBeAbleToGetTimestampsOffAClusterOfServices() throws Exception {
        TimestampService timestampService = createTimestampCluster(timelockPorts());

        long timestamp1 = timestampService.getFreshTimestamp();
        long timestamp2 = timestampService.getFreshTimestamp();

        assertThat(timestamp1, is(lessThan(timestamp2)));
    }

    private List<DockerPort> timelockPorts() {
        return TIMELOCK_NODES.stream()
                .map(this::timelockPort)
                .collect(toList());
    }

    private static TimestampService createTimestampCluster(List<DockerPort> timelockPorts) {
        List<String> endpoints = timelockPorts.stream()
                .map(port -> port.inFormat("http://$HOST:$EXTERNAL_PORT"))
                .collect(toList());

        return AtlasDbHttpClients.createProxyWithFailover(Optional.absent(), endpoints, TimestampService.class);
    }

    private DockerPort timelockPort(String container) {
        try {
            return dockerComposition.portOnContainerWithInternalMapping(container, TIMELOCK_SERVER_PORT);
        } catch (IOException | InterruptedException e) {
            throw propagate(e);
        }
    }

    private static MultiServiceHealthCheck toHaveElectedALeader() {
        return (containers) -> {
            List<DockerPort> timelockPorts = containers.stream()
                    .map(container -> container.portMappedInternallyTo(TIMELOCK_SERVER_PORT))
                    .collect(toList());

            TimestampService timestampService = createTimestampCluster(timelockPorts);

            return SuccessOrFailure.onResultOf(() -> {
                timestampService.getFreshTimestamp();
                return true;
            });
        };
    }

}
