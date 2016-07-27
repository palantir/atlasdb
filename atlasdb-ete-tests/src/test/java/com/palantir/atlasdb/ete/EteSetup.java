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
package com.palantir.atlasdb.ete;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Collection;

import javax.net.ssl.SSLSocketFactory;

import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final int ETE_PORT = 3828;

    private static DockerComposition dockerComposition;

    protected <T> T createClientToSingleNode(Class<T> clazz) {
        return createClientFor(clazz, asPort("ete1"));
    }

    public <T> T createClientToMultipleNodes(Class<T> clazz, String... nodeNames) {
        Collection<String> uris = ImmutableList.copyOf(nodeNames).stream()
                .map(node -> asPort(node))
                .map(port -> port.inFormat("http://$HOST:$EXTERNAL_PORT"))
                .collect(toList());

        return AtlasDbHttpClients.createProxyWithFailover(NO_SSL, uris, clazz);
    }

    private DockerPort asPort(String node) {
        try {
            return dockerComposition.portOnContainerWithInternalMapping(node, ETE_PORT);
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    protected static RuleChain setupComposition(String composeFile) {
        dockerComposition = DockerComposition.of(composeFile)
                .waitingForService("ete1", toBeReady())
                .saveLogsTo("container-logs")
                .build();

        return RuleChain
                .outerRule(GRADLE_PREPARE_TASK)
                .around(dockerComposition);
    }

    private static <T> T createClientFor(Class<T> clazz, Container container) {
        return createClientFor(clazz, container.portMappedInternallyTo(ETE_PORT));
    }

    private static <T> T createClientFor(Class<T> clazz, DockerPort port) {
        String uri = port.inFormat("http://$HOST:$EXTERNAL_PORT");
        return AtlasDbHttpClients.createProxy(NO_SSL, uri, clazz);
    }

    private static HealthCheck<Container> toBeReady() {
        return (container) -> {
            TodoResource todos = createClientFor(TodoResource.class, container);

            return SuccessOrFailure.onResultOf(() -> {
                todos.isHealthy();
                return true;
            });
        };
    }
}
