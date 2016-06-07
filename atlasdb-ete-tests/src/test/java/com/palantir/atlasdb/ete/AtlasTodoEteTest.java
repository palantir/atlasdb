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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import static com.google.common.base.Throwables.propagate;

import javax.net.ssl.SSLSocketFactory;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.AtlasTodos;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class AtlasTodoEteTest {
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final int TODO_PORT = 3828;
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    public static DockerComposition dockerComposition = DockerComposition.of("docker-compose.yml")
            .waitingForService("todo1", toBeReady())
            .saveLogsTo("container-logs")
            .build();

    private static HealthCheck<Container> toBeReady() {
        return (container) -> {
            AtlasTodos todos = createTodoClientFor(container);

            return SuccessOrFailure.onResultOf(() -> {
                todos.isHealthy();
                return true;
            });
        };
    }

    public static Gradle gradle = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");

    @ClassRule
    public static RuleChain rules = RuleChain
            .outerRule(gradle)
            .around(dockerComposition);

    @Test public void
    shouldBeAbleToWriteAndListTodos() {
        AtlasTodos todoClient = createTodoClient();

        todoClient.addTodo(TODO);
        assertThat(todoClient.listTodos(), contains(TODO));
    }

    private AtlasTodos createTodoClient() {
        try {
            DockerPort port = dockerComposition.portOnContainerWithInternalMapping("todo1", TODO_PORT);
            return createTodoClientFor(port);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private static AtlasTodos createTodoClientFor(Container container) {
        return createTodoClientFor(container.portMappedInternallyTo(TODO_PORT));
    }

    private static AtlasTodos createTodoClientFor(DockerPort port) {
        String uri = port.inFormat("http://$HOST:$EXTERNAL_PORT");
        return AtlasDbHttpClients.createProxy(NO_SSL, uri, AtlasTodos.class);
    }
}
