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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.containers.CassandraVersion;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.testing.DockerProxyRule;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import com.palantir.docker.compose.logging.LogDirectory;

// We don't use EteSetup because we need much finer-grained control of the orchestration here, compared to the other
// ETE tests where the general idea is "set up all the containers, and fire".
public class TimeLockMigrationEteTest {
    private static final DockerMachine DOCKER_MACHINE = DockerMachine.localMachine()
            .withEnvironment(CassandraVersion.getEnvironment())
            .build();

    private static final DockerComposeRule DOCKER_COMPOSE_RULE = DockerComposeRule.builder()
            .machine(DOCKER_MACHINE)
            .file("docker-compose.timelock-migration.cassandra.yml")
            .waitingForService("cassandra", HealthChecks.toHaveAllPortsOpen())
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(TimeLockMigrationEteTest.class.getSimpleName()))
            .addClusterWait(new ClusterWait(ClusterHealthCheck.nativeHealthChecks(), Duration.standardMinutes(1)))
            .build();
    private static final DockerProxyRule DOCKER_PROXY_RULE
            = new DockerProxyRule(DOCKER_COMPOSE_RULE.projectName(), TimeLockMigrationEteTest.class);

    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final int DEFAULT_PORT = 3828;

    @ClassRule
    public static final RuleChain RULE_CHAIN = RuleChain.outerRule(DOCKER_COMPOSE_RULE)
            .around(DOCKER_PROXY_RULE)
            .around(waitForServersToBeReady());

    @Test
    public void canAutomaticallyMigrateTimestamps() throws IOException, InterruptedException {
        TodoResource todoClient = createClientFor(TodoResource.class, "ete1", DEFAULT_PORT);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));

        DOCKER_COMPOSE_RULE.dockerCompose().stop(DOCKER_COMPOSE_RULE.containers().container("ete1"));

        todoClient.addTodo(TODO);
    }

    private static <T> T createClientFor(Class<T> clazz, String host, int port) {
        String uri = String.format("http://%s:%s", host, port);
        return AtlasDbHttpClients.createProxy(Optional.absent(), uri, clazz);
    }

    private static ExternalResource waitForServersToBeReady() {
        return new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                Awaitility.await()
                        .ignoreExceptions()
                        .atMost(com.jayway.awaitility.Duration.ONE_MINUTE)
                        .pollInterval(com.jayway.awaitility.Duration.ONE_SECOND)
                        .until(serversAreReady());
            }
        };
    }

    private static Callable<Boolean> serversAreReady() {
        return () -> {
            TodoResource todos = createClientFor(TodoResource.class, "ete1", DEFAULT_PORT);
            todos.isHealthy();
            return true;
        };
    }
}
