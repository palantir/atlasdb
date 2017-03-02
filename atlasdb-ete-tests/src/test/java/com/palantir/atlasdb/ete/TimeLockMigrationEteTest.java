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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
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
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.execution.ImmutableDockerComposeExecArgument;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.timestamp.TimestampService;

import feign.FeignException;

// We don't use EteSetup because we need much finer-grained control of the orchestration here, compared to the other
// ETE tests where the general idea is "set up all the containers, and fire".
public class TimeLockMigrationEteTest {
    private static final Map<String, String> ENVIRONMENT = Maps.newHashMap();

    // Docker Engine daemon only has limited access to the filesystem, if the user is using Docker-Machine
    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File(System.getProperty("user.home")));

    private static DockerMachine dockerMachine;
    private static DockerComposeRule dockerComposeRule;
    private static DockerProxyRule dockerProxyRule;
    private static File configFile;

    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final Todo TODO_2 = ImmutableTodo.of("more stuff to do");
    private static final int DEFAULT_PORT = 3828;

    private static final ExternalResource SETUP_VARIABLES_RULE = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            try {
                configFile = TEMPORARY_FOLDER.newFile("atlasdb-ete.yml");
                FileUtils.writeStringToFile(configFile, FileUtils.readFileToString(
                        new File("docker/conf/atlasdb-ete.no-leader.cassandra.yml")));
                ENVIRONMENT.put("CONFIG_FILE_MOUNTPOINT", TEMPORARY_FOLDER.getRoot().getAbsolutePath());
                ENVIRONMENT.putAll(CassandraVersion.getEnvironment());
                System.out.println(ENVIRONMENT);

                dockerMachine = DockerMachine.localMachine()
                        .withEnvironment(ENVIRONMENT)
                        .build();

                dockerComposeRule = DockerComposeRule.builder()
                        .machine(dockerMachine)
                        .file("docker-compose.timelock-migration.cassandra.yml")
                        .waitingForService("cassandra", HealthChecks.toHaveAllPortsOpen())
                        .saveLogsTo(LogDirectory.circleAwareLogDirectory(TimeLockMigrationEteTest.class.getSimpleName()))
                        .addClusterWait(new ClusterWait(ClusterHealthCheck.nativeHealthChecks(), Duration.standardMinutes(100)))
                        .build();

                dockerProxyRule = new DockerProxyRule(dockerComposeRule.projectName(), TimeLockMigrationEteTest.class);

                dockerComposeRule.before();
                dockerProxyRule.before();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        protected void after() {
            dockerProxyRule.after();
            dockerComposeRule.after();
        }
    };

    @ClassRule
    public static final RuleChain RULE_CHAIN = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(SETUP_VARIABLES_RULE)
            .around(waitForServersToBeReady());

    @Test
    public void canAutomaticallyMigrateTimestamps()
            throws IOException, InterruptedException, NoSuchFieldException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {
        TodoResource todoClient = createClientFor(TodoResource.class, "ete1", DEFAULT_PORT);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));

        // change config!
        FileUtils.writeStringToFile(configFile, FileUtils.readFileToString(
                new File("docker/conf/atlasdb-ete.timelock.cassandra.yml")));

        System.out.println(dockerComposeRule.exec(DockerComposeExecOption.noOptions(),
                "ete1",
                ImmutableDockerComposeExecArgument.arguments("bash", "-c", "sed -i 's/ps $PID > \\/dev\\/null;/kill -0 $PID/' service/bin/init.sh")));
        System.out.println(dockerComposeRule.exec(DockerComposeExecOption.noOptions(),
                "ete1",
                ImmutableDockerComposeExecArgument.arguments("nohup", "service/bin/init.sh", "restart")));

        Awaitility.await()
                .ignoreExceptions()
                .atMost(com.jayway.awaitility.Duration.TEN_MINUTES)
                .pollInterval(com.jayway.awaitility.Duration.ONE_SECOND)
                .until(serversAreReady());

        todoClient.addTodo(TODO_2);
        assertThat(todoClient.getTodoList(), hasItems(TODO, TODO_2));

        TimestampService timestampClient = createClientFor(TimestampService.class, "ete1", DEFAULT_PORT);
        try {
            timestampClient.getFreshTimestamp();
            fail();
        } catch (FeignException e) {
            // Expected
            assertThat(e.getMessage().contains("404"), is(true));
        }
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
                        .atMost(com.jayway.awaitility.Duration.TEN_MINUTES)
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
