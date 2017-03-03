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
import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Optional;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.timestamp.TimestampService;

import feign.FeignException;

// We don't use EteSetup because we need much finer-grained control of the orchestration here, compared to the other
// ETE tests where the general idea is "set up all the containers, and fire".
public class TimeLockMigrationEteTest {
    // Docker Engine daemon only has limited access to the filesystem, if the user is using Docker-Machine
    // Thus root the temporary folder as a subdirectory of the user's home directory
    private static final TemporaryFolder TEMPORARY_FOLDER
            = new TemporaryFolder(new File(System.getProperty("user.home")));

    private static final DockerClientOrchestrationRule CLIENT_ORCHESTRATION_RULE
            = new DockerClientOrchestrationRule(TEMPORARY_FOLDER);

    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final Todo TODO_2 = ImmutableTodo.of("more stuff to do");
    private static final Todo TODO_3 = ImmutableTodo.of("even more stuff to do");
    private static final int DEFAULT_PORT = 3828;

    @ClassRule
    public static final RuleChain RULE_CHAIN = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(CLIENT_ORCHESTRATION_RULE);
    public static final String CONTAINER = "ete1";

    @BeforeClass
    public static void setUp() {
        CLIENT_ORCHESTRATION_RULE.updateProcessLivenessScript();
        waitUntil(serversAreReady());
    }

    @Test
    public void canAutomaticallyMigrateTimestampsAndFailsOnRestart() throws Exception {
        TodoResource todoClient = createClientFor(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));

        CLIENT_ORCHESTRATION_RULE.updateClientConfig(new File("docker/conf/atlasdb-ete.timelock.cassandra.yml"));

        CLIENT_ORCHESTRATION_RULE.restartAtlasClient();
        waitUntil(serversAreReady());
        todoClient.addTodo(TODO_2);
        assertThat(todoClient.getTodoList(), hasItems(TODO, TODO_2));

        // Did not expose a timestamp server!
        TimestampService timestampClient = createClientFor(TimestampService.class);
        try {
            timestampClient.getFreshTimestamp();
            fail();
        } catch (FeignException e) {
            // Expected
            assertThat(e.getMessage().contains("404"), is(true));
        }

        CLIENT_ORCHESTRATION_RULE.updateClientConfig(new File("docker/conf/atlasdb-ete.no-leader.cassandra.yml"));
        CLIENT_ORCHESTRATION_RULE.restartAtlasClient();
        waitForTransactionManagerCreationError();
        try {
            todoClient.addTodo(TODO_3);
            fail();
        } catch (FeignException e) {
            // Expected
        }
    }

    private static void waitUntil(Callable<Boolean> condition) {
        Awaitility.await()
                .ignoreExceptions()
                .atMost(com.jayway.awaitility.Duration.TWO_MINUTES)
                .pollInterval(com.jayway.awaitility.Duration.TWO_SECONDS)
                .until(condition);
    }

    private static void waitForTransactionManagerCreationError() {
        waitUntil(logsContainTransactionManagerCreationFailure());
    }

    private static Callable<Boolean> logsContainTransactionManagerCreationFailure() {
        return () -> {
            String serverLogs = CLIENT_ORCHESTRATION_RULE.getClientLogs();
            return serverLogs.contains("An error occurred while trying to create transaction manager.");
        };
    }

    private static Callable<Boolean> serversAreReady() {
        return () -> {
            createClientFor(TodoResource.class).isHealthy();
            return true;
        };
    }

    private static <T> T createClientFor(Class<T> clazz) {
        String uri = String.format("http://%s:%s", CONTAINER, DEFAULT_PORT);
        return AtlasDbHttpClients.createProxy(Optional.absent(), uri, clazz);
    }
}
