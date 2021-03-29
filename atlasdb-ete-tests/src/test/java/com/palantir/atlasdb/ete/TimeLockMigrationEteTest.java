/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.catchThrowable;

import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.timestamp.TimestampService;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.assertj.core.api.JUnitSoftAssertions;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

// We don't use EteSetup because we need much finer-grained control of the orchestration here, compared to the other
// ETE tests where the general idea is "set up all the containers, and fire".
public class TimeLockMigrationEteTest {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Gradle DOCKER_TASK = Gradle.ensureTaskHasRun(":timelock-server-distribution:dockerTag");

    // Docker Engine daemon only has limited access to the filesystem, if the user is using Docker-Machine
    // Thus ensure the temporary folder is a subdirectory of the user's home directory
    private static final TemporaryFolder TEMPORARY_FOLDER =
            new TemporaryFolder(new File(System.getProperty("user.home")));
    private static final DockerClientOrchestrationRule CLIENT_ORCHESTRATION_RULE =
            new DockerClientOrchestrationRule(TEMPORARY_FOLDER);

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    public static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);

    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final Todo TODO_2 = ImmutableTodo.of("more stuff to do");
    private static final Todo TODO_3 = ImmutableTodo.of("even more stuff to do");

    private static final int ETE_PORT = 3828;
    private static final String ETE_CONTAINER = "ete1";
    private static final String TIMELOCK_CONTAINER = "timelock";
    private static final int TIMELOCK_PORT = 8421;
    private static final String TEST_CLIENT = "atlasete";

    @ClassRule
    public static final RuleChain RULE_CHAIN = RuleChain.outerRule(GRADLE_PREPARE_TASK)
            .around(DOCKER_TASK)
            .around(TEMPORARY_FOLDER)
            .around(CLIENT_ORCHESTRATION_RULE);

    @Rule
    public final JUnitSoftAssertions softAssertions = new JUnitSoftAssertions();

    @BeforeClass
    public static void setUp() {
        CLIENT_ORCHESTRATION_RULE.updateProcessLivenessScript();
        waitUntil(serversAreReady());
    }

    @Test
    public void automaticallyMigratesTimestampsAndFailsOnRestart() throws Exception {
        TimestampService timestampClient = createEteClientFor(TimestampService.class);
        TodoResource todoClient = createEteClientFor(TodoResource.class);

        todoClient.addTodo(TODO);
        softAssertions
                .assertThat(todoClient.getTodoList())
                .as("contains one todo pre-migration")
                .contains(TODO);

        long embeddedTimestamp = timestampClient.getFreshTimestamp();
        softAssertions
                .assertThat(embeddedTimestamp)
                .as("can get a timestamp before migration")
                .isNotNull();

        upgradeAtlasClientToTimelock();

        assertTimeLockGivesHigherTimestampThan(embeddedTimestamp);

        softAssertions
                .assertThat(todoClient.getTodoList())
                .as("can still read todo after migration to TimeLock")
                .contains(TODO);

        todoClient.addTodo(TODO_2);
        softAssertions
                .assertThat(todoClient.getTodoList())
                .as("can add a new todo using TimeLock")
                .contains(TODO, TODO_2);

        assertNoLongerExposesEmbeddedTimestampService();

        downgradeAtlasClientFromTimelockWithoutMigration();

        assertCanNeitherReadNorWrite();
    }

    private void upgradeAtlasClientToTimelock() {
        CLIENT_ORCHESTRATION_RULE.updateClientConfig(DockerClientOrchestrationRule.TIMELOCK_CONFIG);
        CLIENT_ORCHESTRATION_RULE.restartAtlasClient();
        waitUntil(serversAreReady());
    }

    private void downgradeAtlasClientFromTimelockWithoutMigration() {
        CLIENT_ORCHESTRATION_RULE.updateClientConfig(DockerClientOrchestrationRule.EMBEDDED_CONFIG);
        CLIENT_ORCHESTRATION_RULE.restartAtlasClient();
        waitForTransactionManagerCreationError();
    }

    private void assertNoLongerExposesEmbeddedTimestampService() {
        TimestampService timestampClient = createEteClientFor(TimestampService.class);

        // as() is not compatible with assertThatThrownBy - see
        // http://joel-costigliola.github.io/assertj/core/api/org/assertj/core/api/Assertions.html
        softAssertions
                .assertThat(catchThrowable(timestampClient::getFreshTimestamp).getMessage())
                .contains("NOT_FOUND")
                .as("no longer exposes an embedded timestamp service");
    }

    private void assertCanNeitherReadNorWrite() {
        TodoResource todoClient = createEteClientFor(TodoResource.class);
        softAssertions
                .assertThat(catchThrowable(() -> todoClient.addTodo(TODO_3)))
                .as("cannot write using embedded service after migration to TimeLock")
                .hasMessageContaining("Dialogue transport failure");
        softAssertions
                .assertThat(catchThrowable(todoClient::getTodoList))
                .as("cannot read using embedded service after migration to TimeLock")
                .hasMessageContaining("Dialogue transport failure");
    }

    private void assertTimeLockGivesHigherTimestampThan(long timestamp) {
        long newTimestamp = createTimeLockTimestampClient().getFreshTimestamp();
        softAssertions
                .assertThat(newTimestamp)
                .as("timestamp was migrated to TimeLock")
                .isGreaterThan(timestamp);
    }

    private static void waitUntil(Callable<Boolean> condition) {
        Awaitility.await()
                .ignoreExceptions()
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(2))
                .until(condition);
    }

    private static void waitForTransactionManagerCreationError() {
        waitUntil(logsContainTransactionManagerCreationFailure());
    }

    // Note that this check is a bit hacky, as it depends on finding a particular log message
    private static Callable<Boolean> logsContainTransactionManagerCreationFailure() {
        return () -> {
            String serverLogs = CLIENT_ORCHESTRATION_RULE.getClientLogs();
            return serverLogs.contains("IllegalArgumentException trying to convert the stored value to a long.");
        };
    }

    private static Callable<Boolean> serversAreReady() {
        return () -> {
            createEteClientFor(TodoResource.class).isHealthy();
            return true;
        };
    }

    private static <T> T createEteClientFor(Class<T> clazz) {
        String uri = String.format("http://%s:%s", ETE_CONTAINER, ETE_PORT);
        return AtlasDbHttpClients.createProxy(
                Optional.of(TRUST_CONTEXT), uri, clazz, TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING);
    }

    private static TimestampService createTimeLockTimestampClient() {
        String uri = String.format("http://%s:%s/%s", TIMELOCK_CONTAINER, TIMELOCK_PORT, TEST_CLIENT);
        return AtlasDbHttpClients.createProxy(
                Optional.of(TRUST_CONTEXT),
                uri,
                TimestampService.class,
                TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING);
    }
}
