/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.connection.DockerPort;

public final class StartupIndependenceUtils {
    private static final int CASSANDRA_PORT = 9160;

    private StartupIndependenceUtils() {
        // utility
    }

    public static void randomizeNamespace() throws IOException, InterruptedException {
        EteSetup.execCliCommandNoTty("sed -i 's/namespace: .*/namespace: "
                + UUID.randomUUID().toString().replace("-", "_")
                + "/' var/conf/atlasdb-ete.yml");
    }

    public static void killCassandraNodes(List<String> nodes) throws InterruptedException {
        runOnCassandraNodes(nodes, MultiCassandraUtils::killCassandraContainer);
        nodes.forEach(node -> {
            DockerPort containerPort = new DockerPort(node, CASSANDRA_PORT, CASSANDRA_PORT);
            assertSatisfiedWithin(10, () -> !containerPort.isListeningNow());
        });
    }

    public static void verifyCassandraIsSettled() throws IOException, InterruptedException {
        restartAtlasWithChecks();
        assertSatisfiedWithin(240, StartupIndependenceUtils::canPerformTransaction);
        randomizeNamespace();
    }

    public static void startCassandraNodes(List<String> nodes) throws InterruptedException {
        runOnCassandraNodes(nodes, MultiCassandraUtils::startCassandraContainer);
    }

    public static void restartAtlasWithChecks() throws InterruptedException, IOException {
        stopAtlasServerAndAssertSuccess();
        startAtlasServerAndAssertSuccess();
    }

    public static void assertSatisfiedWithin(long time, Callable<Boolean> condition) {
        Awaitility.waitAtMost(time, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(condition);
    }

    public static boolean canPerformTransaction() {
        try {
            addTodo();
            return true;
        } catch (AtlasDbRemoteException e) {
            return false;
        } catch (Exception e) {
            if (exceptionIsRetryableAndContainsMessage(e, "is not initialized yet")) {
                return false;
            }
            throw e;
        }
    }

    public static void assertNotInitializedExceptionIsThrownAndMappedCorrectly() {
        try {
            addTodo();
            fail("Expected to throw an exception");
        } catch (Exception e) {
            assertTrue(exceptionIsRetryableAndContainsMessage(e, "TransactionManager is not initialized yet"));
        }
    }

    private static void stopAtlasServerAndAssertSuccess() throws IOException, InterruptedException {
        EteSetup.execCliCommandNoTty("service/bin/init.sh stop");
        assertSatisfiedWithin(120, () -> !serverRunning());
    }

    private static void startAtlasServerAndAssertSuccess() throws IOException, InterruptedException {
        EteSetup.execCliCommandNoTty("service/bin/init.sh start");
        assertSatisfiedWithin(240, StartupIndependenceUtils::serverRunning);
    }

    private static boolean serverRunning() {
        try {
            canPerformTransaction();
            return true;
        } catch (Exception e) {
            if (exceptionIsRetryableAndContainsMessage(e, "Connection refused")) {
                return false;
            }
            throw e;
        }
    }

    private static boolean exceptionIsRetryableAndContainsMessage(Exception exc, String message) {
        // We shade Feign, so we can't rely on our client's RetryableException exactly matching ours.
        return exc.getClass().getName().contains("RetryableException") && exc.getMessage().contains(message);
    }

    private static void addTodo() {
        TodoResource todos = EteSetup.createClient(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }

    private static void runOnCassandraNodes(List<String> nodes, CassandraContainerOperator operator)
            throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(nodes.size());

        executorService.invokeAll(nodes.stream()
                .map(cassandraContainer -> Executors.callable(() -> operator.nodeOperation(cassandraContainer)))
                .collect(Collectors.toList()));
        executorService.shutdown();
    }

    private interface CassandraContainerOperator {
        void nodeOperation(String node);
    }
}
