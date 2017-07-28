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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

import feign.RetryableException;

@RunWith(Parameterized.class)
public class MultiCassandraStartupOrderingEteTest {

    private final List<String> cassandraNodesToStartOrStop;

    public MultiCassandraStartupOrderingEteTest(List<String> cassandraNodesToStartorStop) {
        this.cassandraNodesToStartOrStop = cassandraNodesToStartorStop;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testCases() {
        Collection<Object[]> params = Lists.newArrayList();

        params.add(new Object[] {ImmutableList.of("cassandra1")});
        params.add(new Object[] {ImmutableList.of("cassandra1", "cassandra2")});
        params.add(new Object[] {ImmutableList.of("cassandra1", "cassandra2", "cassandra3")});
        return params;
    }

    @Test
    public void shouldBeAbleToStartUpIfCassandraIsDownAndRunTransactionsWhenCassandraIsUp()
            throws IOException, InterruptedException {
        Assert.assertTrue(canAddTodo());

        stopTheAtlasServer();
        runOnCassandraNodes(MultiCassandraTestSuite::killCassandraContainer);
        startTheAtlasServer();

        waitUntil(this::serverStarted);

        runOnCassandraNodes(MultiCassandraTestSuite::startCassandraContainer);

        waitUntil(this::canAddTodo);
        Assert.assertTrue(canAddTodo());
    }

    private void waitUntil(Callable<Boolean> condition) {
        Awaitility.waitAtMost(120, TimeUnit.SECONDS).pollInterval(10, TimeUnit.SECONDS).until(condition);
    }

    private boolean canAddTodo() {
        try {
            addATodo();
            return true;
        } catch (AtlasDbRemoteException e) {
            return false;
        }
    }

    private boolean serverStarted() {
        try {
            canAddTodo();
            return true;
        } catch (AtlasDbRemoteException e) {
            return true;
        } catch (RetryableException e) {
            return false;
        }
    }

    private void stopTheAtlasServer() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh stop");
    }

    private void startTheAtlasServer() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh start");
    }

    private void runOnCassandraNodes(CassandraContainerOperator operator)
            throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(cassandraNodesToStartOrStop.size());

        executorService.invokeAll(cassandraNodesToStartOrStop.stream()
                .map(cassandraContainer -> Executors.callable(() -> operator.nodeOperation(cassandraContainer)))
                .collect(Collectors.toList()));
    }

    private void addATodo() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }

    interface CassandraContainerOperator {
        void nodeOperation(String node);
    }
}
