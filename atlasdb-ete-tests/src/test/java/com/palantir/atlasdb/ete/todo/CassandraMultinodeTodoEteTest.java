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
package com.palantir.atlasdb.ete.todo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

public class CassandraMultinodeTodoEteTest extends EteSetup {
    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            "cassandra-multinode",
            "docker-compose.multiple-cassandra.yml");

    private static final List<String> CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2", "cassandra3");

    private static final long MAX_CASSANDRA_NODE_DOWN_MILLIS = 60000;
    private static final long MAX_CASSANDRA_NODES_RUNNING_MILLIS = 3000;

    @Test
    public void shouldRunTransactionsWithAllCassandraNodesRunningWithoutUnacceptableDelay() throws InterruptedException {
        TodoResource clientToSingleNode = createClientToSingleNode(TodoResource.class);

        long transactionStartTime = System.currentTimeMillis();
        assertAddTodoTransactionWasSuccessful(clientToSingleNode);
        long transactionEndTime = System.currentTimeMillis();

        long transactionTimeWithAllNodesRunning = transactionEndTime - transactionStartTime;

        assertThat("transactionTimeWithAllNodesRunning",
                transactionTimeWithAllNodesRunning,
                is(lessThan(MAX_CASSANDRA_NODES_RUNNING_MILLIS)));
    }

    @Test
    public void shouldRunTransactionsAfterCassandraNodeIsShutDownWithoutUnacceptableDelay() throws InterruptedException {
        TodoResource clientToSingleNode = createClientToSingleNode(TodoResource.class);

        assertAddTodoTransactionWasSuccessful(clientToSingleNode);

        String cassandraNodeToKill = getRandomCassandraNodeToShutdown();
        killCassandraContainer(cassandraNodeToKill);

        long transactionStartTime = System.currentTimeMillis();
        assertAddTodoTransactionWasSuccessful(clientToSingleNode);
        long transactionEndTime = System.currentTimeMillis();

        long transactionTimeAfterNodeIsKilled = transactionEndTime - transactionStartTime;

        startCassandraContainer(cassandraNodeToKill);

        assertThat("transactionTimeAfterNodeIsKilled",
                transactionTimeAfterNodeIsKilled,
                is(lessThan(MAX_CASSANDRA_NODE_DOWN_MILLIS)));
    }

    private String getRandomCassandraNodeToShutdown() {
        int index = ThreadLocalRandom.current().nextInt(CASSANDRA_NODES.size());
        return CASSANDRA_NODES.get(index);
    }

    private void assertAddTodoTransactionWasSuccessful(TodoResource todoClient) {
        Todo todo = ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());

        todoClient.addTodo(todo);
        List<Todo> todoList = todoClient.getTodoList();

        assertThat(todoList, hasItem(todo));
    }
}
