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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

public class MultiCassandraPerformanceEteTest {
    private static final long MAX_CASSANDRA_NODE_DOWN_MILLIS = 30_000;
    private static final long MAX_CASSANDRA_NODES_RUNNING_MILLIS = 3000;

    @Before
    public void setUp() throws IOException, InterruptedException {
        MultiCassandraTestSuite.waitUntilAllNodesAreUp();
    }

    @Test
    public void shouldRunTransactionsFastEnoughWithAllCassandraNodesUp()
            throws InterruptedException, IOException {
        TodoResource clientToSingleNode = MultiCassandraTestSuite.createTodoResouce();

        assertAddTodoTransactionIsFastEnough(
                clientToSingleNode,
                "transactionTimeWithAllNodesRunning",
                MAX_CASSANDRA_NODES_RUNNING_MILLIS);
    }

    @Test
    public void shouldRunTransactionsFastEnoughWithOneCassandraNodeDown()
            throws InterruptedException {
        TodoResource clientToSingleNode = MultiCassandraTestSuite.createTodoResouce();

        assertAddTodoTransactionWasSuccessful(clientToSingleNode);

        String cassandraNodeToKill = MultiCassandraTestSuite.getRandomCassandraNode();
        MultiCassandraTestSuite.killCassandraContainer(cassandraNodeToKill);

        assertAddTodoTransactionIsFastEnough(
                clientToSingleNode,
                "transactionTimeAfterNodeIsKilled",
                MAX_CASSANDRA_NODE_DOWN_MILLIS);

        MultiCassandraTestSuite.startCassandraContainer(cassandraNodeToKill);
    }

    private void assertAddTodoTransactionIsFastEnough(TodoResource resource, String description, long timeLimit) {
        long transactionStartTime = System.currentTimeMillis();
        assertAddTodoTransactionWasSuccessful(resource);
        long transactionEndTime = System.currentTimeMillis();

        long transactionTimeAfterNodeIsKilled = transactionEndTime - transactionStartTime;

        assertThat(description,
                transactionTimeAfterNodeIsKilled,
                is(lessThan(timeLimit)));
    }

    private void assertAddTodoTransactionWasSuccessful(TodoResource resource) {
        Todo todo = ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());

        resource.addTodo(todo);
        List<Todo> todoList = resource.getTodoList();

        assertThat(todoList, hasItem(todo));
    }
}
