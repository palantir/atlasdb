/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;

@ShouldRetry // In some cases we obtain a TTransportException from Cassandra, probably because we don't wait enough?
public class MultiCassandraSingleNodeDownEteTest {
    private static final String CASSANDRA_NODE_TO_KILL = "cassandra1";

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    @BeforeClass
    public static void shutdownCassandraNode() {
        MultiCassandraUtils.killCassandraContainer(CASSANDRA_NODE_TO_KILL);
    }

    @AfterClass
    public static void startupCassandraNode() {
        MultiCassandraUtils.startCassandraContainer(CASSANDRA_NODE_TO_KILL);
    }

    @Test
    public void shouldBeAbleToWriteWithOneCassandraNodeDown() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    @Test
    public void shouldBeAbleToReadWithOneCassandraNodeDown() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
        assertThat(todos.getTodoList()).contains(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }
}
