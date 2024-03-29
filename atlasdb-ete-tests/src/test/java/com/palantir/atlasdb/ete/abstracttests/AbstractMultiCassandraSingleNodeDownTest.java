/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.ete.abstracttests;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.ete.utilities.EteExtension;
import com.palantir.atlasdb.ete.utilities.MultiCassandraUtils;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.flake.FlakeRetryTest;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractMultiCassandraSingleNodeDownTest {
    private static final ImmutableSet<String> ALL_CASSANDRA_NODES =
            ImmutableSet.of("cassandra1", "cassandra2", "cassandra3");
    private static final String CASSANDRA_NODE_TO_KILL = "cassandra1";

    @BeforeAll
    public static void shutdownCassandraNode() {
        MultiCassandraUtils.killCassandraContainer(CASSANDRA_NODE_TO_KILL);
    }

    @AfterAll
    public static void resetCassandraNodes() {
        MultiCassandraUtils.resetCassandraCluster(ALL_CASSANDRA_NODES);
    }

    // In some cases we obtain a TTransportException from Cassandra, probably because we don't wait enough?
    @FlakeRetryTest
    public void shouldBeAbleToWriteWithOneCassandraNodeDown() {
        TodoResource todos = EteExtension.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    @FlakeRetryTest
    public void shouldBeAbleToReadWithOneCassandraNodeDown() {
        TodoResource todos = EteExtension.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
        assertThat(todos.getTodoList()).contains(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }
}
