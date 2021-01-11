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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiCassandraDoubleNodeDownEteTest {
    private static final ImmutableSet<String> ALL_CASSANDRA_NODES =
            ImmutableSet.of("cassandra1", "cassandra2", "cassandra3");
    private static final ImmutableList<String> CASSANDRA_NODES_TO_KILL = ImmutableList.of("cassandra1", "cassandra2");

    @BeforeClass
    public static void shutdownCassandraNode() {
        CASSANDRA_NODES_TO_KILL.forEach(MultiCassandraUtils::killCassandraContainer);
    }

    @AfterClass
    public static void resetCassandraNode() {
        MultiCassandraUtils.resetCassandraCluster(ALL_CASSANDRA_NODES);
    }

    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToWriteWithTwoCassandraNodesDown() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        todos.addTodo(getUniqueTodo());
    }

    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToReadWithTwoCassandraNodesDown() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        todos.getTodoList();
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }
}
