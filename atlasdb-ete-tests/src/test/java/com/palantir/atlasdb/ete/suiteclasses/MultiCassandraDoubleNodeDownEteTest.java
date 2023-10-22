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
package com.palantir.atlasdb.ete.suiteclasses;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.ete.utilities.EteSetup;
import com.palantir.atlasdb.ete.utilities.MultiCassandraUtils;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MultiCassandraDoubleNodeDownEteTest {
    private static final ImmutableSet<String> ALL_CASSANDRA_NODES =
            ImmutableSet.of("cassandra1", "cassandra2", "cassandra3");
    private static final ImmutableList<String> CASSANDRA_NODES_TO_KILL = ImmutableList.of("cassandra1", "cassandra2");

    @BeforeAll
    public static void shutdownCassandraNode() {
        CASSANDRA_NODES_TO_KILL.forEach(MultiCassandraUtils::killCassandraContainer);
    }

    @AfterAll
    public static void resetCassandraNode() {
        MultiCassandraUtils.resetCassandraCluster(ALL_CASSANDRA_NODES);
    }

    @Test
    public void shouldNotBeAbleToWriteWithTwoCassandraNodesDown() {
        assertThatThrownBy(() ->
                        EteSetup.createClientToSingleNode(TodoResource.class).addTodo(getUniqueTodo()))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void shouldNotBeAbleToReadWithTwoCassandraNodesDown() {
        assertThatThrownBy(() ->
                        EteSetup.createClientToSingleNode(TodoResource.class).getTodoList())
                .isInstanceOf(RuntimeException.class);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }
}
