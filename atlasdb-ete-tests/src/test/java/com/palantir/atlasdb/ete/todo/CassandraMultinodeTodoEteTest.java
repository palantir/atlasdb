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
import static org.hamcrest.core.IsCollectionContaining.hasItem;

import java.util.List;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.timestamp.TimestampService;

public class CassandraMultinodeTodoEteTest extends TodoEteTest {
    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            "cassandra-multinode",
            "docker-compose.multiple-cassandra.yml");

    @Override
    protected TimestampService createTimestampClient() {
        return createClientToMultipleNodes(TimestampService.class, "ete1");
    }

    @Test
    public void shouldWorkWithAnyContainerDown() throws InterruptedException {
        TodoResource clientToSingleNode = createClientToSingleNode(TodoResource.class);
        tryWithContainerDown("cassandra1", clientToSingleNode);
        tryWithContainerDown("cassandra2", clientToSingleNode);
        tryWithContainerDown("cassandra3", clientToSingleNode);
    }

    private void tryWithContainerDown(String container, TodoResource todoClient) {
        System.out.println("Running without container " + container);
        stopCassandraContainer(container);
        assertNewTodoWasAdded(todoClient);
        startCassandraContainer(container);
    }

    private void assertNewTodoWasAdded(TodoResource todoClient) {
        Todo todo = ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());

        todoClient.addTodo(todo);
        List<Todo> todoList = todoClient.getTodoList();

        assertThat(todoList, hasItem(todo));
    }
}
