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

import com.palantir.atlasdb.ete.utilities.EteExtension;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractTodoTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private final TodoResource todoClient = EteExtension.createClientToSingleNode(TodoResource.class);

    @AfterEach
    public void cleanupStreamTables() {
        todoClient.truncate();
    }

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList()).contains(TODO);
    }
}
