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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.timestamp.TimestampService;

public abstract class TodoEteTest extends EteSetup {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    protected abstract TimestampService createTimestampClient();

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        TodoResource todoClient = createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), contains(TODO));
    }

    @Test
    public void
    shouldExposeATimestampServer() {
        TimestampService timestampClient = createTimestampClient();

        assertThat(timestampClient.getFreshTimestamp(), is(not(nullValue())));
    }
}
