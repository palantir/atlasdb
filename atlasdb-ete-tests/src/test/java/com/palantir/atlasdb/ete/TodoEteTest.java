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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Test;

import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

// TODO (gbrova) does this still need to extend EteSetup?
public class TodoEteTest extends EteSetup {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        TodoResource todoClient = createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), contains(TODO));
    }
}
