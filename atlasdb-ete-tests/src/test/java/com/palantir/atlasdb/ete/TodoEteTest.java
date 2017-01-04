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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.timestamp.TimestampService;

public class TodoEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));
    }

    @Test
    public void shouldExposeATimestampServer() {
        Set<String> set = Sets.newHashSet();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            set.add("abcpwejrf39mn0g3[k12" + i);
            System.err.println("3p0"
                    + "q129jm cn1103pftjmg2q -----" + i);
        }
        System.out.println(set);

        TimestampService timestampClient = EteSetup.createClientToAllNodes(TimestampService.class);

        assertThat(timestampClient.getFreshTimestamp(), is(not(nullValue())));
    }
}
