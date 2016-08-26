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
import static org.hamcrest.Matchers.hasItem;

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
    public void runForever() throws InterruptedException {
        for (int i = 0 ; true ; i++) {
            Todo todo = ImmutableTodo.of(i + ": some stuff to do");

            shouldWork(todo);
            System.out.println(i + ": Success");
            Thread.sleep(1000);
        }
    }

    private void shouldWork(Todo todo) {
        TodoResource todoClient = createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(todo);

       // assertThat(todoClient.getTodoList(), hasItem(todo));
        if (todoClient.getTodoList().contains(todo)) {
            System.out.println("Success found " + todo);
        } else {
            System.out.println("Failure, could not find " + todo);
        }

    }

}
