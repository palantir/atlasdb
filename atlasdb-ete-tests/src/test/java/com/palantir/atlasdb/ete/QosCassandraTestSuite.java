/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

public class QosCassandraTestSuite extends EteSetup {
    private static final List<String> CLIENTS = ImmutableList.of("ete1");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            QosCassandraTestSuite.class,
            "docker-compose.qos.cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Test
    public void shouldFailIfWritingTooManyBytes() {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
        // Doesn't throw the first time as we estimate low values
        todoClient.addTodo(getTodoOfSize(100_000));
        assertThatThrownBy(() -> todoClient.addTodo(getTodoOfSize(100_000)))
                .isInstanceOf(RuntimeException.class)
                .as("Cannot write 100_000 bytes the second time as write limit of 1000 was consumed and the burst isnt enough either.");
    }

    @Test
    public void shouldFailIfReadingTooManyBytes() throws InterruptedException {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
        IntStream.range(0, 30).forEach(i -> todoClient.addTodo(getTodoOfSize(1_000)));

        assertThatThrownBy(todoClient::getTodoList)
                .isInstanceOf(RuntimeException.class)
                .as("Cant read 30_000 bytes in 10 batches i.e. 3000 bytes multiple times when limit is 1000.");
    }

    @After
    public void after() {
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    }

    private Todo getTodoOfSize(int size) {
        return ImmutableTodo.of(String.join("", Collections.nCopies(size, "a")));
    }
}
