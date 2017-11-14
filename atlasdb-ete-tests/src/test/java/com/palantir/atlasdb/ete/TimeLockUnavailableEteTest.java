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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.common.exception.AtlasDbDependencyException;

public class TimeLockUnavailableEteTest extends EteSetup {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    private static final List<String> CLIENTS = ImmutableList.of("ete1");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            MultiCassandraTestSuite.class,
            "docker-compose.timelock.cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Test
    public void throwsDependencyUnavailableWhenTimeLockDown() throws IOException, InterruptedException {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));

        stopTimeLock();

        assertThatThrownBy(() -> todoClient.addTodo(TODO)).isInstanceOf(AtlasDbDependencyException.class);
    }

    private void stopTimeLock() throws IOException, InterruptedException {
        getContainer("timelock").kill();
    }

}
