/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.util.Random;

import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;

public class TodoEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList(), hasItem(TODO));
    }

    @Test
    public void shouldSweepStreamIndices() {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        storeFiveStreams(todoClient, 4321);

        SweepResults firstSweep = todoClient.sweepSnapshotIndices();
        assertThat(firstSweep.getCellTsPairsExamined(), equalTo(9L));
        assertThat(firstSweep.getStaleValuesDeleted(), equalTo(4L));

        SweepResults valueSweep = todoClient.sweepSnapshotValues();
        assertThat(valueSweep.getCellTsPairsExamined(), equalTo(9L));
        assertThat(valueSweep.getStaleValuesDeleted(), equalTo(4L));

        storeFiveStreams(todoClient, 7654321);

        SweepResults secondSweep = todoClient.sweepSnapshotIndices();
        assertThat(secondSweep.getCellTsPairsExamined(), equalTo(15L));
        assertThat(secondSweep.getStaleValuesDeleted(), equalTo(5L));

        SweepResults secondValueSweep = todoClient.sweepSnapshotValues();
        assertThat(secondValueSweep.getCellTsPairsExamined(), equalTo(177L)); // 6L + 19 * 8L + 19L));
        assertThat(secondValueSweep.getStaleValuesDeleted(), equalTo(1L + 19 * 4L));
    }

    private void storeFiveStreams(TodoResource todoClient, int streamSize) {
        Random random = new Random();
        byte[] bytes = new byte[streamSize];
        for (int i = 0; i < 5; i++) {
            random.nextBytes(bytes);
            todoClient.storeSnapshot(PtBytes.toString(bytes));
        }
    }
}
