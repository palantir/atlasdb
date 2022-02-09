/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.BackupAndRestoreResource;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import java.net.SocketTimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TodoEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");

    private final TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    @After
    public void cleanupStreamTables() {
        todoClient.truncate();
    }

    @Test
    public void shouldBeAbleToWriteAndListTodos() {
        todoClient.addTodo(TODO);
        assertThat(todoClient.getTodoList()).contains(TODO);
    }

    // TODO(gs): split into own test class
    @Test
    public void canPrepareBackup() {
        todoClient.addTodo(TODO);
        BackupAndRestoreResource backupResource = EteSetup.createClientToSingleNode(BackupAndRestoreResource.class);
        // TODO(gs): constants?
        Namespace namespace = Namespace.of("atlasete");
        backupResource.prepareBackup(ImmutableSet.of(namespace));

        // verify we persisted the immutable timestamp to disk
        assertThat(backupResource.getStoredImmutableTimestamp(namespace)).isNotEmpty();
    }

    @Test
    @ShouldRetry(
            numAttempts = 10,
            retryableExceptions = {SocketTimeoutException.class})
    public void shouldSweepStreamIndices() {
        // Stores five small streams, each of which fits into a single block
        // Each time a stream is stored, the previous stream (if any) is deleted
        // This is represented by a delete in the index table.
        StreamTestUtils.storeFiveStreams(todoClient, 4321);

        SweepResults firstSweep = todoClient.sweepSnapshotIndices();

        // The index table contains 5 rows, 4 with two cells (a reference and a delete), and one with just the reference
        // Sweep examines these nine cells, and deletes the old references.
        assertThat(firstSweep.getCellTsPairsExamined()).isEqualTo(9L);
        assertThat(firstSweep.getStaleValuesDeleted()).isEqualTo(4L);

        // When sweep deletes cells from the index table, a cleanup task is run to propagate the deletes to the value
        // table. The value table thus contains 4 rows with two cells (stream data + a delete), and 1 with just the data
        // Sweep examines these nine cells, and deletes the old values.
        SweepResults valueSweep = todoClient.sweepSnapshotValues();
        assertThat(valueSweep.getCellTsPairsExamined()).isEqualTo(9L);
        assertThat(valueSweep.getStaleValuesDeleted()).isEqualTo(4L);

        // Stores five larger streams, which take 3 blocks each
        StreamTestUtils.storeFiveStreams(todoClient, 1254321);

        // The index table now contains ten rows:
        // 4 rows with a sentinel and a delete (sweep sees the delete, but doesn't sweep it - 4 examined, 0 deleted)
        // 5 rows with a reference and a delete (sweep sees both, and sweeps the references - 10 examined, 5 deleted)
        // 1 row (the most recent) with just a reference - 1 examined, 0 deleted
        // So sweep examines 15 cells in total, and deletes 5
        SweepResults secondSweep = todoClient.sweepSnapshotIndices();
        assertThat(secondSweep.getCellTsPairsExamined()).isEqualTo(15L);
        assertThat(secondSweep.getStaleValuesDeleted()).isEqualTo(5L);

        // The deletes of the second index-sweep propagate deletes to *each* of the blocks in the value table
        // The value table now contains ten rows:
        // 4 rows with a sentinel and a delete (sweep sees the delete, but doesn't sweep it - 4 E, 0 D)
        // 1 single-block row with one block and a delete (sweep sees both, and sweeps the value - 2 E, 1 D)
        // 4 rows with 3 blocks and 3 deletes (6 E, 3 D per row = 24 E, 12 D)
        // 1 row (the most recent) with 3 blocks (3 E, 0 D)
        // The total cells examined is thus 4 + 2 + 4*2*3 + 3 = 33
        // And the total cells deleted is 1 + 4*3 = 13
        SweepResults secondValueSweep = todoClient.sweepSnapshotValues();
        assertThat(secondValueSweep.getCellTsPairsExamined()).isEqualTo(33L);
        assertThat(secondValueSweep.getStaleValuesDeleted()).isEqualTo(13L);
    }
}
