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

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.todo.generated.TodoSchemaTableFactory;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;

public class TargetedSweepEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final TodoSchemaTableFactory FACTORY = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
    private static final TableReference INDEX_TABLE =
            FACTORY.getSnapshotsStreamIdxTable(null).getTableRef();
    private static final TableReference HASH_TABLE =
            FACTORY.getSnapshotsStreamHashAidxTable(null).getTableRef();
    private static final TableReference METADATA_TABLE =
            FACTORY.getSnapshotsStreamMetadataTable(null).getTableRef();
    private static final TableReference VALUES_TABLE =
            FACTORY.getSnapshotsStreamValueTable(null).getTableRef();

    private TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

    @After
    public void cleanupStreamTables() {
        todoClient.truncate();
    }

    @Test
    public void backgroundThoroughSweepDeletesOldVersion() throws InterruptedException {
        long ts = todoClient.addTodoWithIdAndReturnTimestamp(100L, TODO);
        assertThat(todoClient.doesNotExistBeforeTimestamp(100L, ts)).isFalse();

        todoClient.addTodoWithIdAndReturnTimestamp(100L, TODO);
        Awaitility.waitAtMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> todoClient.doesNotExistBeforeTimestamp(100L, ts));
    }

    @Test
    public void targetedSweepSmallStreamsTest() {
        // store 5 streams, marking 4 as unused
        StreamTestUtils.storeFiveStreams(todoClient, 20);
        // first iteration of sweep sweeps away the entries in the index table, and deletes the entries in the
        // other three tables, but does not sweep them yet
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4);
        assertDeletedAndSwept(4, 0, 0, 0);

        // store 5 more streams, marking 1 + 4 as unused
        StreamTestUtils.storeFiveStreams(todoClient, 20);
        // sweeps away the 5, 4, 4, 4 entries that were deleted, then deletes the rest
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(9, 9, 9, 9);
        assertDeletedAndSwept(9, 4, 4, 4);

        // sweeps away the last remaining entries
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(9, 9, 9, 9);
        assertDeletedAndSwept(9, 9, 9, 9);
    }

    @Test
    public void targetedSweepLargeStreamsTest() {
        // same as above, except the stream is bigger, so each uses 4 cells in the values table
        StreamTestUtils.storeFiveStreams(todoClient, 1500000);
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4 * 4);
        assertDeletedAndSwept(4, 0, 0, 0);

        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4 * 4);
        assertDeletedAndSwept(4, 4, 4, 4 * 4);
    }

    @Test
    public void targetedSweepCleansUpUnmarkedStreamsTest() {
        todoClient.storeUnmarkedSnapshot("snap");
        todoClient.storeUnmarkedSnapshot("crackle");
        todoClient.storeUnmarkedSnapshot("pop");
        todoClient.runIterationOfTargetedSweep();

        assertDeleted(0, 3, 3, 3);
    }

    private void assertDeleted(long idx, long hash, long meta, long val) {
        assertThat(todoClient.numberOfCellsDeleted(INDEX_TABLE)).isEqualTo(idx);
        assertThat(todoClient.numberOfCellsDeleted(HASH_TABLE)).isEqualTo(hash);
        assertThat(todoClient.numberOfCellsDeleted(METADATA_TABLE)).isEqualTo(meta);
        assertThat(todoClient.numberOfCellsDeleted(VALUES_TABLE)).isEqualTo(val);
    }

    private void assertDeletedAndSwept(long idx, long hash, long meta, long val) {
        assertThat(todoClient.numberOfCellsDeletedAndSwept(INDEX_TABLE)).isEqualTo(idx);
        assertThat(todoClient.numberOfCellsDeletedAndSwept(HASH_TABLE)).isEqualTo(hash);
        assertThat(todoClient.numberOfCellsDeletedAndSwept(METADATA_TABLE)).isEqualTo(meta);
        assertThat(todoClient.numberOfCellsDeletedAndSwept(VALUES_TABLE)).isEqualTo(val);
    }
}
