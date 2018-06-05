/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.todo.generated.TodoSchemaTableFactory;

public class TargetedSweepEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final TodoSchemaTableFactory FACTORY = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
    private static final TableReference INDEX_TABLE = FACTORY.getSnapshotsStreamIdxTable(null).getTableRef();
    private static final TableReference HASH_TABLE = FACTORY.getSnapshotsStreamHashAidxTable(null).getTableRef();
    private static final TableReference METADATA_TABLE = FACTORY.getSnapshotsStreamMetadataTable(null).getTableRef();
    private static final TableReference VALUES_TABLE = FACTORY.getSnapshotsStreamValueTable(null).getTableRef();

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
        Awaitility.waitAtMost(2, TimeUnit.MINUTES).pollInterval(2, TimeUnit.SECONDS)
                .until(() -> todoClient.doesNotExistBeforeTimestamp(100L, ts));
    }

    @Test
    public void targetedSweepSmallStreamsTest() {
        storeFiveStreams(20);
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4);
        assertDeletedAndSwept(4, 0, 0, 0);

        storeFiveStreams(20);
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(9, 9, 9, 9);
        assertDeletedAndSwept(9, 4, 4, 4);

        todoClient.runIterationOfTargetedSweep();
        assertDeleted(9, 9, 9, 9);
        assertDeletedAndSwept(9, 9, 9, 9);
    }

    @Test
    public void targetedSweepLargeStreamsTest() {
        storeFiveStreams(1500000);
        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4 * 4);
        assertDeletedAndSwept(4, 0, 0, 0);

        todoClient.runIterationOfTargetedSweep();
        assertDeleted(4, 4, 4, 4 * 4);
        assertDeletedAndSwept(4, 4, 4, 4 * 4);
    }

    private void storeFiveStreams(int streamSize) {
        Random random = new Random();
        byte[] bytes = new byte[streamSize];
        for (int i = 0; i < 5; i++) {
            random.nextBytes(bytes);
            todoClient.storeSnapshot(PtBytes.toString(bytes));
        }
    }

    private void assertDeleted(long idx, long hash, long meta, long val) {
        Assert.assertThat(todoClient.numberOfCellsDeleted(INDEX_TABLE), equalTo(idx));
        Assert.assertThat(todoClient.numberOfCellsDeleted(HASH_TABLE), equalTo(hash));
        Assert.assertThat(todoClient.numberOfCellsDeleted(METADATA_TABLE), equalTo(meta));
        Assert.assertThat(todoClient.numberOfCellsDeleted(VALUES_TABLE), equalTo(val));
    }

    private void assertDeletedAndSwept(long idx, long hash, long meta, long val) {
        Assert.assertThat(todoClient.numberOfCellsDeletedAndSwept(INDEX_TABLE), equalTo(idx));
        Assert.assertThat(todoClient.numberOfCellsDeletedAndSwept(HASH_TABLE), equalTo(hash));
        Assert.assertThat(todoClient.numberOfCellsDeletedAndSwept(METADATA_TABLE), equalTo(meta));
        Assert.assertThat(todoClient.numberOfCellsDeletedAndSwept(VALUES_TABLE), equalTo(val));
    }
}
