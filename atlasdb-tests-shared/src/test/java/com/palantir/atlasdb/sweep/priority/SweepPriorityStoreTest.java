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
package com.palantir.atlasdb.sweep.priority;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.SweepTestUtils;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting2.tracing.Tracers;

public class SweepPriorityStoreTest {
    private ExecutorService exec;
    private TransactionManager txManager;
    private SweepPriorityStore priorityStore;

    @Before
    public void setup() {
        exec = Tracers.wrap(PTExecutors.newCachedThreadPool());
        KeyValueService kvs = new InMemoryKeyValueService(false, exec);
        txManager = SweepTestUtils.setupTxManager(kvs);
        priorityStore = new SweepPriorityStore(SweepTableFactory.of());
    }

    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    @Test
    public void testLoadEmpty() {
        Assert.assertTrue(txManager.runTaskReadOnly(
                tx -> priorityStore.loadOldPriorities(tx, tx.getTimestamp())).isEmpty());
        Assert.assertTrue(txManager.runTaskReadOnly(
                tx -> priorityStore.loadNewPriorities(tx)).isEmpty());
    }

    @Test
    public void testStoreAndLoadNew() throws Exception {
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    fullUpdate(0));
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("qwe.rty"),
                    fullUpdate(1));
            return null;
        });
        Assert.assertEquals(
                ImmutableSet.of(priority("foo.bar", 0), priority("qwe.rty", 1)),
                ImmutableSet.copyOf(txManager.runTaskReadOnly(priorityStore::loadNewPriorities)));
    }

    @Test
    public void testUpdateAndLoad() {
        long oldTs = txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    fullUpdate(0));
            return tx.getTimestamp();
        });
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    fullUpdate(1));
            return null;
        });
        Assert.assertEquals(
                ImmutableList.of(priority("foo.bar", 1)),
                txManager.runTaskReadOnly(priorityStore::loadNewPriorities));
        // TODO(gbonik): This currently fails because the getTimestamp override hack never worked.
        // We should create a ticket to track this.
        //Assert.assertEquals(
        //        ImmutableList.of(priority("foo.bar", 0)),
        //        txManager.runTaskReadOnly(tx -> priorityStore.loadOldPrioritites(tx, oldTs + 1)));
    }

    @Test
    public void testDelete() throws Exception {
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    fullUpdate(0));
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("qwe.rty"),
                    fullUpdate(1));
            return null;
        });
        assertThat(txManager.runTaskReadOnly(priorityStore::loadNewPriorities))
                .containsExactlyInAnyOrder(priority("foo.bar", 0), priority("qwe.rty", 1));
        txManager.runTaskWithRetry(tx -> {
            priorityStore.delete(tx, ImmutableList.of(TableReference.createFromFullyQualifiedName("foo.bar")));
            return null;
        });
        Assert.assertEquals(
                ImmutableList.of(priority("qwe.rty", 1)),
                txManager.runTaskReadOnly(priorityStore::loadNewPriorities));
    }

    @Test
    public void testPartialUpdate() {
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    fullUpdate(0));
            return null;
        });
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    ImmutableUpdateSweepPriority.builder()
                        .newStaleValuesDeleted(555)
                        .build());
            return null;
        });
        Assert.assertEquals(
                ImmutableList.of(ImmutableSweepPriority.builder()
                    .tableRef(TableReference.createFromFullyQualifiedName("foo.bar"))
                    .staleValuesDeleted(555)
                    .cellTsPairsExamined(10)
                    .lastSweepTimeMillis(123)
                    .minimumSweptTimestamp(456)
                    .writeCount(5)
                    .build()),
                txManager.runTaskReadOnly(priorityStore::loadNewPriorities));
    }

    @Test
    public void testLoadDefaultsIfFieldMissing() {
        txManager.runTaskWithRetry(tx -> {
            priorityStore.update(
                    tx,
                    TableReference.createFromFullyQualifiedName("foo.bar"),
                    ImmutableUpdateSweepPriority.builder()
                            .newStaleValuesDeleted(1)
                            .build());
            return null;
        });
        Assert.assertEquals(
                ImmutableList.of(ImmutableSweepPriority.builder()
                        .tableRef(TableReference.createFromFullyQualifiedName("foo.bar"))
                        .staleValuesDeleted(1)
                        .cellTsPairsExamined(0)
                        .lastSweepTimeMillis(OptionalLong.empty())
                        .minimumSweptTimestamp(Long.MIN_VALUE)
                        .writeCount(0)
                        .build()),
                txManager.runTaskReadOnly(priorityStore::loadNewPriorities));
    }

    private static UpdateSweepPriority fullUpdate(int increment) {
        return ImmutableUpdateSweepPriority.builder()
                .newStaleValuesDeleted(3 + increment)
                .newCellTsPairsExamined(10 + increment)
                .newLastSweepTimeMillis(123 + increment)
                .newMinimumSweptTimestamp(456 + increment)
                .newWriteCount(5 + increment)
                .build();
    }

    private static SweepPriority priority(String tableName, int number)  {
        return ImmutableSweepPriority.builder()
                .tableRef(TableReference.createFromFullyQualifiedName(tableName))
                .staleValuesDeleted(3 + number)
                .cellTsPairsExamined(10 + number)
                .lastSweepTimeMillis(123 + number)
                .minimumSweptTimestamp(456 + number)
                .writeCount(5 + number)
                .build();
    }

}
