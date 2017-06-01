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
package com.palantir.atlasdb.sweep.progress;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.SweepTestUtils;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting1.tracing.Tracers;

public class SweepProgressStoreTest {
    private ExecutorService exec;
    private TransactionManager txManager;
    private SweepProgressStore progressStore;

    private static final SweepProgress PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {1, 2, 3})
            .minimumSweptTimestamp(12345L)
            .staleValuesDeleted(10L)
            .cellTsPairsExamined(200L)
            .tableRef(TableReference.createFromFullyQualifiedName("foo.bar"))
            .build();
    private static final SweepProgress OTHER_PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {4, 5, 6})
            .minimumSweptTimestamp(67890L)
            .staleValuesDeleted(11L)
            .cellTsPairsExamined(202L)
            .tableRef(TableReference.createFromFullyQualifiedName("qwe.rty"))
            .build();

    @Before
    public void setup() {
        exec = Tracers.wrap(PTExecutors.newCachedThreadPool());
        KeyValueService kvs = new InMemoryKeyValueService(false, exec);
        txManager = SweepTestUtils.setupTxManager(kvs);
        progressStore = new SweepProgressStore(kvs, SweepTableFactory.of());
    }

    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    @Test
    public void testLoadEmpty() {
        Assert.assertFalse(txManager.runTaskReadOnly(progressStore::loadProgress).isPresent());
    }

    @Test
    public void testSaveAndLoad() {
        txManager.runTaskWithRetry(tx -> {
            progressStore.saveProgress(tx, PROGRESS);
            return null;
        });
        Assert.assertEquals(Optional.of(PROGRESS), txManager.runTaskReadOnly(progressStore::loadProgress));
    }

    @Test
    public void testOverwrite() {
        txManager.runTaskWithRetry(tx -> {
            progressStore.saveProgress(tx, PROGRESS);
            return null;
        });
        txManager.runTaskWithRetry(tx -> {
            progressStore.saveProgress(tx, OTHER_PROGRESS);
            return null;
        });
        Assert.assertEquals(Optional.of(OTHER_PROGRESS), txManager.runTaskReadOnly(progressStore::loadProgress));
    }

    @Test
    public void testClear() {
        txManager.runTaskWithRetry(tx -> {
            progressStore.saveProgress(tx, PROGRESS);
            return null;
        });
        Assert.assertTrue(txManager.runTaskReadOnly(progressStore::loadProgress).isPresent());
        progressStore.clearProgress();
        Assert.assertFalse(txManager.runTaskReadOnly(progressStore::loadProgress).isPresent());
    }

}
