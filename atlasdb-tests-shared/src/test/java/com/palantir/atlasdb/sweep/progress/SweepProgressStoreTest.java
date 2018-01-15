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

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.SweepTestUtils;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.PTExecutors;

public class SweepProgressStoreTest {
    private ExecutorService exec;
    private TransactionManager txManager;
    private SweepProgressStore progressStore;

    private static final SweepProgress PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {1, 2, 3})
            .startColumn(PtBytes.toBytes("unused"))
            .minimumSweptTimestamp(12345L)
            .staleValuesDeleted(10L)
            .cellTsPairsExamined(200L)
            .tableRef(TableReference.createFromFullyQualifiedName("foo.bar"))
            .timeInMillis(0L)
            .startTimeInMillis(0L)
            .build();
    private static final SweepProgress OTHER_PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {4, 5, 6})
            .startColumn(PtBytes.toBytes("unused"))
            .minimumSweptTimestamp(67890L)
            .staleValuesDeleted(11L)
            .cellTsPairsExamined(202L)
            .tableRef(TableReference.createFromFullyQualifiedName("qwe.rty"))
            .timeInMillis(1L)
            .startTimeInMillis(2L)
            .build();

    @Before
    public void setup() {
        exec = PTExecutors.newCachedThreadPool();
        KeyValueService kvs = new InMemoryKeyValueService(false, exec);
        txManager = SweepTestUtils.setupTxManager(kvs);
        progressStore = SweepProgressStoreImpl.create(kvs, false);
    }

    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    @Test
    public void testLoadEmpty() {
        Assert.assertFalse(progressStore.loadProgress().isPresent());
    }

    @Test
    public void testSaveAndLoad() {
        progressStore.saveProgress(PROGRESS);
        Assert.assertEquals(Optional.of(PROGRESS), progressStore.loadProgress());
    }

    @Test
    public void testOverwrite() {
        progressStore.saveProgress(PROGRESS);
        progressStore.saveProgress(OTHER_PROGRESS);
        Assert.assertEquals(Optional.of(OTHER_PROGRESS), progressStore.loadProgress());
    }

    @Test
    public void testClear() {
        progressStore.saveProgress(PROGRESS);
        Assert.assertEquals(Optional.of(PROGRESS), progressStore.loadProgress());
        progressStore.clearProgress();
        Assert.assertFalse(progressStore.loadProgress().isPresent());
    }
}
