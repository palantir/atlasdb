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
package com.palantir.atlasdb.sweep.progress;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractSweepProgressStoreTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final TableReference OTHER_TABLE = TableReference.createFromFullyQualifiedName("qwe.rty");

    private final KvsManager kvsManager;

    private static final SweepProgress PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {1, 2, 3})
            .startColumn(PtBytes.toBytes("unused"))
            .minimumSweptTimestamp(12345L)
            .staleValuesDeleted(10L)
            .cellTsPairsExamined(200L)
            .tableRef(TABLE)
            .timeInMillis(0L)
            .startTimeInMillis(0L)
            .build();
    private static final SweepProgress OTHER_PROGRESS = ImmutableSweepProgress.builder()
            .startRow(new byte[] {4, 5, 6})
            .startColumn(PtBytes.toBytes("unused"))
            .minimumSweptTimestamp(67890L)
            .staleValuesDeleted(11L)
            .cellTsPairsExamined(202L)
            .tableRef(OTHER_TABLE)
            .timeInMillis(1L)
            .startTimeInMillis(2L)
            .build();
    private static final SweepProgress SECOND_PROGRESS =
            ImmutableSweepProgress.copyOf(OTHER_PROGRESS).withTableRef(TABLE);

    private KeyValueService kvs;
    private SweepProgressStore progressStore;

    protected AbstractSweepProgressStoreTest(KvsManager kvsManager) {
        this.kvsManager = kvsManager;
    }

    @Before
    public void setup() {
        kvs = kvsManager.getDefaultKvs();
        progressStore = SweepProgressStoreImpl.create(kvs, false);
    }

    @After
    public void clearKvs() {
        kvs.truncateTable(AtlasDbConstants.SWEEP_PROGRESS_TABLE);
    }

    @Test
    public void testLoadEmpty() {
        assertThat(progressStore.loadProgress(TABLE)).isNotPresent();
    }

    @Test
    public void testSaveAndLoad() {
        progressStore.saveProgress(PROGRESS);
        assertThat(progressStore.loadProgress(TABLE)).contains(PROGRESS);
    }

    @Test
    public void testOtherTablesDoNotConflict() {
        progressStore.saveProgress(PROGRESS);
        assertThat(progressStore.loadProgress(OTHER_TABLE)).isNotPresent();

        progressStore.saveProgress(OTHER_PROGRESS);
        assertThat(progressStore.loadProgress(TABLE)).contains(PROGRESS);
    }

    @Test
    public void testOverwrite() {
        progressStore.saveProgress(PROGRESS);
        progressStore.saveProgress(SECOND_PROGRESS);
        assertThat(progressStore.loadProgress(TABLE)).contains(SECOND_PROGRESS);
    }

    @Test
    public void testClearOne() {
        progressStore.saveProgress(PROGRESS);
        progressStore.saveProgress(OTHER_PROGRESS);
        assertThat(progressStore.loadProgress(TABLE)).contains(PROGRESS);

        progressStore.clearProgress(TABLE);
        assertThat(progressStore.loadProgress(TABLE)).isNotPresent();
        assertThat(progressStore.loadProgress(OTHER_TABLE)).contains(OTHER_PROGRESS);
    }

    @Test
    public void testClearAndRewrite() {
        progressStore.saveProgress(PROGRESS);
        progressStore.clearProgress(TABLE);
        progressStore.saveProgress(SECOND_PROGRESS);

        assertThat(progressStore.loadProgress(TABLE)).contains(SECOND_PROGRESS);
    }

    @Test
    public void testClearTwice() {
        progressStore.saveProgress(PROGRESS);
        progressStore.clearProgress(TABLE);
        progressStore.clearProgress(TABLE);

        assertThat(progressStore.loadProgress(TABLE)).isNotPresent();
    }

    @Test
    public void testReadFromOldProgress() throws JsonProcessingException {
        byte[] progressBytes = SweepProgressStoreImpl.progressToBytes(PROGRESS);
        kvs.checkAndSet(CheckAndSetRequest.newCell(
                AtlasDbConstants.SWEEP_PROGRESS_TABLE, SweepProgressStoreImpl.LEGACY_CELL, progressBytes));

        // Enforce initialisation, which is where we expect the legacy value to be read.
        SweepProgressStore newProgressStore = SweepProgressStoreImpl.create(kvs, false);
        assertThat(newProgressStore.loadProgress(TABLE)).contains(PROGRESS);
    }

    @Test
    public void repeatedCreationDoesNotMoveProgressBackwards() throws JsonProcessingException {
        byte[] progressBytes = SweepProgressStoreImpl.progressToBytes(PROGRESS);
        kvs.checkAndSet(CheckAndSetRequest.newCell(
                AtlasDbConstants.SWEEP_PROGRESS_TABLE, SweepProgressStoreImpl.LEGACY_CELL, progressBytes));

        // Enforce initialisation, which is where we expect the legacy value to be read.
        SweepProgressStore newProgressStore = SweepProgressStoreImpl.create(kvs, false);
        assertThat(newProgressStore.loadProgress(TABLE)).contains(PROGRESS);
        newProgressStore.saveProgress(SECOND_PROGRESS);

        // This will fail if the legacy value is not removed by the initialisation of newProgressStore
        SweepProgressStore newerProgressStore = SweepProgressStoreImpl.create(kvs, false);
        assertThat(newerProgressStore.loadProgress(TABLE)).contains(SECOND_PROGRESS);
    }
}
