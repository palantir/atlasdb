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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SweepProgressStoreImpl implements SweepProgressStore {

    private final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_SweepProgressStore {
        @Override
        public SweepProgressStoreImpl delegate() {
            checkInitialized();
            return SweepProgressStoreImpl.this;
        }

        @Override
        protected void tryInitialize() {
            SweepProgressStoreImpl.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "SweepProgressStore";
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SweepProgressStoreImpl.class);

    private final KeyValueService kvs;
    private final InitializingWrapper wrapper = new InitializingWrapper();

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new AfterburnerModule());

    private static final String ROW_AND_COLUMN_NAME = "s";
    private static final byte[] ROW_AND_COLUMN_NAME_BYTES = PtBytes.toCachedBytes(ROW_AND_COLUMN_NAME);

    @VisibleForTesting
    static final Cell LEGACY_CELL = Cell.create(ROW_AND_COLUMN_NAME_BYTES, ROW_AND_COLUMN_NAME_BYTES);

    private static final byte[] FINISHED_TABLE = PtBytes.toBytes("Table finished");

    private static final TableMetadata SWEEP_PROGRESS_METADATA = TableMetadata.internal()
            .singleRowComponent("dummy", ValueType.STRING)
            .singleNamedColumn(ROW_AND_COLUMN_NAME, "sweep_progress", ValueType.BLOB)
            .build();

    private SweepProgressStoreImpl(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public static SweepProgressStore create(KeyValueService kvs, boolean initializeAsync) {
        SweepProgressStoreImpl progressStore = new SweepProgressStoreImpl(kvs);
        progressStore.wrapper.initialize(initializeAsync);
        return progressStore.wrapper.isInitialized() ? progressStore : progressStore.wrapper;
    }

    private Optional<SweepProgress> loadLegacyProgress() {
        Map<Cell, Value> entry = kvs.get(AtlasDbConstants.SWEEP_PROGRESS_TABLE, ImmutableMap.of(LEGACY_CELL, 1L));
        return hydrateProgress(entry);
    }

    @Override
    public Optional<SweepProgress> loadProgress(TableReference tableRef) {
        Map<Cell, Value> entry = getStoredProgress(tableRef);
        return hydrateProgress(entry);
    }

    protected Map<Cell, Value> getStoredProgress(TableReference tableRef) {
        return kvs.get(AtlasDbConstants.SWEEP_PROGRESS_TABLE, ImmutableMap.of(getCell(tableRef), 1L));
    }

    @Override
    public void saveProgress(SweepProgress newProgress) {
        Cell cell = getCell(newProgress.tableRef());
        Map<Cell, Value> entry = getStoredProgress(newProgress.tableRef());

        try {
            kvs.checkAndSet(casProgressRequest(cell, entry, newProgress));
        } catch (Exception e) {
            log.warn(
                    "Exception trying to persist sweep progress. The intermediate progress might not have been "
                            + "persisted. This should not cause sweep issues unless the problem persists.",
                    e);
        }
    }

    private CheckAndSetRequest casProgressRequest(Cell cell, Map<Cell, Value> storedProgress, SweepProgress newProgress)
            throws JsonProcessingException {
        if (storedProgress.isEmpty()) {
            // Progress for this thread has never been stored
            return CheckAndSetRequest.newCell(
                    AtlasDbConstants.SWEEP_PROGRESS_TABLE, cell, progressToBytes(newProgress));
        }

        Value storedValue = Iterables.getOnlyElement(storedProgress.values());
        if (isFinishedTablePlaceholder(storedValue)) {
            // Last iteration, this thread finished a table
            return CheckAndSetRequest.singleCell(
                    AtlasDbConstants.SWEEP_PROGRESS_TABLE, cell, FINISHED_TABLE, progressToBytes(newProgress));
        } else {
            return CheckAndSetRequest.singleCell(
                    AtlasDbConstants.SWEEP_PROGRESS_TABLE,
                    cell,
                    progressToBytes(hydrateProgress(storedProgress).get()),
                    progressToBytes(newProgress));
        }
    }

    /**
     * Fully remove a single column of the sweep progress table.
     */
    @Override
    public void clearProgress(TableReference tableRef) {
        loadProgress(tableRef).ifPresent(this::clearProgress);
    }

    private void clearProgress(SweepProgress progress) {
        clearProgressFromCell(progress, getCell(progress.tableRef()));
    }

    private void clearProgressFromCell(SweepProgress progress, Cell cell) {
        try {
            CheckAndSetRequest request = CheckAndSetRequest.singleCell(
                    AtlasDbConstants.SWEEP_PROGRESS_TABLE, cell, progressToBytes(progress), FINISHED_TABLE);
            kvs.checkAndSet(request);
        } catch (JsonProcessingException e) {
            log.warn(
                    "Exception trying to clear sweep progress. "
                            + "Sweep may continue examining the same range if the problem persists.",
                    e);
        }
    }

    private Cell getCell(TableReference tableRef) {
        return Cell.create(PtBytes.toBytes(tableRef.getQualifiedName()), ROW_AND_COLUMN_NAME_BYTES);
    }

    private void tryInitialize() {
        kvs.createTable(AtlasDbConstants.SWEEP_PROGRESS_TABLE, SWEEP_PROGRESS_METADATA.persistToBytes());
        loadLegacyProgress().ifPresent(this::moveToNewSchema);
    }

    private void moveToNewSchema(SweepProgress legacyProgress) {
        log.info(
                "Upgrading AtlasDB's sweep progress schema - sweep of table {} will resume where it left off when "
                        + "this table is next swept, but other tables may be swept in the meantime.",
                LoggingArgs.tableRef(legacyProgress.tableRef()));
        saveProgress(legacyProgress);
        clearLegacyProgress(legacyProgress);
    }

    private void clearLegacyProgress(SweepProgress legacyProgress) {
        clearProgressFromCell(legacyProgress, LEGACY_CELL);
    }

    @VisibleForTesting
    static byte[] progressToBytes(SweepProgress value) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsBytes(value);
    }

    private static Optional<SweepProgress> hydrateProgress(Map<Cell, Value> result) {
        if (result.isEmpty()) {
            log.info("No persisted SweepProgress information found.");
            return Optional.empty();
        }
        try {
            Value value = Iterables.getOnlyElement(result.values());
            if (isFinishedTablePlaceholder(value)) {
                log.debug("This thread finished a table last time around - returning empty progress object.");
                return Optional.empty();
            }
            return Optional.of(OBJECT_MAPPER.readValue(value.getContents(), SweepProgress.class));
        } catch (Exception e) {
            log.warn("Error deserializing SweepProgress object.", e);
            return Optional.empty();
        }
    }

    private static boolean isFinishedTablePlaceholder(Value value) {
        return Arrays.equals(value.getContents(), FINISHED_TABLE);
    }
}
