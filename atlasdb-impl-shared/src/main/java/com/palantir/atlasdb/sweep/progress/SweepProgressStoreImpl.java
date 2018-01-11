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

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = SweepProgressStore.class)
public final class SweepProgressStoreImpl implements SweepProgressStore {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_SweepProgressStore {
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new AfterburnerModule());

    private static final String ROW_AND_COLUMN_NAME = "s";
    private static final byte[] ROW_AND_COLUMN_NAME_BYTES = PtBytes.toCachedBytes(ROW_AND_COLUMN_NAME);
    private static final Cell CELL = Cell.create(ROW_AND_COLUMN_NAME_BYTES, ROW_AND_COLUMN_NAME_BYTES);

    private static final TableMetadata SWEEP_PROGRESS_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription.Builder()
                            .componentName("dummy")
                            .type(ValueType.STRING)
                            .build())),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            ROW_AND_COLUMN_NAME,
                            "sweep_progress",
                            ColumnValueDescription.forType(ValueType.BLOB)))),
            ConflictHandler.IGNORE_ALL);

    private SweepProgressStoreImpl(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public static SweepProgressStore create(KeyValueService kvs, boolean initializeAsync) {
        SweepProgressStoreImpl progressStore = new SweepProgressStoreImpl(kvs);
        progressStore.wrapper.initialize(initializeAsync);
        return progressStore.wrapper.isInitialized() ? progressStore : progressStore.wrapper;
    }

    @Override
    public Optional<SweepProgress> loadProgress()  {
        Map<Cell, Value> entry = kvs.get(AtlasDbConstants.SWEEP_PROGRESS_TABLE, ImmutableMap.of(CELL, 1L));
        return hydrateProgress(entry);
    }

    @Override
    public void saveProgress(SweepProgress newProgress) {
        Optional<SweepProgress> oldProgress = loadProgress();
        try {
            kvs.checkAndSet(casProgressRequest(oldProgress, newProgress));
        } catch (Exception e) {
            log.warn("Exception trying to persist sweep progress. The intermediate progress might not have been "
                    + "persisted. This should not cause sweep issues unless the problem persists.", e);
        }
    }

    /**
     * Fully remove the contents of the sweep progress table.
     */
    @Override
    public void clearProgress() {
        // Use deleteRange instead of truncate
        // 1) The table should be small, performance difference should be negligible.
        // 2) Truncate takes an exclusive lock in Postgres, which can interfere
        // with concurrently running backups.
        kvs.deleteRange(AtlasDbConstants.SWEEP_PROGRESS_TABLE, RangeRequest.all());
    }

    private CheckAndSetRequest casProgressRequest(Optional<SweepProgress> oldProgress, SweepProgress progress)
            throws JsonProcessingException {
        if (!oldProgress.isPresent()) {
            return CheckAndSetRequest.newCell(AtlasDbConstants.SWEEP_PROGRESS_TABLE, CELL, progressToBytes(progress));
        }
        return CheckAndSetRequest.singleCell(AtlasDbConstants.SWEEP_PROGRESS_TABLE,
                CELL, progressToBytes(oldProgress.get()), progressToBytes(progress));
    }

    private void tryInitialize() {
        kvs.createTable(AtlasDbConstants.SWEEP_PROGRESS_TABLE, SWEEP_PROGRESS_METADATA.persistToBytes());
    }

    private byte[] progressToBytes(SweepProgress value) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsBytes(value);
    }

    private static Optional<SweepProgress> hydrateProgress(Map<Cell, Value> result) {
        if (result.isEmpty()) {
            log.info("No persisted SweepProgress information found.");
            return Optional.empty();
        }
        try {
            return Optional.of(OBJECT_MAPPER.readValue(result.get(CELL).getContents(), SweepProgress.class));
        } catch (Exception e) {
            log.warn("Error deserializing SweepProgress object.", e);
            return Optional.empty();
        }
    }
}
