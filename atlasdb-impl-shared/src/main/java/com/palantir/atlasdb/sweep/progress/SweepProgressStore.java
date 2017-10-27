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
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;

public class SweepProgressStore {
    private static final Logger log = LoggerFactory.getLogger(SweepProgressStore.class);

    private final KeyValueService kvs;
    private final TableReference tableRef;

    private static final byte[] BYTE_ROW = SweepProgressTable.SweepProgressRow.of(0).persistToBytes();
    private static final byte[] BYTE_COL = SweepProgressTable.SweepProgressNamedColumn.SWEEP_PROGRESS.getShortName();
    private static final Cell CELL = Cell.create(BYTE_ROW, BYTE_COL);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new AfterburnerModule());

    public SweepProgressStore(KeyValueService kvs, SweepTableFactory tableFactory) {
        this.kvs = kvs;
        this.tableRef = tableFactory.getSweepProgressTable(null).getTableRef();
    }

    public Optional<SweepProgress> loadProgress()  {
        Map<Cell, Value> entry = kvs.get(tableRef, ImmutableMap.of(CELL, Long.MAX_VALUE));
        return hydrateProgress(entry);
    }

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
    public void clearProgress() {
        // Use deleteRange instead of truncate
        // 1) The table should be small, performance difference should be negligible.
        // 2) Truncate takes an exclusive lock in Postgres, which can interfere
        // with concurrently running backups.
        kvs.deleteRange(tableRef, RangeRequest.all());
    }

    private CheckAndSetRequest casProgressRequest(Optional<SweepProgress> oldProgress, SweepProgress newProgress)
            throws JsonProcessingException {
        if (!oldProgress.isPresent()) {
            return CheckAndSetRequest.newCell(tableRef, CELL, progressToBytes(newProgress));
        }
        return CheckAndSetRequest.singleCell(tableRef,
                CELL, progressToBytes(oldProgress.get()), progressToBytes(newProgress));
    }

    private byte[] progressToBytes(SweepProgress value) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsBytes(value);
    }

    private static Optional<SweepProgress> hydrateProgress(Map<Cell, Value> result) {
        if (result.isEmpty()) {
            log.info("No persisted intermediate SweepProgress information found. "
                    + "Sweep will choose a new table to sweep.");
            return Optional.empty();
        }
        try {
            return Optional.of(OBJECT_MAPPER.readValue(result.get(CELL).getContents(), SweepProgress.class));
        } catch (Exception e) {
            log.warn("Error deserializing SweepProgress object while attempting to load intermediate result. "
                    + "Sweep will choose a new table to sweep.", e);
            return Optional.empty();
        }
    }
}
