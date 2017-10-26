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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.schema.generated.SweepProgressTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SweepProgressStore {
    private static final Logger log = LoggerFactory.getLogger(SweepProgressStore.class);

    private final KeyValueService kvs;
    private final SweepTableFactory tableFactory;

    private static final SweepProgressTable.SweepProgressRow ROW = SweepProgressTable.SweepProgressRow.of(0);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new AfterburnerModule());

    public SweepProgressStore(KeyValueService kvs, SweepTableFactory tableFactory) {
        this.kvs = kvs;
        this.tableFactory = tableFactory;
    }

    public Optional<SweepProgress> loadProgress(Transaction tx)  {
        SweepProgressTable table = tableFactory.getSweepProgressTable(tx);
        return hydrateProgress(table.getSweepProgresss(ImmutableList.of(ROW)));
    }

    public void saveProgress(Transaction transaction, SweepProgress newProgress) {
        SweepProgressTable table = tableFactory.getSweepProgressTable(transaction);
        try {
            table.putSweepProgress(ROW, OBJECT_MAPPER.writeValueAsBytes(newProgress));
        } catch (JsonProcessingException e) {
            log.warn("Error serializing SweepProgress object while attempting to save intermittent result.");
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
        kvs.deleteRange(tableFactory.getSweepProgressTable(null).getTableRef(), RangeRequest.all());
    }

    public static Optional<SweepProgress> hydrateProgress(Map<SweepProgressTable.SweepProgressRow, byte[]> result) {
        if (result.isEmpty()) {
            log.info("No persisted SweepProgress information found. Sweep will choose a new table to sweep.");
            return Optional.empty();
        }
        try {
            return Optional.of(OBJECT_MAPPER.readValue(result.get(ROW), ImmutableSweepProgress.class));
        } catch (Exception e) {
            log.warn("Error deserializing SweepProgress object while attempting to load intermittent result. Sweep "
                    + "will choose a new table to sweep.");
            return Optional.empty();
        }
    }
}
