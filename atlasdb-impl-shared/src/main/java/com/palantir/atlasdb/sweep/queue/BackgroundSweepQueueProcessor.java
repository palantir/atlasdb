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

package com.palantir.atlasdb.sweep.queue;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;

public class BackgroundSweepQueueProcessor {

    private final Logger log = LoggerFactory.getLogger(BackgroundSweepQueueProcessor.class);

    private final KeyValueService kvs;
    private final Function<TableReference, SweepQueueProcessor> processorFactory;

    public BackgroundSweepQueueProcessor(
            KeyValueService kvs,
            Function<TableReference, SweepQueueProcessor> processorFactory) {
        this.kvs = kvs;
        this.processorFactory = processorFactory;
    }

    public void sweepOneBatchForAllTables() {
        for (TableReference table : kvs.getAllTableNames()) {
            trySweepOneBatch(table);
        }
    }

    @VisibleForTesting
    public void trySweepOneBatch(TableReference table) {
        try {
            processorFactory.apply(table).processNextBatch();
        } catch (Exception ex) {
            log.warn("error while processing sweep queue for table {}", LoggingArgs.tableRef(table));
        }
    }
}
