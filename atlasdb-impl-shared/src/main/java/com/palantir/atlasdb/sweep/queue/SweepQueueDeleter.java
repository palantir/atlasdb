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

package com.palantir.atlasdb.sweep.queue;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.Sweeper;

public class SweepQueueDeleter {
    private static final Logger log = LoggerFactory.getLogger(SweepQueueDeleter.class);

    private final KeyValueService kvs;
    private final TargetedSweepFollower follower;

    SweepQueueDeleter(KeyValueService kvs, TargetedSweepFollower follower) {
        this.kvs = kvs;
        this.follower = follower;
    }

    /**
     * Executes targeted sweep, by inserting ranged tombstones corresponding to the given writes, using the sweep
     * strategy determined by the sweeper.
     *
     * @param writes individual writes to sweep for. Depending on the strategy, we will insert a ranged tombstone for
     * each write at either the write's timestamp - 1, or at its timestamp.
     * @param sweeper supplies the strategy-specific behaviour: the timestamp for the tombstone and whether we must use
     * sentinels or not.
     */
    public void sweep(Collection<WriteInfo> writes, Sweeper sweeper) {
        Map<TableReference, Map<Cell, Long>> maxTimestampByCell = writesPerTable(writes, sweeper);
        for (Map.Entry<TableReference, Map<Cell, Long>> entry : maxTimestampByCell.entrySet()) {
            try {
                Iterables.partition(entry.getValue().keySet(), SweepQueueUtils.BATCH_SIZE_KVS)
                        .forEach(cells -> {
                            Map<Cell, Long> maxTimestampByCellPartition = cells.stream()
                                    .collect(Collectors.toMap(Function.identity(), entry.getValue()::get));
                            follower.run(entry.getKey(), maxTimestampByCellPartition.keySet());
                            if (sweeper.shouldAddSentinels()) {
                                kvs.addGarbageCollectionSentinelValues(entry.getKey(),
                                        maxTimestampByCellPartition.keySet());
                                kvs.deleteAllTimestamps(entry.getKey(), maxTimestampByCellPartition, false);
                            } else {
                                kvs.deleteAllTimestamps(entry.getKey(), maxTimestampByCellPartition, true);
                            }
                        });
            } catch (IllegalArgumentException e) {
                // hack because InMemoryKVS doesn't throw a TableMappingNotFoundException...
                if (e.getCause() instanceof TableMappingNotFoundException
                        || e.getMessage().endsWith("does not exist")) {
                    log.warn("Could not resolve full name for table reference {}, "
                            + "assuming table has been deleted and therefore relevant cells as well.",
                            LoggingArgs.tableRef(entry.getKey()));
                    log.debug("Stack trace follows:", e);
                } else {
                    throw e;
                }
            }
        }
    }

    private Map<TableReference, Map<Cell, Long>> writesPerTable(Collection<WriteInfo> writes, Sweeper sweeper) {
        return writes.stream().collect(Collectors.groupingBy(
                WriteInfo::tableRef,
                Collectors.toMap(WriteInfo::cell, write -> write.timestampToDeleteAtExclusive(sweeper))));
    }
}
