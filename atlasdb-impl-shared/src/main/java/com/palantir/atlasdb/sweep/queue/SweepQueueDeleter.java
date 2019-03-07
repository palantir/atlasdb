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
package com.palantir.atlasdb.sweep.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.common.streams.KeyedStream;

public class SweepQueueDeleter {
    private static final Logger log = LoggerFactory.getLogger(SweepQueueDeleter.class);

    private final KeyValueService kvs;
    private final TargetedSweepFollower follower;
    private final TargetedSweepFilter filter;

    SweepQueueDeleter(KeyValueService kvs, TargetedSweepFollower follower,
            TargetedSweepFilter filter) {
        this.kvs = kvs;
        this.follower = follower;
        this.filter = filter;
    }

    /**
     * Executes targeted sweep, by inserting ranged tombstones corresponding to the given writes, using the sweep
     * strategy determined by the sweeper.
     *
     * @param unfilteredWrites individual writes to sweep for. Depending on the strategy, we will insert a ranged
     * tombstone for each write at either the write's timestamp - 1, or at its timestamp.
     * @param sweeper supplies the strategy-specific behaviour: the timestamp for the tombstone and whether we must use
     * sentinels or not.
     */
    public void sweep(Collection<WriteInfo> unfilteredWrites, Sweeper sweeper) {
        Collection<WriteInfo> writes = filter.filter(unfilteredWrites);
        sweep(writesPerTable(writes), sweeper);
        sweep(thoroughTombstoneWritesPerTable(writes, sweeper), sweeper);
    }

    private static <K, V> Stream<Map<K, V>> partition(Map<K, V> map, int batchSize) {
        return Streams.stream(Iterables.partition(map.keySet(), batchSize))
                .map(keys -> KeyedStream.of(keys).map(map::get).collectToMap());
    }

    private void sweep(Map<TableReference, Map<Cell, Long>> maxTimestampByCell, Sweeper sweeper) {
        for (Map.Entry<TableReference, Map<Cell, Long>> entry : maxTimestampByCell.entrySet()) {
            try {
                partition(entry.getValue(), SweepQueueUtils.BATCH_SIZE_KVS)
                        .forEach(cells -> sweepBatch(entry.getKey(), cells, sweeper));
            } catch (Exception e) {
                if (tableWasDropped(entry.getKey())) {
                    log.info("The table {} has been deleted.", LoggingArgs.tableRef(entry.getKey()), e);
                } else {
                    throw e;
                }
            }
        }
    }

    private void sweepBatch(TableReference table, Map<Cell, Long> cells, Sweeper sweeper) {
        follower.run(table, cells.keySet());
        if (sweeper.shouldAddSentinels()) {
            kvs.addGarbageCollectionSentinelValues(table, cells.keySet());
            kvs.deleteAllTimestamps(table, cells, false);
        } else {
            kvs.deleteAllTimestamps(table, cells, true);
        }
    }

    private boolean tableWasDropped(TableReference tableRef) {
        return Arrays.equals(kvs.getMetadataForTable(tableRef), AtlasDbConstants.EMPTY_TABLE_METADATA);
    }

    private Map<TableReference, Map<Cell, Long>> writesPerTable(Collection<WriteInfo> writes) {
        return writes.stream().collect(Collectors.groupingBy(
                WriteInfo::tableRef,
                Collectors.toMap(WriteInfo::cell, WriteInfo::timestamp)));
    }

    private Map<TableReference, Map<Cell, Long>> thoroughTombstoneWritesPerTable(
            Collection<WriteInfo> writes, Sweeper sweeper) {
        return writes.stream()
                .filter(write -> write.timestampToDeleteAtExclusive(sweeper) != write.timestamp())
                .collect(Collectors.groupingBy(WriteInfo::tableRef,
                Collectors.toMap(WriteInfo::cell, write -> write.timestampToDeleteAtExclusive(sweeper))));
    }
}
