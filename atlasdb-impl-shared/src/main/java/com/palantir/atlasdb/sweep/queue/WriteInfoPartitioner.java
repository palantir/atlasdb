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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WriteInfoPartitioner {
    private static final SafeLogger log = SafeLoggerFactory.get(WriteInfoPartitioner.class);

    private final KeyValueService kvs;
    private final Supplier<Integer> numShards;
    private final Supplier<Integer> shardRotationIntervalMinutes;

    private final LoadingCache<TableReference, SweeperStrategy> cache =
            Caffeine.newBuilder().build(this::getStrategyFromKvs);

    public WriteInfoPartitioner(
            KeyValueService kvs, Supplier<Integer> numShards, Supplier<Integer> shardRotationIntervalMinutes) {
        this.kvs = kvs;
        this.numShards = numShards;
        this.shardRotationIntervalMinutes = shardRotationIntervalMinutes;
    }

    /**
     * If all writes are made into tables with SweepStrategy NOTHING, then the write info is just the start
     * timestamp, otherwise filters out all writes made into tables with SweepStrategy NOTHING and then partitions the
     * writes according to shard and strategy.
     */
    public Map<PartitionInfo, List<WriteInfo>> filterAndPartition(List<WriteInfo> writes) {
        if (writes.isEmpty()) {
            return Map.of();
        }

        Collection<List<WriteInfo>> partitionedWrites = writes.stream()
                .collect(Collectors.groupingBy(WriteInfo::timestamp))
                .values();

        Map<PartitionInfo, List<WriteInfo>> writesPartitionedByTimestamp = new HashMap<>();
        for (List<WriteInfo> writeInfos : partitionedWrites) {
            filterAndPartitionForSingleTimestamp(writeInfos)
                    .forEach((partition, writeInfo) -> writesPartitionedByTimestamp
                            .computeIfAbsent(partition, _k -> new ArrayList<>())
                            .addAll(writeInfo));
        }
        return writesPartitionedByTimestamp;
    }

    private Map<PartitionInfo, List<WriteInfo>> filterAndPartitionForSingleTimestamp(List<WriteInfo> writes) {
        if (writes.isEmpty()) {
            return Map.of();
        }

        int shards = numShards.get();
        int rotationIntervalMinutes = shardRotationIntervalMinutes.get();

        // Note that we do a single pass over writes to determine their sweep strategy, and only add sweepable write to
        // sweepablePartitionedWrites map, so when it is empty we consider all writes for that timestamp non-sweepable.
        Map<PartitionInfo, List<WriteInfo>> sweepablePartitionedWrites = new HashMap<>();
        for (WriteInfo writeInfo : writes) {
            SweeperStrategy strategy = getStrategy(writeInfo);
            if (strategy != SweeperStrategy.NON_SWEEPABLE) {
                PartitionInfo partition = PartitionInfo.of(
                        writeInfo.toShard(shards, rotationIntervalMinutes), strategy, writeInfo.timestamp());
                sweepablePartitionedWrites
                        .computeIfAbsent(partition, _k -> new ArrayList<>())
                        .add(writeInfo);
            }
        }

        if (sweepablePartitionedWrites.isEmpty()) {
            // All writes for this partition are non-sweepable
            long startTs = writes.stream().findFirst().orElseThrow().timestamp();
            return SweepQueueUtils.partitioningForNonSweepable(startTs);
        }
        return sweepablePartitionedWrites;
    }

    @VisibleForTesting
    SweeperStrategy getStrategy(WriteInfo writeInfo) {
        Optional<WriteReference> writeReference = writeInfo.writeRef();
        if (writeReference.isPresent()) {
            return getStrategyForTable(writeReference.get().tableRef());
        }
        return SweeperStrategy.NON_SWEEPABLE;
    }

    SweeperStrategy getStrategyForTable(TableReference tableRef) {
        return cache.get(tableRef);
    }

    private SweeperStrategy getStrategyFromKvs(TableReference tableRef) {
        try {
            return SweepStrategy.from(
                            TableMetadata.BYTES_HYDRATOR
                                    .hydrateFromBytes(kvs.getMetadataForTable(tableRef))
                                    .getSweepStrategy(),
                            kvs)
                    .getSweeperStrategy()
                    .orElse(SweeperStrategy.NON_SWEEPABLE);
        } catch (Exception e) {
            log.warn("Failed to obtain sweep strategy for table {}.", LoggingArgs.tableRef(tableRef), e);
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
