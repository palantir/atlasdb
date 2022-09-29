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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WriteInfoPartitioner {
    private static final SafeLogger log = SafeLoggerFactory.get(WriteInfoPartitioner.class);

    private final KeyValueService kvs;
    private final Supplier<Integer> numShards;

    private final LoadingCache<TableReference, SweeperStrategy> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<>() {
                @Override
                public SweeperStrategy load(TableReference key) {
                    return getStrategyFromKvs(key);
                }
            });

    public WriteInfoPartitioner(KeyValueService kvs, Supplier<Integer> numShards) {
        this.kvs = kvs;
        this.numShards = numShards;
    }

    /**
     * If all writes are made into tables with SweepStrategy NOTHING, then the write info is just the start
     * timestamp, otherwise filters out all writes made into tables with SweepStrategy NOTHING and then partitions the
     * writes according to shard and strategy.
     */
    public Map<PartitionInfo, List<WriteInfo>> filterAndPartition(List<WriteInfo> writes) {
        return writes.stream().collect(Collectors.groupingBy(WriteInfo::timestamp)).values().stream()
                .map(this::filterAndPartitionForSingleTimestamp)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<PartitionInfo, List<WriteInfo>> filterAndPartitionForSingleTimestamp(List<WriteInfo> writes) {
        if (writes.stream().allMatch(writeInfo -> getStrategy(writeInfo) == SweeperStrategy.NON_SWEEPABLE)) {
            long startTs = writes.stream().findFirst().get().timestamp();
            return SweepQueueUtils.partitioningForNonSweepable(startTs);
        }
        return partitionWritesByShardStrategyTimestamp(writes);
    }

    private Map<PartitionInfo, List<WriteInfo>> partitionWritesByShardStrategyTimestamp(List<WriteInfo> writes) {
        int shards = numShards.get();
        return writes.stream()
                .filter(writeInfo -> getStrategy(writeInfo) != SweeperStrategy.NON_SWEEPABLE)
                .collect(Collectors.groupingBy(write -> getPartitionInfo(write, shards), Collectors.toList()));
    }

    private PartitionInfo getPartitionInfo(WriteInfo write, int shards) {
        return PartitionInfo.of(write.toShard(shards), getStrategy(write), write.timestamp());
    }

    @VisibleForTesting
    SweeperStrategy getStrategy(WriteInfo writeInfo) {
        return writeInfo
                .writeRef()
                .map(WriteReference::tableRef)
                .map(this::getStrategyForTable)
                .orElse(SweeperStrategy.NON_SWEEPABLE);
    }

    SweeperStrategy getStrategyForTable(TableReference tableRef) {
        return cache.getUnchecked(tableRef);
    }

    private SweeperStrategy getStrategyFromKvs(TableReference tableRef) {
        try {
            return SweepStrategy.from(TableMetadata.BYTES_HYDRATOR
                            .hydrateFromBytes(kvs.getMetadataForTable(tableRef))
                            .getSweepStrategy())
                    .getSweeperStrategy()
                    .orElse(SweeperStrategy.NON_SWEEPABLE);
        } catch (Exception e) {
            log.warn("Failed to obtain sweep strategy for table {}.", LoggingArgs.tableRef(tableRef), e);
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
