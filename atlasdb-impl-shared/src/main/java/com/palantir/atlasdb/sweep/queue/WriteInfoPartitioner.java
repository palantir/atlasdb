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
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WriteInfoPartitioner {
    private static final SafeLogger log = SafeLoggerFactory.get(WriteInfoPartitioner.class);

    private final KeyValueService kvs;
    private final Supplier<Integer> numShards;

    private final LoadingCache<TableReference, Optional<SweeperStrategy>> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<TableReference, Optional<SweeperStrategy>>() {
                @Override
                public Optional<SweeperStrategy> load(TableReference key) throws Exception {
                    return getStrategyFromKvs(key);
                }
            });

    public WriteInfoPartitioner(KeyValueService kvs, Supplier<Integer> numShards) {
        this.kvs = kvs;
        this.numShards = numShards;
    }

    public Map<PartitionInfo, List<WriteInfo>> filterAndPartitionUnsweepableCells(List<WriteInfo> writes) {
        // todo(snanda); which shards to use?
        return partitionUnsweepableWritesByShardStrategyTimestamp(filterOutSweepableTables(writes));
    }

    // Todo(snanda): if we have introduces a third state, then we do not have to filter out the writes.
    @VisibleForTesting
    List<WriteInfo> filterOutSweepableTables(List<WriteInfo> writes) {
        return writes.stream()
                .filter(writeInfo -> getStrategy(writeInfo).isEmpty())
                .collect(Collectors.toList());
    }

    Map<PartitionInfo, List<WriteInfo>> partitionUnsweepableWritesByShardStrategyTimestamp(List<WriteInfo> writes) {
        int shards = numShards.get();
        return writes.stream()
                .collect(Collectors.groupingBy(write -> getWeirdPartitionInfo(write, shards), Collectors.toList()));
    }

    private PartitionInfo getWeirdPartitionInfo(WriteInfo write, int shards) {
        return PartitionInfo.of(shards + write.toShard(shards), getSweeperStrategyForWrite(write), write.timestamp());
    }

    /**
     * Filters out all writes made into tables with SweepStrategy NOTHING, then partitions the writes according to
     * shard, strategy, and start timestamp of the transaction that performed the write.
     */
    public Map<PartitionInfo, List<WriteInfo>> filterAndPartition(List<WriteInfo> writes) {
        return partitionWritesByShardStrategyTimestamp(filterOutUnsweepableTables(writes));
    }
    // todo(snanda): this filtering is so obscure - refactor required
    @VisibleForTesting
    List<WriteInfo> filterOutUnsweepableTables(List<WriteInfo> writes) {
        return writes.stream()
                .filter(writeInfo -> getStrategy(writeInfo).isPresent())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    Map<PartitionInfo, List<WriteInfo>> partitionWritesByShardStrategyTimestamp(List<WriteInfo> writes) {
        int shards = numShards.get();
        return writes.stream()
                .collect(Collectors.groupingBy(write -> getPartitionInfo(write, shards), Collectors.toList()));
    }

    private PartitionInfo getPartitionInfo(WriteInfo write, int shards) {
        return PartitionInfo.of(write.toShard(shards), getSweeperStrategyForWrite(write), write.timestamp());
    }

    // todo(snanda): consequences
    private SweeperStrategy getSweeperStrategyForWrite(WriteInfo write) {
        Optional<SweeperStrategy> strategy = getStrategy(write);
        return strategy.orElse(SweeperStrategy.NOTHING);
    }

    @VisibleForTesting
    Optional<SweeperStrategy> getStrategy(WriteInfo writeInfo) {
        return getStrategyForTable(writeInfo.tableRef());
    }

    Optional<SweeperStrategy> getStrategyForTable(TableReference tableRef) {
        return cache.getUnchecked(tableRef);
    }

    private Optional<SweeperStrategy> getStrategyFromKvs(TableReference tableRef) {
        try {
            return SweepStrategy.from(TableMetadata.BYTES_HYDRATOR
                            .hydrateFromBytes(kvs.getMetadataForTable(tableRef))
                            .getSweepStrategy())
                    .getSweeperStrategy();
        } catch (Exception e) {
            log.warn("Failed to obtain sweep strategy for table {}.", LoggingArgs.tableRef(tableRef), e);
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
