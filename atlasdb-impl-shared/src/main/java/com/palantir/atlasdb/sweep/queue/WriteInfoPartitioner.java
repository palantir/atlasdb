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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;

public class WriteInfoPartitioner {
    private static final Logger log = LoggerFactory.getLogger(WriteInfoPartitioner.class);

    private final KeyValueService kvs;
    private final Supplier<Integer> numShards;

    private final LoadingCache<TableReference, TableMetadataPersistence.SweepStrategy> cache = CacheBuilder
            .newBuilder().build(
                    new CacheLoader<TableReference, TableMetadataPersistence.SweepStrategy>() {
                        @Override
                        public TableMetadataPersistence.SweepStrategy load(TableReference key) throws Exception {
                            return getStrategyFromKvs(key);
                        }
                    });

    public WriteInfoPartitioner(KeyValueService kvs, Supplier<Integer> numShards) {
        this.kvs = kvs;
        this.numShards = numShards;
    }

    /**
     * Filters out all writes made into tables with SweepStrategy NOTHING, then partitions the writes according to
     * shard, strategy, and start timestamp of the transaction that performed the write.
     */
    public Map<PartitionInfo, List<WriteInfo>> filterAndPartition(List<WriteInfo> writes) {
        return partitionWritesByShardStrategyTimestamp(filterOutUnsweepableTables(writes));
    }

    @VisibleForTesting
    List<WriteInfo> filterOutUnsweepableTables(List<WriteInfo> writes) {
        return writes.stream()
                .filter(writeInfo -> getStrategy(writeInfo) != TableMetadataPersistence.SweepStrategy.NOTHING)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    Map<PartitionInfo, List<WriteInfo>> partitionWritesByShardStrategyTimestamp(List<WriteInfo> writes) {
        int shards = numShards.get();
        return writes.stream()
                .collect(Collectors.groupingBy(write -> getPartitionInfo(write, shards), Collectors.toList()));
    }

    private PartitionInfo getPartitionInfo(WriteInfo write, int shards) {
        return PartitionInfo.of(write.toShard(shards), isConservative(write), write.timestamp());
    }

    @VisibleForTesting
    TableMetadataPersistence.SweepStrategy getStrategy(WriteInfo writeInfo) {
        return cache.getUnchecked(writeInfo.tableRef());
    }

    private TableMetadataPersistence.SweepStrategy getStrategyFromKvs(TableReference tableRef) {
        try {
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(kvs.getMetadataForTable(tableRef)).getSweepStrategy();
        } catch (Exception e) {
            log.warn("Failed to obtain sweep strategy for table {}.",
                    LoggingArgs.tableRef(tableRef), e);
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private boolean isConservative(WriteInfo write) {
        return getStrategy(write) == TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
    }
}
