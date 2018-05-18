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

import java.util.ArrayList;
import java.util.HashMap;
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
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.UnsafeArg;

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
        Map<PartitionInfo, List<WriteInfo>> result = new HashMap<>();
        writes.forEach(write ->
                result.computeIfAbsent(getPartitionInfo(write, shards), no -> new ArrayList<>()).add(write));
        return result;
    }

    private PartitionInfo getPartitionInfo(WriteInfo write, int shards) {
        return PartitionInfo.of(write.toShard(shards), isConservative(write), write.timestamp());
    }

    @VisibleForTesting
    TableMetadataPersistence.SweepStrategy getStrategy(WriteInfo writeInfo) {
        return cache.getUnchecked(writeInfo.writeRef().tableRef());
    }

    private TableMetadataPersistence.SweepStrategy getStrategyFromKvs(TableReference tableRef) {
        try {
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(kvs.getMetadataForTable(tableRef)).getSweepStrategy();
        } catch (Throwable th) {
            log.warn("Failed to obtain sweep strategy for table {}. Assuming sweep strategy is CONSERVATIVE.",
                    UnsafeArg.of("tableRef", tableRef), th);
            return TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
        }
    }

    private boolean isConservative(WriteInfo write) {
        return getStrategy(write) == TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
    }
}
