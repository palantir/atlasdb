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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;

public class WriteInfoPartitioner {
    // todo(gmaretic): temporarily constant
    static final int SHARDS = 128;

    private final KeyValueService kvs;

    private ConcurrentMap<TableReference, TableMetadataPersistence.SweepStrategy> cache = new ConcurrentHashMap<>();

    public WriteInfoPartitioner(KeyValueService kvs) {
        this.kvs = kvs;
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
        Map<PartitionInfo, List<WriteInfo>> result = new HashMap<>();
        writes.forEach(write -> result.computeIfAbsent(getPartitionInfo(write), no -> new ArrayList<>()).add(write));
        return result;
    }

    private PartitionInfo getPartitionInfo(WriteInfo write) {
        return PartitionInfo.of(getShard(write), isConservative(write), write.timestamp());
    }

    @VisibleForTesting
    TableMetadataPersistence.SweepStrategy getStrategy(WriteInfo writeInfo) {
        return cache.computeIfAbsent(writeInfo.tableRefCell().tableRef(), this::getStrategyFromKvs);
    }

    private TableMetadataPersistence.SweepStrategy getStrategyFromKvs(TableReference tableRef) {
        // todo(gmaretic): fail gracefully if we cannot hydrate? How -- return NOTHING?
        return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(kvs.getMetadataForTable(tableRef)).getSweepStrategy();
    }

    private static int getShard(WriteInfo writeInfo) {
        int shard = writeInfo.tableRefCell().hashCode() % SHARDS;
        return (shard + SHARDS) % SHARDS;
    }

    private boolean isConservative(WriteInfo write) {
        return getStrategy(write) == TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
    }
}
