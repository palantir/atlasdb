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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.util.Pair;

public class KvsSweepQueuePersister implements MultiTableSweepQueueWriter {
    // todo(gmaretic): temporarily constant
    private static final int SHARDS = 128;

    private final KeyValueService kvs;
    private final SweepStrategyCache cache;
    private final SweepableCellsWriter sweepableCellsWriter;
    private final SweepableTimestampsWriter sweepableTimestampsWriter;

    public KvsSweepQueuePersister(KeyValueService kvs, TargetedSweepTableFactory tableFactory) {
        this.kvs = kvs;
        this.cache =  new SweepStrategyCache(kvs);
        this.sweepableCellsWriter = new SweepableCellsWriter(kvs, tableFactory, cache);
        this.sweepableTimestampsWriter = new SweepableTimestampsWriter(kvs, tableFactory, cache);
        initialize();
    }

    public void initialize() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        List<WriteInfo> filteredWrites = filterOutUnsweepableTables(writes);
        List<List<WriteInfo>> partitionedWrites = partitionWritesByShardAndStrategy(filteredWrites);
        partitionedWrites.forEach(sweepableCellsWriter::enqueue);
        partitionedWrites.forEach(sweepableTimestampsWriter::enqueue);
    }

    private List<WriteInfo> filterOutUnsweepableTables(List<WriteInfo> writes) {
        return writes.stream()
                .filter(writeInfo -> cache.getStrategy(writeInfo) != TableMetadataPersistence.SweepStrategy.NOTHING)
                .collect(Collectors.toList());
    }

    private List<List<WriteInfo>> partitionWritesByShardAndStrategy(List<WriteInfo> writes) {
        Map<Pair<Integer, TableMetadataPersistence.SweepStrategy>, List<WriteInfo>> mapping = new HashMap<>();
        for (WriteInfo write : writes) {
            mapping.computeIfAbsent(Pair.create(getShard(write), cache.getStrategy(write)), unused -> new ArrayList<>())
                    .add(write);
        }
        return ImmutableList.copyOf(mapping.values());
    }

    public static int getShard(WriteInfo writeInfo) {
        int shard = writeInfo.tableRefCell().hashCode() % SHARDS;
        return (shard + SHARDS) % SHARDS;
    }
}
