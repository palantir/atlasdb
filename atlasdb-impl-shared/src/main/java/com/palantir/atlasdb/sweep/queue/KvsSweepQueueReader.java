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
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class KvsSweepQueueReader implements SweepQueueReader {
    private final SweepableCellsReader cellsReader;
    private final SweepableTimestampsReader timestampsReader;
    private final ShardAndStrategy shardStrategy;

    KvsSweepQueueReader(KeyValueService kvs, ShardAndStrategy shardStrategy) {
        this.cellsReader = new SweepableCellsReader(kvs);
        this.timestampsReader = new SweepableTimestampsReader(kvs);
        this.shardStrategy = shardStrategy;
    }

    public static KvsSweepQueueReader create(KeyValueService kvs, int shard,
            TableMetadataPersistence.SweepStrategy sweepStrategy) {
        return new KvsSweepQueueReader(kvs, ShardAndStrategy.of(shard, sweepStrategy));
    }

    @Override
    public void consumeNextBatch(Consumer<Collection<WriteInfo>> consumer, long maxTimestampExclusive) {
        consumer.accept(getNextBatch(maxTimestampExclusive));
    }

    private List<WriteInfo> getNextBatch(long tsExclusive) {
        Optional<Long> partitionFine = timestampsReader.nextSweepableTimestampPartition(shardStrategy, tsExclusive);
        if (!partitionFine.isPresent()) {
            return ImmutableList.of();
        }
        return cellsReader.getLatestWrites(partitionFine.get(), shardStrategy);
    }
}
