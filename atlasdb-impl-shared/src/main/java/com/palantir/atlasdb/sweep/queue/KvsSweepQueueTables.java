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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public final class KvsSweepQueueTables {
    private static final int FIVE_MINUTES = 5000 * 60;

    final SweepableCells sweepableCells;
    final SweepableTimestamps sweepableTimestamps;
    final KvsSweepQueueProgress progress;
    final Supplier<Integer> numShards;

    private KvsSweepQueueTables(SweepableCells cells, SweepableTimestamps timestamps, KvsSweepQueueProgress progress,
            Supplier<Integer> numShards) {
        this.sweepableCells = cells;
        this.sweepableTimestamps = timestamps;
        this.progress = progress;
        this.numShards = numShards;
    }

    public static KvsSweepQueueTables create(KeyValueService kvs, Supplier<Integer> shardsConfig) {
        KvsSweepQueueProgress progress = new KvsSweepQueueProgress(kvs);
        Supplier<Integer> shards = createUpdatingSupplier(shardsConfig, progress::updateNumberOfShards, FIVE_MINUTES);
        WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
        SweepableCells cells = new SweepableCells(kvs, partitioner);
        SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
        return new KvsSweepQueueTables(cells, timestamps, progress, shards);
    }

    @VisibleForTesting
    static Supplier<Integer> createUpdatingSupplier(Supplier<Integer> runtimeConfig,
            UnaryOperator<Integer> updateOperator, long refreshTimeMillis) {
        return Suppliers.memoizeWithExpiration(
                () -> updateOperator.apply(runtimeConfig.get()), refreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    public long getLastSweptTimestamp(ShardAndStrategy shardStrategy) {
        return progress.getLastSweptTimestamp(shardStrategy);
    }

    public SweepBatch getNextBatchAndSweptTimestamp(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        return sweepableTimestamps.nextSweepableTimestampPartition(shardStrategy, lastSweptTs, sweepTs)
                .map(fine -> sweepableCells.getBatchForPartition(shardStrategy, fine, lastSweptTs, sweepTs))
                .orElse(SweepBatch.of(ImmutableList.of(), sweepTs - 1L));
    }

    public void enqueue(List<WriteInfo> writes) {
        sweepableTimestamps.enqueue(writes);
        sweepableCells.enqueue(writes);
    }

    public int modShards(long number) {
        return (int) (number % numShards.get());
    }
}
