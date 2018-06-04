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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.logsafe.SafeArg;

public final class SweepQueue implements SweepQueueWriter {
    private static final Logger log = LoggerFactory.getLogger(SweepQueue.class);
    private static final long FIVE_MINUTES = TimeUnit.MINUTES.toMillis(5L);

    private final SweepableCells sweepableCells;
    private final SweepableTimestamps sweepableTimestamps;
    private final ShardProgress progress;
    private final SweepQueueDeleter deleter;
    private final SweepQueueCleaner cleaner;
    private final Supplier<Integer> numShards;
    private final TargetedSweepMetrics metrics;

    private SweepQueue(SweepableCells cells, SweepableTimestamps timestamps, ShardProgress progress,
            SweepQueueDeleter deleter, SweepQueueCleaner cleaner, Supplier<Integer> numShards,
            TargetedSweepMetrics metrics) {
        this.sweepableCells = cells;
        this.sweepableTimestamps = timestamps;
        this.progress = progress;
        this.deleter = deleter;
        this.cleaner = cleaner;
        this.numShards = numShards;
        this.metrics = metrics;
    }


    public static SweepQueue create(KeyValueService kvs, Supplier<Integer> shardsConfig, int minShards) {
        TargetedSweepMetrics metrics = TargetedSweepMetrics.create(kvs, FIVE_MINUTES);
        ShardProgress progress = new ShardProgress(kvs);
        progress.updateNumberOfShards(minShards);
        Supplier<Integer> shards = createProgressUpdatingSupplier(shardsConfig, progress, FIVE_MINUTES);
        WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
        SweepableCells cells = new SweepableCells(kvs, partitioner, metrics);
        SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
        SweepQueueDeleter deleter = new SweepQueueDeleter(kvs);
        SweepQueueCleaner cleaner = new SweepQueueCleaner(cells, timestamps, progress);
        return new SweepQueue(cells, timestamps, progress, deleter, cleaner, shards, metrics);
    }

    /**
     * Creates a supplier such that the first call to {@link Supplier#get()} on it will take the maximum of the runtime
     * configuration and the persisted number of shards, and persist and memoize the result. Subsequent calls will
     * return the cached value until refreshTimeMillis has passed, at which point the next call will again perform the
     * check and set.
     *
     * @param runtimeConfig live reloadable runtime configuration for the number of shards
     * @param progress progress table persisting the number of shards
     * @param refreshTimeMillis timeout for caching the number of shards
     * @return supplier calculating and persisting the number of shards to use
     */
    @VisibleForTesting
    static Supplier<Integer> createProgressUpdatingSupplier(Supplier<Integer> runtimeConfig,
            ShardProgress progress, long refreshTimeMillis) {
        return Suppliers.memoizeWithExpiration(
                () -> progress.updateNumberOfShards(runtimeConfig.get()), refreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        sweepableTimestamps.enqueue(writes);
        sweepableCells.enqueue(writes);
        log.debug("Enqueued {} writes into the sweep queue.", SafeArg.of("writes", writes.size()));
    }

    /**
     * Sweep the next batch for the shard and strategy specified by shardStrategy, with the sweep timestamp sweepTs.
     * After successful deletes, the persisted information about the writes is removed, and progress is updated
     * accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @param sweepTs sweep timestamp, the upper limit to the start timestamp of writes to sweep
     */
    public void sweepNextBatch(ShardAndStrategy shardStrategy, long sweepTs) {
        metrics.updateSweepTimestamp(shardStrategy, sweepTs);
        long lastSweptTs = progress.getLastSweptTimestamp(shardStrategy);

        if (lastSweptTs + 1 >= sweepTs) {
            return;
        }

        log.debug("Beginning iteration of targeted sweep for {}, and sweep timestamp {}. Last previously swept timestamp"
                + " for this shard and strategy was {}.", SafeArg.of("shardStrategy", shardStrategy.toText()),
                SafeArg.of("sweepTs", sweepTs), SafeArg.of("lastSweptTs", lastSweptTs));

        SweepBatch sweepBatch = getNextBatchToSweep(shardStrategy, lastSweptTs, sweepTs);

        deleter.sweep(sweepBatch.writes(), Sweeper.of(shardStrategy));

        if (sweepBatch.writes().size() > 0) {
            log.debug("Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardStrategy.toText()));
        }

        cleaner.clean(shardStrategy, lastSweptTs, sweepBatch.lastSweptTimestamp());

        metrics.updateNumberOfTombstones(shardStrategy, sweepBatch.writes().size());
        metrics.updateProgressForShard(shardStrategy, sweepBatch.lastSweptTimestamp());
    }

    private SweepBatch getNextBatchToSweep(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        return sweepableTimestamps.nextSweepableTimestampPartition(shardStrategy, lastSweptTs, sweepTs)
                .map(fine -> sweepableCells.getBatchForPartition(shardStrategy, fine, lastSweptTs, sweepTs))
                .orElse(SweepBatch.of(ImmutableList.of(), sweepTs - 1L));
    }

    /**
     * Returns number modulo the most recently known number of shards.
     */
    public int modShards(long number) {
        return (int) (number % numShards.get());
    }
}
