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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.logsafe.SafeArg;

public final class SweepQueue implements MultiTableSweepQueueWriter {
    private static final Logger log = LoggerFactory.getLogger(SweepQueue.class);
    private final ShardProgress progress;
    private final SweepQueueWriter writer;
    private final SweepQueueReader reader;
    private final SweepQueueDeleter deleter;
    private final SweepQueueCleaner cleaner;
    private final Supplier<Integer> numShards;
    private final TargetedSweepMetrics metrics;

    private SweepQueue(SweepQueueFactory factory, TargetedSweepFollower follower) {
        this.progress = factory.progress;
        this.writer = factory.createWriter();
        this.reader = factory.createReader();
        this.deleter = factory.createDeleter(follower);
        this.cleaner = factory.createCleaner();
        this.numShards = factory.numShards;
        this.metrics = factory.metrics;
    }

    public static SweepQueue create(TargetedSweepMetrics metrics, KeyValueService kvs, Supplier<Integer> shardsConfig,
            TargetedSweepFollower follower) {
        return new SweepQueue(SweepQueueFactory.create(metrics, kvs, shardsConfig), follower);
    }

    /**
     * Creates a SweepQueueWriter, performing all the necessary initialization.
     */
    public static MultiTableSweepQueueWriter createWriter(TargetedSweepMetrics metrics, KeyValueService kvs,
            Supplier<Integer> shardsConfig) {
        return SweepQueueFactory.create(metrics, kvs, shardsConfig).createWriter();
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
    public static Supplier<Integer> createProgressUpdatingSupplier(Supplier<Integer> runtimeConfig,
            ShardProgress progress, long refreshTimeMillis) {
        return Suppliers.memoizeWithExpiration(
                () -> progress.updateNumberOfShards(runtimeConfig.get()), refreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        writer.enqueue(writes);
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

        log.debug("Beginning iteration of targeted sweep for {}, and sweep timestamp {}. Last previously swept "
                        + "timestamp for this shard and strategy was {}.",
                SafeArg.of("shardStrategy", shardStrategy.toText()),
                SafeArg.of("sweepTs", sweepTs), SafeArg.of("lastSweptTs", lastSweptTs));

        SweepBatch sweepBatch = reader.getNextBatchToSweep(shardStrategy, lastSweptTs, sweepTs);

        deleter.sweep(sweepBatch.writes(), Sweeper.of(shardStrategy));

        if (!sweepBatch.isEmpty()) {
            log.debug("Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardStrategy.toText()));
        }

        cleaner.clean(shardStrategy, lastSweptTs, sweepBatch.lastSweptTimestamp(), sweepBatch.dedicatedRows());

        metrics.updateNumberOfTombstones(shardStrategy, sweepBatch.writes().size());
        metrics.updateProgressForShard(shardStrategy, sweepBatch.lastSweptTimestamp());

        if (sweepBatch.isEmpty()) {
            metrics.registerOccurrenceOf(SweepOutcome.NOTHING_TO_SWEEP);
        } else {
            metrics.registerOccurrenceOf(SweepOutcome.SUCCESS);
        }
    }

    /**
     * Returns the most recently known number of shards.
     */
    public int getNumShards() {
        return numShards.get();
    }

    private static final class SweepQueueFactory {
        private final ShardProgress progress;
        private final Supplier<Integer> numShards;
        private final SweepableCells cells;
        private final SweepableTimestamps timestamps;
        private final TargetedSweepMetrics metrics;
        private final KeyValueService kvs;

        private SweepQueueFactory(ShardProgress progress, Supplier<Integer> numShards, SweepableCells cells,
                SweepableTimestamps timestamps, TargetedSweepMetrics metrics, KeyValueService kvs) {
            this.progress = progress;
            this.numShards = numShards;
            this.cells = cells;
            this.timestamps = timestamps;
            this.metrics = metrics;
            this.kvs = kvs;
        }

        static SweepQueueFactory create(TargetedSweepMetrics metrics, KeyValueService kvs,
                Supplier<Integer> shardsConfig) {
            Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
            ShardProgress shardProgress = new ShardProgress(kvs);
            Supplier<Integer> shards = createProgressUpdatingSupplier(shardsConfig, shardProgress,
                    SweepQueueUtils.REFRESH_TIME);
            WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
            SweepableCells cells = new SweepableCells(kvs, partitioner, metrics);
            SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
            return new SweepQueueFactory(shardProgress, shards, cells, timestamps, metrics, kvs);
        }

        private SweepQueueWriter createWriter() {
            return new SweepQueueWriter(timestamps, cells);
        }

        private SweepQueueReader createReader() {
            return new SweepQueueReader(timestamps, cells);
        }

        private SweepQueueDeleter createDeleter(TargetedSweepFollower follower) {
            return new SweepQueueDeleter(kvs, follower);
        }

        private SweepQueueCleaner createCleaner() {
            return new SweepQueueCleaner(cells, timestamps, progress);
        }
    }
}
