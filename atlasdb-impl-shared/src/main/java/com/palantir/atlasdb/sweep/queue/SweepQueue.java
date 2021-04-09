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

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.DefaultSensitiveLoggingArgProducers;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader.ReadBatchingRuntimeContext;
import com.palantir.atlasdb.sweep.queue.clear.DefaultTableClearer;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static SweepQueue create(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            TransactionService transaction,
            TargetedSweepFollower follower,
            ReadBatchingRuntimeContext readBatchingRuntimeContext) {
        SweepQueueFactory factory =
                SweepQueueFactory.create(metrics, kvs, timelock, shardsConfig, transaction, readBatchingRuntimeContext);
        return new SweepQueue(factory, follower);
    }

    /**
     * Creates a SweepQueueWriter, performing all the necessary initialization.
     */
    public static MultiTableSweepQueueWriter createWriter(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            ReadBatchingRuntimeContext readBatchingRuntimeContext) {
        return SweepQueueFactory.create(metrics, kvs, timelock, shardsConfig, readBatchingRuntimeContext)
                .createWriter();
    }

    /**
     * Creates a supplier such that the first call to {@link Supplier#get()} on it will take the maximum of the runtime
     * configuration and the persisted number of shards, and persist and memoize the result. Subsequent calls will
     * return the cached value until refreshTimeMillis has passed, at which point the next call will again perform the
     * check and set.
     *
     * @param runtimeConfig     live reloadable runtime configuration for the number of shards
     * @param progress          progress table persisting the number of shards
     * @param refreshTimeMillis timeout for caching the number of shards
     * @return supplier calculating and persisting the number of shards to use
     */
    public static Supplier<Integer> createProgressUpdatingSupplier(
            Supplier<Integer> runtimeConfig, ShardProgress progress, long refreshTimeMillis) {
        return Suppliers.memoizeWithExpiration(
                () -> progress.updateNumberOfShards(runtimeConfig.get()), refreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        writer.enqueue(writes);
    }

    @Override
    public Optional<SweeperStrategy> getSweepStrategy(TableReference tableReference) {
        return writer.getSweepStrategy(tableReference);
    }

    /**
     * Sweep the next batch for the shard and strategy specified by shardStrategy, with the sweep timestamp sweepTs.
     * After successful deletes, the persisted information about the writes is removed, and progress is updated
     * accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @param sweepTs       sweep timestamp, the upper limit to the start timestamp of writes to sweep
     * @return number of cells that were swept
     */
    public long sweepNextBatch(ShardAndStrategy shardStrategy, long sweepTs) {
        metrics.updateSweepTimestamp(shardStrategy, sweepTs);
        long lastSweptTs = progress.getLastSweptTimestamp(shardStrategy);

        if (lastSweptTs + 1 >= sweepTs) {
            return 0L;
        }

        log.debug(
                "Beginning iteration of targeted sweep for {}, and sweep timestamp {}. Last previously swept "
                        + "timestamp for this shard and strategy was {}.",
                SafeArg.of("shardStrategy", shardStrategy.toText()),
                SafeArg.of("sweepTs", sweepTs),
                SafeArg.of("lastSweptTs", lastSweptTs));

        SweepBatchWithPartitionInfo batchWithInfo = reader.getNextBatchToSweep(shardStrategy, lastSweptTs, sweepTs);
        SweepBatch sweepBatch = batchWithInfo.sweepBatch();
        metrics.registerEntriesReadInBatch(shardStrategy, sweepBatch.entriesRead());

        deleter.sweep(sweepBatch.writes(), Sweeper.of(shardStrategy));

        if (!sweepBatch.isEmpty()) {
            log.debug(
                    "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardStrategy.toText()));
        }

        cleaner.clean(
                shardStrategy,
                batchWithInfo.partitionsForPreviousLastSweptTs(lastSweptTs),
                sweepBatch.lastSweptTimestamp(),
                sweepBatch.dedicatedRows());

        metrics.updateNumberOfTombstones(shardStrategy, sweepBatch.writes().size());
        metrics.updateProgressForShard(shardStrategy, sweepBatch.lastSweptTimestamp());

        if (sweepBatch.isEmpty()) {
            metrics.registerOccurrenceOf(shardStrategy, SweepOutcome.NOTHING_TO_SWEEP);
        } else {
            metrics.registerOccurrenceOf(shardStrategy, SweepOutcome.SUCCESS);
        }

        return sweepBatch.entriesRead();
    }

    public void resetSweepProgress() {
        int shards = getNumShards();
        log.info("Now attempting to reset sweep progress for both strategies...", SafeArg.of("numShards", shards));
        for (int shard = 0; shard < shards; shard++) {
            progress.resetProgressForShard(ShardAndStrategy.conservative(shard));
            progress.resetProgressForShard(ShardAndStrategy.thorough(shard));
        }
        log.info(
                "Sweep progress was reset for shards for both strategies. If you are running your service in an HA"
                        + " configuration, this message by itself does NOT mean that the reset is complete. The reset"
                        + " is only guaranteed to be complete after this message has been printed by ALL nodes.",
                SafeArg.of("numShards", shards));
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
        private final WriteInfoPartitioner partitioner;
        private final TargetedSweepMetrics metrics;
        private final KeyValueService kvs;
        private final TimelockService timelock;
        private final ReadBatchingRuntimeContext readBatchingRuntimeContext;

        private SweepQueueFactory(
                ShardProgress progress,
                Supplier<Integer> numShards,
                SweepableCells cells,
                SweepableTimestamps timestamps,
                WriteInfoPartitioner partitioner,
                TargetedSweepMetrics metrics,
                KeyValueService kvs,
                TimelockService timelock,
                ReadBatchingRuntimeContext readBatchingRuntimeContext) {
            this.progress = progress;
            this.numShards = numShards;
            this.cells = cells;
            this.timestamps = timestamps;
            this.partitioner = partitioner;
            this.metrics = metrics;
            this.kvs = kvs;
            this.timelock = timelock;
            this.readBatchingRuntimeContext = readBatchingRuntimeContext;
        }

        static SweepQueueFactory create(
                TargetedSweepMetrics metrics,
                KeyValueService kvs,
                TimelockService timelock,
                Supplier<Integer> shardsConfig,
                ReadBatchingRuntimeContext readBatchingRuntimeContext) {
            // It is OK that the transaction service is different from the one used by the transaction manager,
            // as transaction services must not hold any local state in them that would affect correctness.
            TransactionService transaction =
                    TransactionServices.createRaw(kvs, new TimelockTimestampServiceAdapter(timelock), false);
            return create(metrics, kvs, timelock, shardsConfig, transaction, readBatchingRuntimeContext);
        }

        static SweepQueueFactory create(
                TargetedSweepMetrics metrics,
                KeyValueService kvs,
                TimelockService timelock,
                Supplier<Integer> shardsConfig,
                TransactionService transaction,
                ReadBatchingRuntimeContext readBatchingRuntimeContext) {
            Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
            ShardProgress shardProgress = new ShardProgress(kvs);
            Supplier<Integer> shards =
                    createProgressUpdatingSupplier(shardsConfig, shardProgress, SweepQueueUtils.REFRESH_TIME);
            WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
            SweepableCells cells = new SweepableCells(kvs, partitioner, metrics, transaction);
            SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
            registerSweepableTimestampsAsSafe();
            return new SweepQueueFactory(
                    shardProgress,
                    shards,
                    cells,
                    timestamps,
                    partitioner,
                    metrics,
                    kvs,
                    timelock,
                    readBatchingRuntimeContext);
        }

        private static void registerSweepableTimestampsAsSafe() {
            LoggingArgs.registerSensitiveLoggingArgProducerForTable(
                    TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef(),
                    DefaultSensitiveLoggingArgProducers.ALWAYS_SAFE);
        }

        private SweepQueueWriter createWriter() {
            return new SweepQueueWriter(timestamps, cells, partitioner);
        }

        private SweepQueueReader createReader() {
            return new SweepQueueReader(timestamps, cells, readBatchingRuntimeContext);
        }

        private SweepQueueDeleter createDeleter(TargetedSweepFollower follower) {
            return new SweepQueueDeleter(kvs, follower, new DefaultTableClearer(kvs, timelock::getImmutableTimestamp));
        }

        private SweepQueueCleaner createCleaner() {
            return new SweepQueueCleaner(cells, timestamps, progress);
        }
    }
}
