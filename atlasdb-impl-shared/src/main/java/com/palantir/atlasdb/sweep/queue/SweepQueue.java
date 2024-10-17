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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.NumberOfShardsProvider.MismatchBehaviour;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader.ReadBatchingRuntimeContext;
import com.palantir.atlasdb.sweep.queue.clear.DefaultTableClearer;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public final class SweepQueue implements MultiTableSweepQueueWriter {
    private static final SafeLogger log = SafeLoggerFactory.get(SweepQueue.class);
    private final ShardProgress progress;
    private final SweepQueueWriter writer;
    private final SingleBatchSweeper sweeper;
    private final NumberOfShardsProvider numShardsProvider;
    private final SweepProgressResetter resetter;

    private SweepQueue(
            SweepQueueFactory factory,
            TargetedSweepFollower follower,
            AbandonedTransactionConsumer abandonedTransactionConsumer) {
        this.progress = factory.progress;
        this.writer = factory.createWriter();
        this.numShardsProvider = NumberOfShardsProvider.createMemoizingProvider(
                progress, factory.numShards, MismatchBehaviour.UPDATE, Duration.ofMillis(SweepQueueUtils.REFRESH_TIME));
        this.sweeper = new DefaultSingleBatchSweeper(
                factory.metrics,
                progress,
                abandonedTransactionConsumer,
                factory.createReader(),
                factory.createDeleter(follower),
                factory.createCleaner());
        this.resetter = new DefaultSweepProgressResetter(
                factory.kvs,
                // TODO(mdaudali): Don't do this hackery - we'll likely move things around when the sweep queue dies
                TargetedSweepTableFactory.of().getSweepBucketProgressTable(null).getTableRef(),
                TargetedSweepTableFactory.of()
                        .getSweepAssignedBucketsTable(null)
                        .getTableRef(),
                progress,
                numShardsProvider::getNumberOfShards);
    }

    public static SweepQueue create(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            TransactionService transaction,
            AbandonedTransactionConsumer abortedTransactionConsumer,
            TargetedSweepFollower follower,
            ReadBatchingRuntimeContext readBatchingRuntimeContext,
            Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions) {
        SweepQueueFactory factory = SweepQueueFactory.create(
                metrics, kvs, timelock, shardsConfig, transaction, readBatchingRuntimeContext, tablesToTrackDeletions);
        return new SweepQueue(factory, follower, abortedTransactionConsumer);
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

    @Override
    public void enqueue(List<WriteInfo> writes) {
        writer.enqueue(writes);
    }

    @Override
    public SweeperStrategy getSweepStrategy(TableReference tableReference) {
        return writer.getSweepStrategy(tableReference);
    }

    public SingleBatchSweeper getSweeper() {
        return sweeper;
    }

    public void resetSweepProgress() {
        resetter.resetProgress(Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH));
    }

    public NumberOfShardsProvider getNumberOfShardsProvider() {
        return numShardsProvider;
    }

    public Map<ShardAndStrategy, Long> getLastSweptTimestamps(Set<ShardAndStrategy> shardAndStrategies) {
        return progress.getLastSweptTimestamps(shardAndStrategies);
    }

    public static final class SweepQueueFactory {
        private final ShardProgress progress;
        private final Supplier<Integer> numShards;
        private final SweepableCells cells;
        private final SweepableTimestamps timestamps;
        private final WriteInfoPartitioner partitioner;
        private final TargetedSweepMetrics metrics;
        private final KeyValueService kvs;
        private final TimelockService timelock;
        private final ReadBatchingRuntimeContext readBatchingRuntimeContext;
        private final Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions;

        private SweepQueueFactory(
                ShardProgress progress,
                Supplier<Integer> numShards,
                SweepableCells cells,
                SweepableTimestamps timestamps,
                WriteInfoPartitioner partitioner,
                TargetedSweepMetrics metrics,
                KeyValueService kvs,
                TimelockService timelock,
                ReadBatchingRuntimeContext readBatchingRuntimeContext,
                Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions) {
            this.progress = progress;
            this.numShards = numShards;
            this.cells = cells;
            this.timestamps = timestamps;
            this.partitioner = partitioner;
            this.metrics = metrics;
            this.kvs = kvs;
            this.timelock = timelock;
            this.readBatchingRuntimeContext = readBatchingRuntimeContext;
            this.tablesToTrackDeletions = tablesToTrackDeletions;
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
            return create(
                    metrics,
                    kvs,
                    timelock,
                    shardsConfig,
                    transaction,
                    readBatchingRuntimeContext,
                    _unused -> Optional.empty());
        }

        static SweepQueueFactory create(
                TargetedSweepMetrics metrics,
                KeyValueService kvs,
                TimelockService timelock,
                Supplier<Integer> shardsConfig,
                TransactionService transaction,
                ReadBatchingRuntimeContext readBatchingRuntimeContext,
                Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions) {
            Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
            ShardProgress shardProgress = new ShardProgress(kvs);
            Supplier<Integer> shards = NumberOfShardsProvider.createMemoizingProvider(
                    shardProgress,
                    shardsConfig,
                    MismatchBehaviour.UPDATE,
                    Duration.ofMillis(SweepQueueUtils.REFRESH_TIME))::getNumberOfShards;
            WriteInfoPartitioner partitioner = new WriteInfoPartitioner(kvs, shards);
            SweepableCells cells = new SweepableCells(kvs, partitioner, metrics, transaction);
            SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);
            return new SweepQueueFactory(
                    shardProgress,
                    shards,
                    cells,
                    timestamps,
                    partitioner,
                    metrics,
                    kvs,
                    timelock,
                    readBatchingRuntimeContext,
                    tablesToTrackDeletions);
        }

        private SweepQueueWriter createWriter() {
            return new SweepQueueWriter(timestamps, cells, partitioner);
        }

        private SweepQueueReader createReader() {
            return new SweepQueueReader(timestamps, cells, readBatchingRuntimeContext);
        }

        private SweepQueueDeleter createDeleter(TargetedSweepFollower follower) {
            return new SweepQueueDeleter(
                    kvs,
                    follower,
                    new DefaultTableClearer(kvs, timelock::getImmutableTimestamp),
                    tablesToTrackDeletions);
        }

        private SweepQueueCleaner createCleaner() {
            return new SweepQueueCleaner(cells, timestamps, progress);
        }
    }
}
