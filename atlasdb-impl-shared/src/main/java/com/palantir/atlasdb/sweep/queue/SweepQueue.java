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
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.NumberOfShardsProvider.MismatchBehaviour;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader.ReadBatchingRuntimeContext;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
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
            SweepQueueComponents components,
            KeyValueService keyValueService,
            TargetedSweepMetrics metrics,
            AbandonedTransactionConsumer abandonedTransactionConsumer) {
        this.progress = components.shardProgress();
        this.writer = components.writer();
        this.numShardsProvider = components.numberOfShardsProvider();
        this.sweeper = new DefaultSingleBatchSweeper(
                metrics,
                progress,
                abandonedTransactionConsumer,
                components.reader(),
                components.deleter(),
                components.cleaner());
        this.resetter = new DefaultSweepProgressResetter(
                keyValueService,
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
        SweepQueueComponents components = SweepQueueComponents.create(
                metrics,
                kvs,
                timelock,
                shardsConfig,
                transaction,
                readBatchingRuntimeContext,
                tablesToTrackDeletions,
                follower,
                MismatchBehaviour.UPDATE);
        return new SweepQueue(components, kvs, metrics, abortedTransactionConsumer);
    }

    /**
     * Creates a SweepQueueWriter, performing all the necessary initialization.
     * Only used in tests (TODO(mdaudali): this will be removed soon.)
     */
    public static MultiTableSweepQueueWriter createWriter(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            ReadBatchingRuntimeContext readBatchingRuntimeContext,
            TargetedSweepFollower follower,
            MismatchBehaviour mismatchBehaviourForShards) {
        return SweepQueueComponents.create(
                        metrics,
                        kvs,
                        timelock,
                        shardsConfig,
                        readBatchingRuntimeContext,
                        follower,
                        mismatchBehaviourForShards)
                .writer();
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
}
