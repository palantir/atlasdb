/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.NumberOfShardsProvider.MismatchBehaviour;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader.ReadBatchingRuntimeContext;
import com.palantir.atlasdb.sweep.queue.clear.DefaultTableClearer;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.v2.TimelockService;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;

@Value.Immutable
public interface SweepQueueComponents {
    SweepQueueReader reader();

    SweepQueueWriter writer();

    SweepQueueDeleter deleter();

    SweepQueueCleaner cleaner();

    ShardProgress shardProgress();

    SweepableCells sweepableCells();

    SweepableTimestamps sweepableTimestamps();

    NumberOfShardsProvider numberOfShardsProvider();

    static SweepQueueComponents create(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            ReadBatchingRuntimeContext readBatchingRuntimeContext,
            TargetedSweepFollower follower,
            MismatchBehaviour mismatchBehaviourForShards,
            Supplier<Integer> rotationIntervalMinutesConfig) {
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
                _unused -> Optional.empty(),
                follower,
                mismatchBehaviourForShards,
                rotationIntervalMinutesConfig);
    }

    static SweepQueueComponents create(
            TargetedSweepMetrics metrics,
            KeyValueService kvs,
            TimelockService timelock,
            Supplier<Integer> shardsConfig,
            TransactionService transaction,
            ReadBatchingRuntimeContext readBatchingRuntimeContext,
            Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions,
            TargetedSweepFollower follower,
            MismatchBehaviour mismatchBehaviourForShards,
            Supplier<Integer> rotationIntervalMinutesConfig) {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
        ShardProgress shardProgress = new ShardProgress(kvs);

        NumberOfShardsProvider numberOfShardsProvider = NumberOfShardsProvider.createMemoizingProvider(
                shardProgress,
                shardsConfig,
                mismatchBehaviourForShards,
                Duration.ofMillis(SweepQueueUtils.REFRESH_TIME));
        Supplier<Integer> rotationIntervalMinutesProvider = Suppliers.memoizeWithExpiration(
                rotationIntervalMinutesConfig::get, Duration.ofMillis(SweepQueueUtils.REFRESH_TIME));

        WriteInfoPartitioner partitioner = new WriteInfoPartitioner(
                kvs, numberOfShardsProvider::getNumberOfShards, rotationIntervalMinutesProvider);
        SweepableCells cells = new SweepableCells(kvs, partitioner, metrics, transaction);
        SweepableTimestamps timestamps = new SweepableTimestamps(kvs, partitioner);

        SweepQueueReader reader = new SweepQueueReader(timestamps, cells, readBatchingRuntimeContext);
        SweepQueueWriter writer = new SweepQueueWriter(timestamps, cells, partitioner);
        SweepQueueDeleter deleter = new SweepQueueDeleter(
                kvs, follower, new DefaultTableClearer(kvs, timelock::getImmutableTimestamp), tablesToTrackDeletions);
        SweepQueueCleaner cleaner = new SweepQueueCleaner(cells, timestamps, shardProgress);
        return ImmutableSweepQueueComponents.builder()
                .reader(reader)
                .writer(writer)
                .deleter(deleter)
                .cleaner(cleaner)
                .shardProgress(shardProgress)
                .sweepableCells(cells)
                .sweepableTimestamps(timestamps)
                .numberOfShardsProvider(numberOfShardsProvider)
                .build();
    }
}
