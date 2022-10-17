/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.coordinated.CoordinationAwareKnownConcludedTransactionsStore;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConcludedTransactionsUpdaterTask implements AutoCloseable {
    public static final int CONCLUDED_TRANSACTIONS_UPDATE_INITIAL_DELAY_MILLIS = 1000;
    public static final int CONCLUDED_TRANSACTIONS_UPDATE_TASK_DELAY_MILLIS = 5000;

    // This has to be ShardProgress#getNumberOfShards(). Not using the persisted value here can lead to SEVERE DATA
    // CORRUPTION.
    private final Supplier<Integer> persistedNumShardsSupplier;

    private final ShardProgress progress;
    private final CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore;
    private final ScheduledExecutorService executor;

    private Set<ShardAndStrategy> shardsAndStrategies;
    private int lastKnownNumShards = -1;

    @VisibleForTesting
    ConcludedTransactionsUpdaterTask(
            Supplier<Integer> persistedNumShardsSupplier,
            CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore,
            ShardProgress progress,
            ScheduledExecutorService executorService) {
        this.persistedNumShardsSupplier = persistedNumShardsSupplier;
        this.concludedTransactionsStore = concludedTransactionsStore;
        this.progress = progress;
        this.executor = executorService;
    }

    public static ConcludedTransactionsUpdaterTask create(
            CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore,
            ShardProgress progress,
            ScheduledExecutorService executor) {
        ConcludedTransactionsUpdaterTask task = new ConcludedTransactionsUpdaterTask(
                progress::getNumberOfShards, concludedTransactionsStore, progress, executor);
        // Todo(Snanda): consider just running this task on the sweep thread at the end
        task.schedule();
        return task;
    }

    private void schedule() {
        executor.scheduleWithFixedDelay(
                this::runOneIteration,
                CONCLUDED_TRANSACTIONS_UPDATE_INITIAL_DELAY_MILLIS,
                CONCLUDED_TRANSACTIONS_UPDATE_TASK_DELAY_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    void runOneIteration() {
        int numShardsAtStart = persistedNumShardsSupplier.get();

        maybeRefreshShardsAndStrategies(numShardsAtStart);

        long minLastSweptTimestamp = progress.getLastSweptTimestamps(shardsAndStrategies).values().stream()
                .min(Comparator.naturalOrder())
                .orElse(SweepQueueUtils.INITIAL_TIMESTAMP);

        if (minLastSweptTimestamp < TransactionConstants.LOWEST_POSSIBLE_START_TS
                || numShardsAtStart != persistedNumShardsSupplier.get()) {
            // Do not want to update knownConcludedTimestamps in case number of shards have changed to avoid
            // correctness issues. This check should be enough as we do not expect the number of shards to go down.
            return;
        }

        Range<Long> concludedTimestamps =
                Range.closed(TransactionConstants.LOWEST_POSSIBLE_START_TS, minLastSweptTimestamp);
        concludedTransactionsStore.supplement(concludedTimestamps);
    }

    private void maybeRefreshShardsAndStrategies(int currentNumShards) {
        if (currentNumShards != lastKnownNumShards) {
            shardsAndStrategies = getAllShardsAndStrategies(currentNumShards);
            lastKnownNumShards = currentNumShards;
        }
    }

    private static Set<ShardAndStrategy> getAllShardsAndStrategies(int numShards) {
        ImmutableSet.Builder<ShardAndStrategy> shardsAndStrategiesBuilder = ImmutableSet.builder();
        shardsAndStrategiesBuilder.addAll(getShardAndStrategies(numShards, SweeperStrategy.CONSERVATIVE));
        shardsAndStrategiesBuilder.addAll(getShardAndStrategies(numShards, SweeperStrategy.THOROUGH));
        return shardsAndStrategiesBuilder.build();
    }

    private static Set<ShardAndStrategy> getShardAndStrategies(int numShards, SweeperStrategy strategy) {
        return IntStream.range(0, numShards)
                .mapToObj(shard -> ShardAndStrategy.of(shard, strategy))
                .collect(Collectors.toSet());
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
