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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.transaction.knowledge.CoordinationAwareKnownConcludedTransactionsStore;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConcludedTransactionsUpdaterTask implements AutoCloseable {
    private final Set<ShardAndStrategy> allShardsAndStrategies;
    private final ShardProgress progress;
    private final CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore;
    private final ScheduledExecutorService executor;

    private ConcludedTransactionsUpdaterTask(
            Set<ShardAndStrategy> allShardsAndStrategies,
            CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore,
            ShardProgress progress,
            ScheduledExecutorService executorService) {
        this.allShardsAndStrategies = allShardsAndStrategies;
        this.concludedTransactionsStore = concludedTransactionsStore;
        this.progress = progress;
        this.executor = executorService;
    }

    public static ConcludedTransactionsUpdaterTask create(
            int numShards,
            CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore,
            ShardProgress progress,
            ScheduledExecutorService executor) {
        ConcludedTransactionsUpdaterTask task = new ConcludedTransactionsUpdaterTask(
                getAllShardsAndStrategies(numShards), concludedTransactionsStore, progress, executor);
        task.schedule();
        return task;
    }

    private void schedule() {
        executor.scheduleWithFixedDelay(
                this::runOneIteration,
                AtlasDbConstants.CONCLUDED_TRANSACTIONS_UPDATE_INITIAL_DELAY_MILLIS,
                AtlasDbConstants.CONCLUDED_TRANSACTIONS_UPDATE_TASK_DELAY_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void runOneIteration() {
        long minLastSweptTimestamp = allShardsAndStrategies.stream()
                .map(progress::getLastSweptTimestamp)
                .min(Comparator.naturalOrder())
                .orElse(1L);

        Range<Long> concludedTimestamps = Range.closed(1L, minLastSweptTimestamp);
        concludedTransactionsStore.supplement(concludedTimestamps);
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
    public void close() throws Exception {
        executor.shutdownNow();
    }
}
