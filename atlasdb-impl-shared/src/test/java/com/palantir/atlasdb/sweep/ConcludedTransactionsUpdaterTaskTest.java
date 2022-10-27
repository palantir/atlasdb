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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.knowledge.coordinated.CoordinationAwareKnownConcludedTransactionsStore;
import com.palantir.common.streams.KeyedStream;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConcludedTransactionsUpdaterTaskTest {
    private static final int NUM_SHARDS = 4;
    private static final Random RANDOM = new Random();
    private static final Set<ShardAndStrategy> shardsAndStrategies = computeShardsAndStrategies();

    private final CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore =
            mock(CoordinationAwareKnownConcludedTransactionsStore.class);
    private final ShardProgress shardProgress = mock(ShardProgress.class);
    private final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

    @Captor
    ArgumentCaptor<Set<ShardAndStrategy>> shardAndStrategyArgumentCaptor;

    @Test
    public void queriesAllShardsBeforeSupplementingConcludedTxnStore() {
        when(shardProgress.getLastSweptTimestamps(any())).thenReturn(generateLastSweptTs());

        ConcludedTransactionsUpdaterTask updaterTask = new ConcludedTransactionsUpdaterTask(
                () -> NUM_SHARDS, concludedTransactionsStore, shardProgress, executorService);

        updaterTask.runOneIteration();

        verify(shardProgress).getLastSweptTimestamps(shardAndStrategyArgumentCaptor.capture());
        assertThat(shardAndStrategyArgumentCaptor.getAllValues()).containsExactlyInAnyOrder(shardsAndStrategies);

        updaterTask.close();
    }

    @Test
    public void supplementsConcludedTxnStoreWithMinLastSweptTs() {
        Map<ShardAndStrategy, Long> lastSweptTs = generateLastSweptTs();
        long expectedMinTs =
                lastSweptTs.values().stream().min(Comparator.naturalOrder()).get();
        when(shardProgress.getLastSweptTimestamps(any())).thenReturn(lastSweptTs);

        ConcludedTransactionsUpdaterTask updaterTask = new ConcludedTransactionsUpdaterTask(
                () -> NUM_SHARDS, concludedTransactionsStore, shardProgress, executorService);

        updaterTask.runOneIteration();

        verify(concludedTransactionsStore).addConcludedTimestamps(Range.closed(1L, expectedMinTs));

        updaterTask.close();
    }

    @Test
    public void doesNotSupplementIfNumberOfShardsChanges() {
        when(shardProgress.getLastSweptTimestamps(any())).thenReturn(generateLastSweptTs());

        Supplier<Integer> shardSupplier = mock(Supplier.class);
        when(shardSupplier.get()).thenReturn(NUM_SHARDS).thenReturn(NUM_SHARDS * 2);

        ConcludedTransactionsUpdaterTask updaterTask = new ConcludedTransactionsUpdaterTask(
                shardSupplier, concludedTransactionsStore, shardProgress, executorService);

        updaterTask.runOneIteration();

        verifyNoMoreInteractions(concludedTransactionsStore);

        updaterTask.close();
    }

    @Test
    public void doesNotSupplementIfNothingSwept() {
        Supplier<Integer> shardSupplier = mock(Supplier.class);
        when(shardSupplier.get()).thenReturn(NUM_SHARDS).thenReturn(NUM_SHARDS * 2);

        ConcludedTransactionsUpdaterTask updaterTask = new ConcludedTransactionsUpdaterTask(
                shardSupplier, concludedTransactionsStore, shardProgress, executorService);

        updaterTask.runOneIteration();

        verifyNoMoreInteractions(concludedTransactionsStore);

        updaterTask.close();
    }

    private Map<ShardAndStrategy, Long> generateLastSweptTs() {
        return KeyedStream.of(shardsAndStrategies)
                .map(_idx -> (long) RANDOM.nextInt(100) + 1)
                .collectToMap();
    }

    private static Set<ShardAndStrategy> computeShardsAndStrategies() {
        ImmutableSet.Builder<ShardAndStrategy> shardAndStrategyBuilder = ImmutableSet.builder();
        for (int shard = 0; shard < NUM_SHARDS; shard++) {
            shardAndStrategyBuilder.add(
                    ShardAndStrategy.of(shard, SweeperStrategy.CONSERVATIVE),
                    ShardAndStrategy.of(shard, SweeperStrategy.THOROUGH));
        }
        return shardAndStrategyBuilder.build();
    }
}
