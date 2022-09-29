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

package com.palantir.atlasdb.sweep.metrics;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public class LastSweptTimestampUpdaterTest {
    private static final long REFRESH_MILLIS = 10L;
    private static final int TICK_COUNT = 5;
    private static final long CONS_TS = 100L;
    private static final long THOR_TS = 200L;
    private static final ShardAndStrategy CONS_SHARD = ShardAndStrategy.conservative(0);

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Parameterized.Parameters(name = "shards = {0}")
    public static Object[] shards() {
        return new Object[] {1, 8, 16};
    }

    @Mock
    private SweepQueue queue;

    @Mock
    private TargetedSweepMetrics metrics;

    private final int shards;
    private final Set<ShardAndStrategy> conservativeShardAndStrategySet;
    private final Map<ShardAndStrategy, Long> conservativeShardAndStrategyMap;
    private final Set<ShardAndStrategy> thoroughShardAndStrategySet;
    private final Map<ShardAndStrategy, Long> thoroughShardAndStrategyMap;
    private final DeterministicScheduler executorService = spy(new DeterministicScheduler());
    private LastSweptTimestampUpdater lastSweptTimestampUpdater;

    public LastSweptTimestampUpdaterTest(int shards) {
        this.shards = shards;
        this.conservativeShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.CONSERVATIVE);
        this.conservativeShardAndStrategyMap = buildShardAndStrategyMap(conservativeShardAndStrategySet, CONS_TS);
        this.thoroughShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.THOROUGH);
        this.thoroughShardAndStrategyMap = buildShardAndStrategyMap(thoroughShardAndStrategySet, THOR_TS);
    }

    @Before
    public void setUp() {
        lastSweptTimestampUpdater = new LastSweptTimestampUpdater(queue, metrics, executorService);
    }

    @Test
    public void unscheduledTaskDoesNotInteractWithExecutorService() {
        verifyNoInteractions(executorService);
    }

    @Test
    public void taskThrowsOnInvalidRefreshMillis() {
        assertThrows(SafeIllegalArgumentException.class, () -> lastSweptTimestampUpdater.schedule(0L));
        assertThrows(SafeIllegalArgumentException.class, () -> lastSweptTimestampUpdater.schedule(-REFRESH_MILLIS));
    }

    @Test
    public void scheduleCallSubmitsRunnableToExecutorServiceOnce() {
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        verify(executorService, times(1))
                .scheduleWithFixedDelay(any(), eq(REFRESH_MILLIS), eq(REFRESH_MILLIS), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduledTaskDoesNotInteractWithMetricsOrQueueBeforeDelayIsElapsed() {
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS - 1, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void scheduledTaskUpdatesProgressForShardsOnceAfterOneDelay() {
        stubWithRealisticReturnValues();
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        verify(queue, atLeastOnce()).getNumShards();
        verify(queue, atLeastOnce()).getLastSweptTimestamps(conservativeShardAndStrategySet);
        verify(queue, atLeastOnce()).getLastSweptTimestamps(thoroughShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++) {
            verify(metrics, times(1)).updateProgressForShard(ShardAndStrategy.conservative(shard), CONS_TS);
            verify(metrics, times(1)).updateProgressForShard(ShardAndStrategy.thorough(shard), THOR_TS);
        }
        verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void scheduledTaskInteractsWithMetricsAndQueueAsExpectedAfterMultipleDelays() {
        stubWithRealisticReturnValues();
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(TICK_COUNT * REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        verify(queue, atLeast(TICK_COUNT)).getNumShards();
        verify(queue, atLeast(TICK_COUNT)).getLastSweptTimestamps(conservativeShardAndStrategySet);
        verify(queue, atLeast(TICK_COUNT)).getLastSweptTimestamps(thoroughShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++) {
            verify(metrics, times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.conservative(shard), CONS_TS);
            verify(metrics, times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.thorough(shard), THOR_TS);
        }
        verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void scheduledTaskKeepsRunningAfterUpdateProgressForShardFails() {
        when(queue.getNumShards()).thenReturn(1);
        when(queue.getLastSweptTimestamps(Collections.singleton(CONS_SHARD)))
                .thenReturn(Collections.singletonMap(CONS_SHARD, CONS_TS));

        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);

        doThrow(RuntimeException.class)
                .doThrow(Error.class)
                .doNothing()
                .when(metrics)
                .updateProgressForShard(any(), anyLong());

        executorService.tick(3 * REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        verify(metrics, times(3)).updateProgressForShard(CONS_SHARD, CONS_TS);
    }

    private void stubWithRealisticReturnValues() {
        when(queue.getNumShards()).thenReturn(shards);
        when(queue.getLastSweptTimestamps(conservativeShardAndStrategySet)).thenReturn(conservativeShardAndStrategyMap);
        when(queue.getLastSweptTimestamps(thoroughShardAndStrategySet)).thenReturn(thoroughShardAndStrategyMap);
    }

    private static Map<ShardAndStrategy, Long> buildShardAndStrategyMap(
            Set<ShardAndStrategy> shardAndStrategySet, long timestamp) {
        return KeyedStream.of(shardAndStrategySet).map(_unused -> timestamp).collectToMap();
    }

    private Set<ShardAndStrategy> buildShardAndStrategySet(SweeperStrategy sweeperStrategy) {
        return IntStream.range(0, shards)
                .mapToObj(shard -> ShardAndStrategy.of(shard, sweeperStrategy))
                .collect(Collectors.toSet());
    }
}
