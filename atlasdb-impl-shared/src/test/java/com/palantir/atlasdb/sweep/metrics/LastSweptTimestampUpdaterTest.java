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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LastSweptTimestampUpdaterTest {
    private static final String PARAMETERIZED_TEST_NAME = "shards = {0}";

    private static final long REFRESH_MILLIS = 10L;
    private static final int TICK_COUNT = 5;
    private static final long CONS_TS = 100L;
    private static final long THOR_TS = 200L;
    private static final ShardAndStrategy CONS_SHARD = ShardAndStrategy.conservative(0);

    public static List<Integer> numberOfShards() {
        return List.of(1, 8, 16);
    }

    @Mock
    private SweepQueue queue;

    @Mock
    private TargetedSweepMetrics metrics;

    private Set<ShardAndStrategy> conservativeShardAndStrategySet;
    private Map<ShardAndStrategy, Long> conservativeShardAndStrategyMap;
    private Set<ShardAndStrategy> thoroughShardAndStrategySet;
    private Map<ShardAndStrategy, Long> thoroughShardAndStrategyMap;

    private final Set<ShardAndStrategy> noneShardAndStrategySet = ImmutableSet.of(ShardAndStrategy.nonSweepable());

    private final Map<ShardAndStrategy, Long> noneShardAndStrategyMap =
            ImmutableMap.of(ShardAndStrategy.nonSweepable(), CONS_TS);
    private final DeterministicScheduler executorService = spy(new DeterministicScheduler());
    private LastSweptTimestampUpdater lastSweptTimestampUpdater;

    @BeforeEach
    public void beforeEach() {
        lastSweptTimestampUpdater = new LastSweptTimestampUpdater(queue, metrics, executorService);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void unscheduledTaskDoesNotInteractWithExecutorService(int shards) {
        setup(shards);
        verifyNoInteractions(executorService);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void taskThrowsOnInvalidRefreshMillis(int shards) {
        setup(shards);
        assertThatThrownBy(() -> lastSweptTimestampUpdater.schedule(0L)).isInstanceOf(SafeIllegalArgumentException.class);
        assertThatThrownBy(() -> lastSweptTimestampUpdater.schedule(-REFRESH_MILLIS)).isInstanceOf(SafeIllegalArgumentException.class);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void scheduleCallSubmitsRunnableToExecutorServiceOnce(int shards) {
        setup(shards);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        verify(executorService, times(1))
                .scheduleWithFixedDelay(any(), eq(REFRESH_MILLIS), eq(REFRESH_MILLIS), eq(TimeUnit.MILLISECONDS));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void scheduledTaskDoesNotInteractWithMetricsOrQueueBeforeDelayIsElapsed(int shards) {
        setup(shards);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS - 1, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(queue, metrics);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void scheduledTaskUpdatesProgressForShardsOnceAfterOneDelay(int shards) {
        setup(shards);
        stubWithRealisticReturnValues(shards);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        verify(queue, atLeastOnce()).getNumShards(any());
        verify(queue, atLeastOnce()).getLastSweptTimestamps(conservativeShardAndStrategySet);
        verify(queue, atLeastOnce()).getLastSweptTimestamps(thoroughShardAndStrategySet);
        verify(queue, atLeastOnce()).getLastSweptTimestamps(noneShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++) {
            verify(metrics, times(1)).updateProgressForShard(ShardAndStrategy.conservative(shard), CONS_TS);
            verify(metrics, times(1)).updateProgressForShard(ShardAndStrategy.thorough(shard), THOR_TS);
        }
        verify(metrics, times(1)).updateProgressForShard(ShardAndStrategy.nonSweepable(), CONS_TS);
        verifyNoMoreInteractions(queue, metrics);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void scheduledTaskInteractsWithMetricsAndQueueAsExpectedAfterMultipleDelays(int shards) {
        setup(shards);
        stubWithRealisticReturnValues(shards);
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(TICK_COUNT * REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        verify(queue, atLeast(TICK_COUNT)).getNumShards(any());
        verify(queue, atLeast(TICK_COUNT)).getLastSweptTimestamps(conservativeShardAndStrategySet);
        verify(queue, atLeast(TICK_COUNT)).getLastSweptTimestamps(thoroughShardAndStrategySet);
        verify(queue, atLeast(TICK_COUNT)).getLastSweptTimestamps(noneShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++) {
            verify(metrics, times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.conservative(shard), CONS_TS);
            verify(metrics, times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.thorough(shard), THOR_TS);
        }
        verify(metrics, times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.nonSweepable(), CONS_TS);

        verifyNoMoreInteractions(queue, metrics);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("numberOfShards")
    public void scheduledTaskKeepsRunningAfterUpdateProgressForShardFails(int shards) {
        setup(shards);
        setNumShardsMock(1);
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

    private void setup(int shards) {
        this.conservativeShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.CONSERVATIVE, shards);
        this.conservativeShardAndStrategyMap = buildShardAndStrategyMap(conservativeShardAndStrategySet, CONS_TS);
        this.thoroughShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.THOROUGH, shards);
        this.thoroughShardAndStrategyMap = buildShardAndStrategyMap(thoroughShardAndStrategySet, THOR_TS);
    }

    private void stubWithRealisticReturnValues(int shards) {
        setNumShardsMock(shards);
        when(queue.getLastSweptTimestamps(conservativeShardAndStrategySet)).thenReturn(conservativeShardAndStrategyMap);
        when(queue.getLastSweptTimestamps(thoroughShardAndStrategySet)).thenReturn(thoroughShardAndStrategyMap);
        when(queue.getLastSweptTimestamps(noneShardAndStrategySet)).thenReturn(noneShardAndStrategyMap);
    }

    private static Map<ShardAndStrategy, Long> buildShardAndStrategyMap(
            Set<ShardAndStrategy> shardAndStrategySet, long timestamp) {
        return KeyedStream.of(shardAndStrategySet).map(_unused -> timestamp).collectToMap();
    }

    private Set<ShardAndStrategy> buildShardAndStrategySet(SweeperStrategy sweeperStrategy, int shards) {
        return IntStream.range(0, shards)
                .mapToObj(shard -> ShardAndStrategy.of(shard, sweeperStrategy))
                .collect(Collectors.toSet());
    }

    private void setNumShardsMock(int num) {
        when(queue.getNumShards(any())).thenAnswer(invocation -> {
            if (invocation.getArguments()[0] == SweeperStrategy.NON_SWEEPABLE) {
                return 1;
            }
            return num;
        });
    }
}
