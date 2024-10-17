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

package com.palantir.atlasdb.sweep.asts;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketAssignerStateMachineTable;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class InitialBucketAssignerStateMachineBootstrapTaskTest {
    @Mock
    private ShardProgress progress;

    @Mock
    private SweepBucketAssignerStateMachineTable stateMachineTable;

    private final AtomicLong freshTimestampSupplier = new AtomicLong(0);
    private InitialBucketAssignerStateMachineBootstrapTask task;

    @BeforeEach
    public void setUp() {
        task = new InitialBucketAssignerStateMachineBootstrapTask(
                progress, stateMachineTable, freshTimestampSupplier::get);
    }

    @Test
    public void doesNothingIfAlreadyBootstrapped() {
        when(stateMachineTable.doesStateMachineStateExist()).thenReturn(true);
        task.run();

        verifyNoInteractions(progress);
        verifyNoMoreInteractions(stateMachineTable);
    }

    @ParameterizedTest
    @ValueSource(longs = {SweepQueueUtils.RESET_TIMESTAMP, SweepQueueUtils.INITIAL_TIMESTAMP})
    public void allInitialOrResetLastSweptTimestampAutoClosesInitialBucketFromZeroToFreshTimestamp(
            long lastSweptTimestamp) {
        when(progress.getNumberOfShards()).thenReturn(123);
        when(progress.getLastSweptTimestamps(allShardsAndStrategies(123)))
                .thenReturn(allShardsAndStrategiesToSameTimestamp(123, lastSweptTimestamp));
        freshTimestampSupplier.set(123456789L);

        task.run();

        verify(stateMachineTable)
                .setInitialStateForBucketAssigner(
                        BucketAssignerState.immediatelyClosing(0, freshTimestampSupplier.get()));
    }

    @Test
    public void allSameLastSweptTimestampNotInitialOrResetCreatesOpenInitialBucketFromThatTimestamp() {
        when(progress.getNumberOfShards()).thenReturn(62);
        when(progress.getLastSweptTimestamps(allShardsAndStrategies(62)))
                .thenReturn(allShardsAndStrategiesToSameTimestamp(62, 12));

        task.run();

        verify(stateMachineTable).setInitialStateForBucketAssigner(BucketAssignerState.start(12));
    }

    @Test
    public void differentLastSweptTimestampsCreatesOpenInitialBucketFromEarliestTimestamp() {
        when(progress.getNumberOfShards()).thenReturn(1);
        when(progress.getLastSweptTimestamps(allShardsAndStrategies(1)))
                .thenReturn(Map.of(
                        ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE), 123L,
                        ShardAndStrategy.of(0, SweeperStrategy.THOROUGH), 456L));
        freshTimestampSupplier.set(123456789L);

        task.run();

        verify(stateMachineTable).setInitialStateForBucketAssigner(BucketAssignerState.start(123L));
    }

    @Test
    public void anyShardWithInitialLastSweptTimestampCausesAutoClosedBucketFromZeroToFreshTimestamp() {
        when(progress.getNumberOfShards()).thenReturn(1);
        when(progress.getLastSweptTimestamps(allShardsAndStrategies(1)))
                .thenReturn(Map.of(
                        ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE),
                        SweepQueueUtils.INITIAL_TIMESTAMP,
                        ShardAndStrategy.of(0, SweeperStrategy.THOROUGH),
                        456L));
        freshTimestampSupplier.set(6542141L);

        task.run();

        verify(stateMachineTable)
                .setInitialStateForBucketAssigner(
                        BucketAssignerState.immediatelyClosing(0, freshTimestampSupplier.get()));
    }

    private Set<ShardAndStrategy> allShardsAndStrategies(int numShards) {
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);
        return IntStream.range(0, numShards)
                .boxed()
                .flatMap(shard -> strategies.stream().map(strategy -> ShardAndStrategy.of(shard, strategy)))
                .collect(Collectors.toSet());
    }

    private Map<ShardAndStrategy, Long> allShardsAndStrategiesToSameTimestamp(int numShards, long timestamp) {
        return allShardsAndStrategies(numShards).stream()
                .collect(Collectors.toMap(Function.identity(), _unused -> timestamp));
    }
}
