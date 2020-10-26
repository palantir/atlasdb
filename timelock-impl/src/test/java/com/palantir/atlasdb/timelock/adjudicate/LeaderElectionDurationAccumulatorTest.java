/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.adjudicate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.timelock.feedback.LeaderElectionDuration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.Test;

public class LeaderElectionDurationAccumulatorTest {
    private static final UUID LEADER_1 = UUID.randomUUID();
    private static final UUID LEADER_2 = UUID.randomUUID();
    private static final UUID LEADER_3 = UUID.randomUUID();

    private LongConsumer mockConsumer = mock(LongConsumer.class);
    private LeaderElectionDurationAccumulator accumulator = new LeaderElectionDurationAccumulator(mockConsumer, 5);

    @Test
    public void nothingConsumedWithFewUpdates() {
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 4, 1);
        leaderElectionResultsWithDurationInRandomOrder(LEADER_2, LEADER_3, 4, 5);
        verifyNoInteractions(mockConsumer);
    }

    @Test
    public void consumeAfterFiveUpdatesWithMinDuration() {
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 5, 1);

        verify(mockConsumer).accept(1L);
        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void consumeOnlyOnceUsingMinFromFirstFiveUpdates() {
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 5, 5);
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 10, 1);

        verify(mockConsumer).accept(5L);
        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void consumeOnceForEachLeaderPair() {
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 5, 10);
        leaderElectionResultsWithDurationInRandomOrder(LEADER_2, LEADER_3, 5, 15);
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 5, 1);
        leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_3, 5, 7);

        verify(mockConsumer).accept(7L);
        verify(mockConsumer).accept(10L);
        verify(mockConsumer).accept(15L);
        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void testInterleavingNoPause() throws InterruptedException {
        ExecutorService executorService = PTExecutors.newFixedThreadPool(3);
        accumulator = new LeaderElectionDurationAccumulator(mockConsumer, 300);
        executorService.submit(() -> leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_2, 300, 1));
        executorService.submit(() -> leaderElectionResultsWithDurationInRandomOrder(LEADER_2, LEADER_3, 300, 17));
        executorService.submit(() -> leaderElectionResultsWithDurationInRandomOrder(LEADER_1, LEADER_3, 300, 76));
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        verify(mockConsumer).accept(1L);
        verify(mockConsumer).accept(17L);
        verify(mockConsumer).accept(76L);
        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void testInterleavingWithPause() throws InterruptedException {
        ExecutorService executorService = PTExecutors.newFixedThreadPool(3);
        accumulator = new LeaderElectionDurationAccumulator(mockConsumer, 300);
        executorService.submit(() -> leaderElectionResultsWithPause(LEADER_1, LEADER_2, 300, 1));
        executorService.submit(() -> leaderElectionResultsWithPause(LEADER_2, LEADER_3, 300, 17));
        executorService.submit(() -> leaderElectionResultsWithPause(LEADER_1, LEADER_3, 300, 76));
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        verify(mockConsumer).accept(1L);
        verify(mockConsumer).accept(17L);
        verify(mockConsumer).accept(76L);
        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void testManyUpdatesForSameLeaders() throws InterruptedException {
        ExecutorService executorService = PTExecutors.newFixedThreadPool(50);
        accumulator = new LeaderElectionDurationAccumulator(mockConsumer, 5_000);
        List<Integer> durations = IntStream.range(0, 100).map(x -> x * 100).boxed().collect(Collectors.toList());
        Collections.shuffle(durations);

        durations.forEach(duration -> executorService.submit(() -> leaderElectionResultsWithPause(LEADER_1, LEADER_2,
                100, duration)));

        for(int i = 0; i < 100; i++) {
            executorService.submit(() -> leaderElectionResultsWithPause(LEADER_1, LEADER_2, 100, 1));
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        verify(mockConsumer).accept(0L);
        verifyNoMoreInteractions(mockConsumer);
    }

    private void leaderElectionResultsWithDurationInRandomOrder(UUID oldLeader, UUID newLeader, int number, int min) {
        List<Long> durations = LongStream.range(min, min + number).boxed().collect(Collectors.toList());
        Collections.shuffle(durations);
        durations.forEach(dur -> accumulator.add(LeaderElectionDuration.of(oldLeader, newLeader, SafeLong.of(dur))));
    }

    private void leaderElectionResultsWithPause(UUID oldLeader, UUID newLeader, int number, int min) {
        List<Long> durations = LongStream.range(min, min + number).boxed().collect(Collectors.toList());
        Collections.shuffle(durations);
        durations.forEach(dur -> {
            accumulator.add(LeaderElectionDuration.of(oldLeader, newLeader, SafeLong.of(dur)));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        });
    }
}
