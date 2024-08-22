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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.ShardedSweepTimestampManager.SweepTimestamps;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FaultTolerantShardedRetrievalStrategyTest {
    private static final ShardAndStrategy SHARD_AND_STRATEGY = ShardAndStrategy.of(12, SweeperStrategy.CONSERVATIVE);
    private static final SweepTimestamps SWEEP_TIMESTAMPS =
            SweepTimestamps.builder().sweepTimestamp(1).lastSweptTimestamp(21).build();
    private static final RuntimeException EXCEPTION = new RuntimeException("failed");

    @Mock
    private ShardedRetrievalStrategy delegate;

    @Mock
    private Consumer<ShardAndStrategy> failureTracker;

    private ShardedRetrievalStrategy strategy;

    @BeforeEach
    public void setup() {
        strategy = FaultTolerantShardedRetrievalStrategy.create(delegate, failureTracker);
    }

    @Test
    public void passesThroughSuccessfulRequest() {
        List<SweepableBucket> buckets = List.of(SweepableBucket.of(SHARD_AND_STRATEGY, 123L));
        when(delegate.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS))
                .thenReturn(buckets);
        assertThat(strategy.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS))
                .containsExactlyElementsOf(buckets);
        verifyNoInteractions(failureTracker);
    }

    @Test
    public void failedRequestsReturnEmptyList() {
        when(delegate.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS))
                .thenThrow(EXCEPTION);

        assertThat(strategy.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS))
                .isEmpty();
    }

    @Test
    public void failedRequestsAreTracked() {
        when(delegate.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS))
                .thenThrow(EXCEPTION);

        strategy.getSweepableBucketsForShard(SHARD_AND_STRATEGY, SWEEP_TIMESTAMPS);
        verify(failureTracker).accept(SHARD_AND_STRATEGY);
    }
}
