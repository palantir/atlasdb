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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultSweepableBucketRetrieverTest {
    @Mock
    SweepBucketPointerTable bucketPointerTable;

    @Mock
    SweepBucketsTable bucketsTable;

    @ParameterizedTest
    @MethodSource("numShardsAndStrategies")
    public void getSweepableBucketsRetrievesSweepableBucketsForAllShardsAndStrategiesRequested(TestContext context) {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(1, SweeperStrategy.CONSERVATIVE), 1);
        SweepableBucket sweepableBucket = SweepableBucket.of(bucket, TimestampRange.of(1, 2));

        when(bucketPointerTable.getStartingBucketsForShards(context.shardsAndStrategies()))
                .thenReturn(Set.of(bucket));
        when(bucketsTable.getSweepableBuckets(Set.of(bucket))).thenReturn(Set.of(sweepableBucket));

        SweepableBucketRetriever retriever = DefaultSweepableBucketRetriever.create(
                context.numShards(), context.strategies(), bucketPointerTable, bucketsTable);

        // Assumes the context calculation for shards and strategies has the same result as the
        // SweepableBucketRetriever.
        assertThat(retriever.getSweepableBuckets()).containsExactly(sweepableBucket);
    }

    @ParameterizedTest
    @MethodSource("emptyShardAndStrategies")
    public void getSweepableBucketsReturnsEmptyIfShardsAndStrategiesEmpty(TestContext context) {
        assertThat(context.shardsAndStrategies()).isEmpty();

        SweepableBucketRetriever retriever = DefaultSweepableBucketRetriever.create(
                context.numShards(), context.strategies(), bucketPointerTable, bucketsTable);

        assertThat(retriever.getSweepableBuckets()).isEmpty();
        verifyNoInteractions(bucketPointerTable, bucketsTable);
    }

    private static Stream<TestContext> numShardsAndStrategies() {
        return Stream.of(
                TestContext.of(1, List.of(SweeperStrategy.CONSERVATIVE)),
                TestContext.of(256, List.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH)));
    }

    private static Stream<TestContext> emptyShardAndStrategies() {
        return Stream.of(TestContext.of(0, List.of(SweeperStrategy.CONSERVATIVE)), TestContext.of(256, List.of()));
    }

    @Value.Immutable
    public interface TestContext {
        @Value.Parameter
        int numShards();

        @Value.Parameter
        List<SweeperStrategy> strategies();

        @Value.Derived
        default Set<ShardAndStrategy> shardsAndStrategies() {
            return IntStream.range(0, numShards())
                    .boxed()
                    .flatMap(shard -> strategies().stream().map(strategy -> ShardAndStrategy.of(shard, strategy)))
                    .collect(Collectors.toSet());
        }

        static TestContext of(int numShards, List<SweeperStrategy> strategies) {
            return ImmutableTestContext.of(numShards, strategies);
        }
    }
}
