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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultBucketRetrieverTests {
    @Mock
    private ShardProgress shardProgress;

    @Mock
    private SweepBucketsTable sweepBucketsTable;

    @Mock
    private SweepBucketPointerTable sweepBucketPointerTable;

    @Test
    public void bucketRetrieverRequestsForAllShards() {
        int shards = 10;
        SweeperStrategy sweeperStrategy = SweeperStrategy.CONSERVATIVE;
        Set<Bucket> expectedBuckets = setupMocks(shards, sweeperStrategy);

        BucketRetriever bucketRetriever = bucketRetriever(sweeperStrategy);
        bucketRetriever.getSweepableBuckets();

        verify(sweepBucketsTable).getSweepableBuckets(expectedBuckets);
    }

    @Test
    public void bucketRetrieverAlwaysUsesLatestNumberOfShards() {
        int shards = 10;
        SweeperStrategy sweeperStrategy = SweeperStrategy.CONSERVATIVE;
        Set<Bucket> firstExpectedBuckets = setupMocks(shards, sweeperStrategy);

        BucketRetriever bucketRetriever = bucketRetriever(sweeperStrategy);
        bucketRetriever.getSweepableBuckets();

        verify(sweepBucketsTable).getSweepableBuckets(firstExpectedBuckets);

        int newShards = 20;
        Set<Bucket> secondExpectedBuckets = setupMocks(newShards, sweeperStrategy);

        bucketRetriever.getSweepableBuckets();
        verify(sweepBucketsTable).getSweepableBuckets(secondExpectedBuckets);
    }

    @ParameterizedTest
    @EnumSource(SweeperStrategy.class)
    public void bucketRetrieverUsesCorrectStrategy(SweeperStrategy sweeperStrategy) {
        int shards = 10;
        Set<Bucket> expectedBuckets = setupMocks(shards, sweeperStrategy);

        BucketRetriever bucketRetriever = bucketRetriever(sweeperStrategy);
        bucketRetriever.getSweepableBuckets();

        verify(sweepBucketsTable).getSweepableBuckets(expectedBuckets);
    }

    private BucketRetriever bucketRetriever(SweeperStrategy strategy) {
        return DefaultBucketRetriever.create(shardProgress, sweepBucketsTable, sweepBucketPointerTable, strategy);
    }

    private Set<Bucket> setupMocks(int numberOfShards, SweeperStrategy strategy) {
        when(shardProgress.getNumberOfShards()).thenReturn(numberOfShards);
        Set<Bucket> expectedBuckets = generateBuckets(numberOfShards, strategy);
        when(sweepBucketPointerTable.getStartingBucketsForShards(generateShardAndStrategies(numberOfShards, strategy)))
                .thenReturn(expectedBuckets);
        return expectedBuckets;
    }

    private Set<ShardAndStrategy> generateShardAndStrategies(int numberOfShards, SweeperStrategy strategy) {
        return IntStream.range(0, numberOfShards)
                .mapToObj(i -> ShardAndStrategy.of(i, strategy))
                .collect(Collectors.toSet());
    }

    private Set<Bucket> generateBuckets(int numberOfShards, SweeperStrategy strategy) {
        return generateShardAndStrategies(numberOfShards, strategy).stream()
                .map(shardAndStrategy ->
                        Bucket.of(shardAndStrategy, ThreadLocalRandom.current().nextLong()))
                .collect(Collectors.toSet());
    }
}
