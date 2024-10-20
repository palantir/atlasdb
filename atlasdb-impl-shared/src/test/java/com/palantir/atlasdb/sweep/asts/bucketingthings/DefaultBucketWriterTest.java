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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketWriter.WriteState;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultBucketWriterTest {
    private static final List<SweeperStrategy> SWEEPER_STRATEGIES =
            ImmutableList.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);
    private static final int NUMBER_OF_SHARDS = 128;
    private static final long BUCKET_IDENTIFIER = 1;
    private static final TimestampRange OLD_TIMESTAMP_RANGE = TimestampRange.of(1, 3);
    private static final TimestampRange NEW_TIMESTAMP_RANGE = TimestampRange.of(4, 6);

    @Mock
    private SweepBucketsTable sweepBucketsTable;

    private BucketWriter bucketWriter;

    @BeforeEach
    public void beforeEach() {
        bucketWriter = DefaultBucketWriter.create(sweepBucketsTable, SWEEPER_STRATEGIES, NUMBER_OF_SHARDS);
    }

    @Test
    public void writesToAllBucketsInOrder() {
        assertThat(bucketWriter.writeToAllBuckets(
                        BUCKET_IDENTIFIER, Optional.of(OLD_TIMESTAMP_RANGE), NEW_TIMESTAMP_RANGE))
                .isEqualTo(WriteState.SUCCESS);
        assertAllBucketsUpdatedInOrder(Optional.of(OLD_TIMESTAMP_RANGE));
    }

    @Test
    // We mock "originally failing part way" by having the first N attempts throw a CAS exception,
    // as that would imply they're not what we originally expect them to be.
    public void successfullyWritesToAllBucketsAfterOriginallyFailingPartWay() {
        // number was arbitrarily chosen
        for (int i = 0; i <= 94; i++) {
            setupThrowCompareAndSetExceptionOnWrite(SweeperStrategy.CONSERVATIVE, i);
        }

        assertThat(bucketWriter.writeToAllBuckets(
                        BUCKET_IDENTIFIER, Optional.of(OLD_TIMESTAMP_RANGE), NEW_TIMESTAMP_RANGE))
                .isEqualTo(WriteState.SUCCESS);
        assertAllBucketsUpdatedInOrder(Optional.of(OLD_TIMESTAMP_RANGE));
    }

    @Test
    public void stopsAndBubblesUpNonCompareAndSetExceptionImmediately() {
        RuntimeException exception = new RuntimeException("failed");
        // Lenient because mockito gets concerned when you call with parameters outside of the mocked set.
        // as it's concerned it's a user error. When this is intentional, it instructs to mark as lenient.
        lenient()
                .doThrow(exception)
                .when(sweepBucketsTable)
                .putTimestampRangeForBucket(
                        // number arbitrarily chosen
                        Bucket.of(ShardAndStrategy.of(53, SweeperStrategy.CONSERVATIVE), BUCKET_IDENTIFIER),
                        Optional.of(OLD_TIMESTAMP_RANGE),
                        NEW_TIMESTAMP_RANGE);

        assertThatThrownBy(() -> bucketWriter.writeToAllBuckets(
                        BUCKET_IDENTIFIER, Optional.of(OLD_TIMESTAMP_RANGE), NEW_TIMESTAMP_RANGE))
                .isEqualTo(exception);

        // 54 is the number of shards in [0, 53]
        // We _try_ to CAS the failed one, hence we include the failed one.
        assertAllBucketsUpToShardForStrategyUpdatedInOrder(
                SweeperStrategy.CONSERVATIVE, 54, Optional.of(OLD_TIMESTAMP_RANGE));
    }

    @Test
    public void stopsImmediatelyAfterCompareAndSetExceptionThatDoesNotOccurAtStart() {
        // number arbitrarily chosen
        setupThrowCompareAndSetExceptionOnWrite(SweeperStrategy.THOROUGH, 18);
        assertThat(bucketWriter.writeToAllBuckets(
                        BUCKET_IDENTIFIER, Optional.of(OLD_TIMESTAMP_RANGE), NEW_TIMESTAMP_RANGE))
                .isEqualTo(WriteState.FAILED_CAS);

        // since it's the second strategy, we should have completed all of the conservative shards.
        assertAllBucketsUpToShardForStrategyUpdatedInOrder(
                SweeperStrategy.CONSERVATIVE, NUMBER_OF_SHARDS, Optional.of(OLD_TIMESTAMP_RANGE));

        // includes the failing shard because we did _try_ to update.
        assertAllBucketsUpToShardForStrategyUpdatedInOrder(
                SweeperStrategy.THOROUGH, 19, Optional.of(OLD_TIMESTAMP_RANGE));
    }

    @Test
    public void failingOnStartOfSecondStrategyDoesNotContinue() {
        setupThrowCompareAndSetExceptionOnWrite(SweeperStrategy.THOROUGH, 0);
        assertThat(bucketWriter.writeToAllBuckets(
                        BUCKET_IDENTIFIER, Optional.of(OLD_TIMESTAMP_RANGE), NEW_TIMESTAMP_RANGE))
                .isEqualTo(WriteState.FAILED_CAS);

        assertAllBucketsUpToShardForStrategyUpdatedInOrder(
                SweeperStrategy.CONSERVATIVE, NUMBER_OF_SHARDS, Optional.of(OLD_TIMESTAMP_RANGE));
        assertAllBucketsUpToShardForStrategyUpdatedInOrder(
                SweeperStrategy.THOROUGH, 1, Optional.of(OLD_TIMESTAMP_RANGE));
    }

    @Test
    public void updatesEvenWithEmptyOriginalTimestampRange() {
        assertThat(bucketWriter.writeToAllBuckets(BUCKET_IDENTIFIER, Optional.empty(), NEW_TIMESTAMP_RANGE))
                .isEqualTo(WriteState.SUCCESS);
        assertAllBucketsUpdatedInOrder(Optional.empty());
    }

    private void setupThrowCompareAndSetExceptionOnWrite(SweeperStrategy strategy, int shard) {
        lenient()
                .doThrow(new CheckAndSetException("failed"))
                .when(sweepBucketsTable)
                .putTimestampRangeForBucket(
                        Bucket.of(ShardAndStrategy.of(shard, strategy), BUCKET_IDENTIFIER),
                        Optional.of(OLD_TIMESTAMP_RANGE),
                        NEW_TIMESTAMP_RANGE);
    }

    private void assertAllBucketsUpdatedInOrder(Optional<TimestampRange> original) {
        InOrder inOrder = inOrder(sweepBucketsTable);
        for (SweeperStrategy sweeperStrategy : SWEEPER_STRATEGIES) {
            assertAllBucketsUpToShardForStrategyUpdatedInOrder(inOrder, sweeperStrategy, NUMBER_OF_SHARDS, original);
        }
        verifyNoMoreInteractions(sweepBucketsTable);
    }

    private void assertAllBucketsUpToShardForStrategyUpdatedInOrder(
            SweeperStrategy strategy, int numberOfShards, Optional<TimestampRange> original) {
        InOrder inOrder = inOrder(sweepBucketsTable);
        assertAllBucketsUpToShardForStrategyUpdatedInOrder(inOrder, strategy, numberOfShards, original);
    }

    private void assertAllBucketsUpToShardForStrategyUpdatedInOrder(
            InOrder inOrder, SweeperStrategy strategy, int numberOfShards, Optional<TimestampRange> original) {
        for (int i = 0; i < numberOfShards; i++) {
            inOrder.verify(sweepBucketsTable)
                    .putTimestampRangeForBucket(
                            Bucket.of(ShardAndStrategy.of(i, strategy), BUCKET_IDENTIFIER),
                            original,
                            NEW_TIMESTAMP_RANGE);
        }
    }
}
