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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// This exists as an abstract class, because the semantics of the atomic table operations are tricky, and novel as
// part of the auto-scaling sweep project. We want to test these against the various DB implementations
public abstract class AbstractDefaultSweepAssignedBucketStoreTest {
    private final KeyValueService keyValueService;
    private final DefaultSweepAssignedBucketStore store;

    protected AbstractDefaultSweepAssignedBucketStoreTest(KvsManager kvsManager) {
        keyValueService = kvsManager.getDefaultKvs();
        store = DefaultSweepAssignedBucketStore.create(keyValueService);
    }

    @BeforeEach
    public void setUp() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), keyValueService);
        keyValueService.truncateTable(DefaultSweepAssignedBucketStore.TABLE_REF);
    }

    @Test
    public void getBucketStateAndIdentifierThrowsIfNoState() {
        assertThatLoggableExceptionThrownBy(store::getBucketStateAndIdentifier)
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("No bucket state and identifier found. This should have been bootstrapped during"
                        + " initialisation, and as such, is an invalid state.");
    }

    @Test
    public void setInitialStateCreatesStartingState() {
        long bucketIdentifier = 123;
        long startTimestamp = 456;
        store.setInitialStateForBucketAssigner(bucketIdentifier, startTimestamp);
        BucketStateAndIdentifier initialState =
                BucketStateAndIdentifier.of(bucketIdentifier, BucketAssignerState.start(startTimestamp));
        assertThat(store.getBucketStateAndIdentifier()).isEqualTo(initialState);
    }

    @Test
    public void cannotSetInitialStateWhenStateAlreadyExists() {
        store.setInitialStateForBucketAssigner(123, 456);
        assertThatThrownBy(() -> store.setInitialStateForBucketAssigner(789, 101112))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void updateStateMachineForBucketAssignerFailsIfInitialDoesNotMatchExisting() {
        store.setInitialStateForBucketAssigner(123, 456);
        BucketStateAndIdentifier incorrectInitialState =
                BucketStateAndIdentifier.of(123, BucketAssignerState.start(789));
        BucketStateAndIdentifier newState = BucketStateAndIdentifier.of(123, BucketAssignerState.start(101112));
        assertThatThrownBy(() -> store.updateStateMachineForBucketAssigner(incorrectInitialState, newState))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void updateStateMachineForBucketAssignerFailsIfNoExistingValuePresent() {
        BucketStateAndIdentifier unsetInitialState = BucketStateAndIdentifier.of(123, BucketAssignerState.start(456));
        BucketStateAndIdentifier newState = BucketStateAndIdentifier.of(123, BucketAssignerState.start(101112));
        assertThatThrownBy(() -> store.updateStateMachineForBucketAssigner(unsetInitialState, newState))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void updateStateMachineForBucketAssignerModifiesStateToNewIfOriginalMatchesExisting() {
        store.setInitialStateForBucketAssigner(123, 456);
        BucketStateAndIdentifier initialState = BucketStateAndIdentifier.of(123, BucketAssignerState.start(456));
        BucketStateAndIdentifier newState = BucketStateAndIdentifier.of(123, BucketAssignerState.start(101112));
        store.updateStateMachineForBucketAssigner(initialState, newState);
        assertThat(store.getBucketStateAndIdentifier()).isEqualTo(newState);
    }

    @Test
    public void getStartingBucketsForShardReturnsStartingBucketWhenSet() {
        List<Bucket> buckets = List.of(
                Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512),
                Bucket.of(ShardAndStrategy.of(54, SweeperStrategy.CONSERVATIVE), 154389),
                Bucket.of(ShardAndStrategy.of(25, SweeperStrategy.NON_SWEEPABLE), 97312907));

        buckets.forEach(store::updateStartingBucketForShardAndStrategy);

        assertThat(store.getStartingBucketsForShards(
                        buckets.stream().map(Bucket::shardAndStrategy).collect(Collectors.toSet())))
                .containsExactlyInAnyOrderElementsOf(buckets);
    }

    @Test
    public void getStartingBucketsForShardReturnsZeroBucketIdentifierForUnsetShards() {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        Bucket unsetBucket = Bucket.of(ShardAndStrategy.of(54, SweeperStrategy.CONSERVATIVE), 0);

        store.updateStartingBucketForShardAndStrategy(bucket);

        assertThat(store.getStartingBucketsForShards(Set.of(bucket.shardAndStrategy(), unsetBucket.shardAndStrategy())))
                .containsExactlyInAnyOrder(bucket, unsetBucket);
    }

    @Test
    public void updateStartingBucketForShardDoesNotDecreaseExistingValue() {
        Bucket existingBucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        Bucket newBucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 256);

        store.updateStartingBucketForShardAndStrategy(existingBucket);
        store.updateStartingBucketForShardAndStrategy(newBucket);

        assertThat(store.getStartingBucketsForShards(Set.of(existingBucket.shardAndStrategy())))
                .containsExactly(existingBucket);
    }

    @Test
    public void updateStartingBucketForShardSetsToNewValueIfGreaterThanExisting() {
        Bucket existingBucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        Bucket newBucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 1024);

        store.updateStartingBucketForShardAndStrategy(existingBucket);
        store.updateStartingBucketForShardAndStrategy(newBucket);

        assertThat(store.getStartingBucketsForShards(Set.of(existingBucket.shardAndStrategy())))
                .containsExactly(newBucket);
    }

    @Test
    public void updateStartingBucketForShardFailsAfterTooManyAttempts() {
        KeyValueService spy = spy(keyValueService);
        DefaultSweepAssignedBucketStore storeWithSpyKvs = DefaultSweepAssignedBucketStore.create(spy);
        doThrow(new CheckAndSetException("Failed")).when(spy).checkAndSet(any());

        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);

        assertThatThrownBy(() -> storeWithSpyKvs.updateStartingBucketForShardAndStrategy(bucket))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void getSweepableBucketsReturnsTimestampRangesforPresentBucketsForEachShardUpToTwoHundredBuckets() {
        ShardAndStrategy shardAndStrategy = ShardAndStrategy.of(12, SweeperStrategy.THOROUGH);
        List<SweepableBucket> buckets = IntStream.range(0, 400)
                .mapToObj(i -> SweepableBucket.of(Bucket.of(shardAndStrategy, i), TimestampRange.of(i, i + 1)))
                .collect(Collectors.toList());
        buckets.forEach(
                bucket -> store.putTimestampRangeForBucket(bucket.bucket(), Optional.empty(), bucket.timestampRange()));

        assertThat(store.getSweepableBuckets(Set.of(Bucket.of(shardAndStrategy, 0))))
                .containsExactlyInAnyOrderElementsOf(buckets.subList(0, 200));
    }

    @Test
    public void getSweepableBucketsSkipsBucketsWithMajorIdentifierBeforeStartBucketProvided() {
        ShardAndStrategy shardAndStrategy = ShardAndStrategy.of(12, SweeperStrategy.THOROUGH);
        List<SweepableBucket> buckets = IntStream.range(0, 200)
                .mapToObj(i -> SweepableBucket.of(Bucket.of(shardAndStrategy, i), TimestampRange.of(i, i + 1)))
                .collect(Collectors.toList());

        buckets.forEach(
                bucket -> store.putTimestampRangeForBucket(bucket.bucket(), Optional.empty(), bucket.timestampRange()));

        assertThat(store.getSweepableBuckets(Set.of(Bucket.of(shardAndStrategy, 100))))
                .containsExactlyInAnyOrderElementsOf(buckets.subList(100, 200));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 12, 199})
    public void getSweepableBucketsReturnsLessThanMaxBucketLimitIfLessThanTwoHundredBucketsPresent(
            int numberOfBuckets) {
        ShardAndStrategy shardAndStrategy = ShardAndStrategy.of(12, SweeperStrategy.THOROUGH);
        List<SweepableBucket> buckets = IntStream.range(0, numberOfBuckets)
                .mapToObj(i -> SweepableBucket.of(Bucket.of(shardAndStrategy, i), TimestampRange.of(i, i + 1)))
                .collect(Collectors.toList());

        buckets.forEach(
                bucket -> store.putTimestampRangeForBucket(bucket.bucket(), Optional.empty(), bucket.timestampRange()));

        assertThat(store.getSweepableBuckets(Set.of(Bucket.of(shardAndStrategy, 0))))
                .containsExactlyInAnyOrderElementsOf(buckets);
    }

    @Test
    public void getSweepableBucketsReturnsBucketsPerShardAndStrategy() {
        List<Bucket> startBuckets = List.of(
                Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 0),
                Bucket.of(ShardAndStrategy.of(54, SweeperStrategy.CONSERVATIVE), 1000),
                Bucket.of(ShardAndStrategy.of(25, SweeperStrategy.NON_SWEEPABLE), 5050));

        List<SweepableBucket> buckets = IntStream.range(0, 112) // 112 chosen arbitrarily below 200
                .mapToObj(i -> {
                    TimestampRange range = TimestampRange.of(i, i + 1);
                    return startBuckets.stream()
                            .map(bucket -> SweepableBucket.of(
                                    Bucket.of(bucket.shardAndStrategy(), bucket.bucketIdentifier() + i), range));
                })
                .flatMap(Function.identity())
                .collect(Collectors.toList());

        buckets.forEach(
                bucket -> store.putTimestampRangeForBucket(bucket.bucket(), Optional.empty(), bucket.timestampRange()));

        assertThat(store.getSweepableBuckets(new HashSet<>(startBuckets))).containsExactlyInAnyOrderElementsOf(buckets);
    }

    @Test
    public void putTimestampRangeForBucketFailsIfOldTimestampRangeDoesNotMatchCurrent() {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        TimestampRange oldTimestampRange = TimestampRange.of(0, 1); // Not actually set
        TimestampRange newTimestampRange = TimestampRange.of(1, 2);

        assertThatThrownBy(() ->
                        store.putTimestampRangeForBucket(bucket, Optional.of(oldTimestampRange), newTimestampRange))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void putTimestampRangeForBucketSucceedsIfOldTimestampRangeMatchesCurrent() {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        TimestampRange newTimestampRange = TimestampRange.of(1, 2);

        store.putTimestampRangeForBucket(bucket, Optional.empty(), newTimestampRange);
        Set<SweepableBucket> sweepableBuckets = store.getSweepableBuckets(Set.of(bucket));
        assertThat(sweepableBuckets).containsExactly(SweepableBucket.of(bucket, newTimestampRange));
    }

    @Test
    public void deleteBucketEntryDoesNotThrowIfBucketNotPresent() {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        assertThatCode(() -> store.deleteBucketEntry(bucket)).doesNotThrowAnyException();
    }

    @Test
    public void deleteBucketEntryDeletesBucket() {
        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);
        TimestampRange timestampRange = TimestampRange.of(1, 2);
        store.putTimestampRangeForBucket(bucket, Optional.empty(), timestampRange);
        store.deleteBucketEntry(bucket);
        assertThat(store.getSweepableBuckets(Set.of(bucket))).isEmpty();
    }

    @Test
    public void getTimestampRangeRecordThrowsIfRecordNotPresent() {
        assertThatLoggableExceptionThrownBy(() -> store.getTimestampRangeRecord(1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("No timestamp range record found for bucket identifier")
                .hasExactlyArgs(SafeArg.of("bucketIdentifier", 1L));
    }

    @Test
    public void putTimestampRangeRecordPutsRecord() {
        TimestampRange timestampRange = TimestampRange.of(1, 2);
        store.putTimestampRangeRecord(1, timestampRange);
        assertThat(store.getTimestampRangeRecord(1)).isEqualTo(timestampRange);
    }

    @Test
    public void putTimestampRangeRecordFailsIfRecordAlreadyExists() {
        TimestampRange timestampRange = TimestampRange.of(1, 2);
        store.putTimestampRangeRecord(1, timestampRange);
        assertThatThrownBy(() -> store.putTimestampRangeRecord(1, timestampRange))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void deleteTimestampRangeRecordDoesNotThrowIfRecordNotPresent() {
        assertThatCode(() -> store.deleteTimestampRangeRecord(1)).doesNotThrowAnyException();
    }

    @Test
    public void deleteTimestampRangeRecordDeletesRecord() {
        TimestampRange timestampRange = TimestampRange.of(1, 2);
        store.putTimestampRangeRecord(1, timestampRange);
        assertThat(store.getTimestampRangeRecord(1)).isEqualTo(timestampRange);

        store.deleteTimestampRangeRecord(1);
        assertThatLoggableExceptionThrownBy(() -> store.getTimestampRangeRecord(1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("No timestamp range record found for bucket identifier")
                .hasExactlyArgs(SafeArg.of("bucketIdentifier", 1L));
    }
}
