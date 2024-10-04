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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// TODO(mdaudali): make these tests abstract like the other tests for BucketProgress
public final class DefaultSweepAssignedBucketStoreTest {
    private final TestResourceManager testResourceManager = TestResourceManager.inMemory();
    private final KeyValueService keyValueService = testResourceManager.getDefaultKvs();
    private final DefaultSweepAssignedBucketStore store = DefaultSweepAssignedBucketStore.create(keyValueService);

    @BeforeEach
    public void before() {
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
        DefaultSweepAssignedBucketStore store = DefaultSweepAssignedBucketStore.create(spy);
        doThrow(new CheckAndSetException("Failed")).when(spy).checkAndSet(any());

        Bucket bucket = Bucket.of(ShardAndStrategy.of(12, SweeperStrategy.THOROUGH), 512);

        assertThatThrownBy(() -> store.updateStartingBucketForShardAndStrategy(bucket))
                .isInstanceOf(CheckAndSetException.class);
    }
}
