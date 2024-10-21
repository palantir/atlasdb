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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepOutcome;
import com.palantir.atlasdb.sweep.asts.locks.Lockable;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockedItem;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepProgressMetrics;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultSweepStateCoordinatorTest {
    private static final SweepableBucket HEAD_BUCKET_SHARD_ZERO = bucket(0, 0);
    private static final SweepableBucket TAIL_BUCKET_SHARD_ZERO = bucket(0, 1);
    private static final Set<SweepableBucket> SWEEPABLE_BUCKETS_SHARD_ZERO =
            Set.of(HEAD_BUCKET_SHARD_ZERO, TAIL_BUCKET_SHARD_ZERO);
    private static final Set<SweepableBucket> HEAD_BUCKET_SHARD_ZERO_ONLY = Set.of(HEAD_BUCKET_SHARD_ZERO);
    private final SettableRefreshable<Set<SweepableBucket>> buckets = Refreshable.create(Set.of());
    private final TestCandidateSweepableBucketRetriever retriever = new TestCandidateSweepableBucketRetriever(buckets);
    private final AtomicReference<Function<List<Lockable<SweepableBucket>>, Stream<Lockable<SweepableBucket>>>>
            iterationOrderGenerator = new AtomicReference<>(Collection::stream);
    private LockableFactory<SweepableBucket> lockableFactory;

    @Mock
    private TimelockService timelockService;

    @Mock
    private TargetedSweepProgressMetrics progressMetrics;

    private SweepStateCoordinator coordinator;

    @BeforeEach
    public void beforeEach() {
        LockDescriptor descriptor = StringLockDescriptor.of("this doesn't really matter");
        lenient()
                .when(timelockService.lock(LockRequest.of(ImmutableSet.of(descriptor), 0L)))
                .thenReturn(LockResponse.successful(LockToken.of(UUID.randomUUID())));
        lockableFactory = LockableFactory.create(
                timelockService, Refreshable.create(Duration.ZERO), sweepableBucket -> descriptor);
        coordinator = new DefaultSweepStateCoordinator(
                retriever, lockableFactory, progressMetrics, buckets -> iterationOrderGenerator
                        .get()
                        .apply(buckets));
    }

    @Test
    public void selectsHeadOfShardsDeterminedAtRefresh() {
        int shards = 10;
        Set<SweepableBucket> firstBucketPerShard =
                // i, i to show that it's not just counting 0 as the head.
                IntStream.range(0, shards).mapToObj(i -> bucket(i, i)).collect(Collectors.toSet());
        Set<SweepableBucket> remainingBuckets =
                IntStream.range(0, shards).mapToObj(i -> bucket(i, i + 1)).collect(Collectors.toSet());

        Set<SweepableBucket> chosenBuckets = new HashSet<>();
        buckets.update(Sets.union(firstBucketPerShard, remainingBuckets));
        for (int i = 0; i < shards; i++) {
            coordinator.tryRunTaskWithBucket(chosenBuckets::add);
        }
        assertThat(chosenBuckets).containsExactlyInAnyOrderElementsOf(firstBucketPerShard);
    }

    @Test
    public void failedTaskUnlocksBucket() {
        buckets.update(HEAD_BUCKET_SHARD_ZERO_ONLY);

        RuntimeException exception = new RuntimeException("I failed");
        assertThatThrownBy(() -> coordinator.tryRunTaskWithBucket(chosenBucket -> {
                    throw exception;
                }))
                .isEqualTo(exception);

        assertThat(isBucketLocked(HEAD_BUCKET_SHARD_ZERO)).isFalse();
    }

    @Test
    public void selectsALockOnChosenBucket() {
        buckets.update(SWEEPABLE_BUCKETS_SHARD_ZERO);
        runTaskWithBucket(chosenBucket -> {
            assertThat(chosenBucket).isEqualTo(HEAD_BUCKET_SHARD_ZERO);
            assertThat(isBucketLocked(HEAD_BUCKET_SHARD_ZERO)).isTrue();
        });

        // We lock regardless of which set it comes from (this is really testing an implementation detail)
        runTaskWithBucket(chosenBucket -> {
            assertThat(chosenBucket).isEqualTo(TAIL_BUCKET_SHARD_ZERO);
            assertThat(isBucketLocked(TAIL_BUCKET_SHARD_ZERO)).isTrue();
        });
    }

    @Test
    public void removesFromCandidateSetOnceComplete() {
        buckets.update(SWEEPABLE_BUCKETS_SHARD_ZERO);
        runTaskWithBucket(chosenBucket -> assertThat(chosenBucket).isEqualTo(HEAD_BUCKET_SHARD_ZERO));
        runTaskWithBucket(chosenBucket -> assertThat(chosenBucket).isEqualTo(TAIL_BUCKET_SHARD_ZERO));
    }

    @Test
    public void removesFromCandidateSetIfRefreshMovesBucketToHeadSet() {
        buckets.update(SWEEPABLE_BUCKETS_SHARD_ZERO);
        runTaskWithBucket(chosenBucket -> assertThat(chosenBucket).isEqualTo(HEAD_BUCKET_SHARD_ZERO));

        runTaskWithBucket(chosenBucket -> {
            // The "tail" bucket will be in the non-head set
            assertThat(chosenBucket).isEqualTo(TAIL_BUCKET_SHARD_ZERO);

            // Now, the "tail" bucket is moved to the head set (provided the refresh completes during the iteration)
            buckets.update(Set.of(TAIL_BUCKET_SHARD_ZERO));
        });

        // We should have removed the "tail" bucket from the candidate set, even if it's moved to the headset.
        assertThat(coordinator.tryRunTaskWithBucket(chosenBucket -> {})).isEqualTo(SweepOutcome.NOTHING_TO_SWEEP);
    }

    @Test
    public void returnsNothingAvailableIfNoBucketUnlocked() {
        buckets.update(HEAD_BUCKET_SHARD_ZERO_ONLY);
        runTaskWithBucket(chosenBucket -> {
            // chosenBucket (the only bucket) is locked.
            assertThat(coordinator.tryRunTaskWithBucket(newBucket -> {
                        throw new RuntimeException("Should not have been called");
                    }))
                    .isEqualTo(SweepOutcome.NOTHING_AVAILABLE);
        });
    }

    @Test
    public void returnsNothingToSweepIfNoBucketsInCandidateSet() {
        assertThat(coordinator.tryRunTaskWithBucket(chosenBucket -> {})).isEqualTo(SweepOutcome.NOTHING_TO_SWEEP);
    }

    @Test
    public void requestsRefreshIfNoBucketsRemaining() {
        assertThat(retriever.getUpdateRequests()).isEqualTo(0);
        coordinator.tryRunTaskWithBucket(chosenBucket -> {
            throw new RuntimeException("Should not have been called");
        });
        assertThat(retriever.getUpdateRequests()).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("CheckReturnValue") // We don't care about the result of the lock, just that we're locking it.
    // but CheckReturnValue forces us to make that explicit.
    public void afterHeadBucketsLockedSelectsBucketBasedOnProvidedIterationOrderUntilFindsFirstUnlocked() {
        List<SweepableBucket> sweepableBuckets =
                IntStream.range(1, 10).mapToObj(i -> bucket(0, i)).collect(Collectors.toList());
        Collections.shuffle(sweepableBuckets);

        // The head bucket needs to go at the front since we're always going to visit the head of the sweep
        List<SweepableBucket> rawIterationOrder = Streams.concat(
                        Stream.of(HEAD_BUCKET_SHARD_ZERO), sweepableBuckets.stream())
                .collect(Collectors.toList());

        List<Lockable<SweepableBucket>> iterationOrder = rawIterationOrder.stream()
                .map(lockableFactory::createLockable)
                .map(Mockito::spy) // So that we can verify that we only try to lock the first few elements
                .collect(Collectors.toList());
        iterationOrderGenerator.set(_buckets -> iterationOrder.stream());

        int numberOfElementsToLockInAdvance = 5;
        for (int i = 0; i < numberOfElementsToLockInAdvance; i++) {
            Lockable<SweepableBucket> lockable = iterationOrder.get(i);
            lockable.tryLock(_ignored -> {});
            reset(lockable); // Reset the spy to make checking lock count easier
        }

        buckets.update(new HashSet<>(rawIterationOrder));
        runTaskWithBucket(chosenBucket ->
                assertThat(chosenBucket).isEqualTo(rawIterationOrder.get(numberOfElementsToLockInAdvance)));

        // Ensure that we only try the first 5 buckets (that are locked) + the 6th bucket (which is unlocked)
        // and none of the others.
        for (int i = 0; i < numberOfElementsToLockInAdvance + 1; i++) {
            verify(iterationOrder.get(i), times(1)).tryLock(any());
        }
        for (int i = numberOfElementsToLockInAdvance + 1; i < 10; i++) {
            verify(iterationOrder.get(i), times(0)).tryLock(any());
        }
    }

    @Test
    public void bucketIsUnlockedAfterTaskCompletes() {
        buckets.update(HEAD_BUCKET_SHARD_ZERO_ONLY);

        runTaskWithBucket(chosenBucket -> {});
        assertThat(isBucketLocked(HEAD_BUCKET_SHARD_ZERO)).isFalse();
    }

    @Test // A test that is specifically here to ensure that we're locking and selecting a bucket
    // lazily, rather than a bad bug where say getFirstUnlockedBucket locks everything and pulls the first
    // present value in the stream (and doesn't unlock them)
    public void doesNotLockAllBucketsWhenSelectingABucket() {
        Set<SweepableBucket> sweepableBuckets =
                IntStream.range(0, 20).mapToObj(i -> bucket(i, i)).collect(Collectors.toSet());

        buckets.update(sweepableBuckets);
        runTaskWithBucket(chosenBucket -> {
            verify(timelockService, times(1)).lock(any());

            // A little redundant with the above, but added to be explicit.
            sweepableBuckets.stream()
                    .filter(bucket -> !chosenBucket.equals(bucket))
                    .forEach(bucket -> {
                        assertThat(isBucketLocked(bucket)).isFalse();
                    });
        });
    }

    private boolean isBucketLocked(SweepableBucket bucket) {
        Optional<LockedItem<SweepableBucket>> item =
                lockableFactory.createLockable(bucket).tryLock(_ignored -> {});
        return item.stream().peek(LockedItem::close).findAny().isEmpty();
    }

    private static SweepableBucket bucket(int shard, int identifier) {
        return SweepableBucket.of(
                Bucket.of(ShardAndStrategy.of(shard, SweeperStrategy.CONSERVATIVE), identifier),
                TimestampRange.of(1, 3));
    }

    // When we have assertions _inside_ tryRunTaskWithBucket, it's possible for those tests to spuriously pass if
    // the task doesn't actually run (e.g., there's a bug that causes the method to return NOTHING_AVAILABLE
    // immediately).
    private void runTaskWithBucket(Consumer<SweepableBucket> task) {
        AtomicBoolean taskRan = new AtomicBoolean(false);
        assertThat(coordinator.tryRunTaskWithBucket(bucket -> {
                    taskRan.set(true);
                    task.accept(bucket);
                }))
                .isEqualTo(SweepOutcome.SWEPT);
        assertThat(taskRan).isTrue();
    }

    private static class TestCandidateSweepableBucketRetriever implements CandidateSweepableBucketRetriever {
        private final Refreshable<Set<SweepableBucket>> buckets;
        private final AtomicInteger updateRequests = new AtomicInteger(0);

        TestCandidateSweepableBucketRetriever(Refreshable<Set<SweepableBucket>> buckets) {
            this.buckets = buckets;
        }

        @Override
        public void requestUpdate() {
            updateRequests.incrementAndGet();
        }

        @Override
        public Disposable subscribeToChanges(Consumer<Set<SweepableBucket>> task) {
            // Not quite the same, since if we update buckets to be the same set, nothing will change.
            // but good enough for testing.
            return buckets.subscribe(task);
        }

        public int getUpdateRequests() {
            return updateRequests.get();
        }
    }
}
