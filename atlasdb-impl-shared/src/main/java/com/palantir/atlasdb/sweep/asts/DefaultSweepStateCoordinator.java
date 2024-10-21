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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.sweep.asts.locks.Lockable;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockedItem;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepProgressMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

public final class DefaultSweepStateCoordinator implements SweepStateCoordinator {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSweepStateCoordinator.class);
    private final LockableFactory<SweepableBucket> lockableFactory;
    private final CandidateSweepableBucketRetriever candidateSweepableBucketRetriever;

    // Exists to facilitate testing, and to make switching out the shuffle strategy easier.
    private final Function<List<Lockable<SweepableBucket>>, Stream<Lockable<SweepableBucket>>> iterationOrderGenerator;

    private volatile Set<Lockable<SweepableBucket>> seenBuckets = ConcurrentHashMap.newKeySet();
    private volatile BucketsLists bucketsLists = ImmutableBucketsLists.builder()
            .firstBucketsOfEachShard(List.of())
            .remainingBuckets(List.of())
            .build();

    @VisibleForTesting
    DefaultSweepStateCoordinator(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory,
            TargetedSweepProgressMetrics progressMetrics,
            Function<List<Lockable<SweepableBucket>>, Stream<Lockable<SweepableBucket>>> iterationOrderGenerator) {
        this.candidateSweepableBucketRetriever = candidateSweepableBucketRetriever;
        this.lockableFactory = lockableFactory;
        this.iterationOrderGenerator = iterationOrderGenerator;

        candidateSweepableBucketRetriever.subscribeToChanges(this::updateBuckets);
        progressMetrics.estimatedPendingNumberOfBucketsToBeSwept(() -> {
            BucketsLists currentBucketsLists = bucketsLists;
            return currentBucketsLists.firstBucketsOfEachShard().size()
                    + currentBucketsLists.remainingBuckets().size()
                    - seenBuckets.size();
        });
    }

    public static DefaultSweepStateCoordinator create(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory,
            TargetedSweepProgressMetrics progressMetrics) {
        return new DefaultSweepStateCoordinator(
                candidateSweepableBucketRetriever,
                lockableFactory,
                progressMetrics,
                buckets -> Streams.stream(new ProbingRandomIterator<>(buckets)));
    }

    @Override
    public SweepOutcome tryRunTaskWithBucket(Consumer<SweepableBucket> task) {
        // This is a loose check - since the variables are not updated atomically, seenBuckets could represent
        // buckets from a previous update. This doesn't really matter, since we _may_ have one NOTHING_TO_SWEEP,
        // and then once seenBuckets has updated, things will be fine again.

        // We grab the bucketsLists at the time this method is called so that we don't need to reason about
        // atomicity between the two lists within, but _not_ capturing seenBuckets is ideal so that we always remove
        // candidates from the _latest_ seen bucket set.
        BucketsLists currentBucketsLists = bucketsLists;
        if (seenBuckets.size()
                >= (currentBucketsLists.firstBucketsOfEachShard().size()
                        + currentBucketsLists.remainingBuckets().size())) {
            candidateSweepableBucketRetriever.requestUpdate();
            return SweepOutcome.NOTHING_TO_SWEEP;
        }

        Optional<LockedItem<SweepableBucket>> maybeBucket = chooseBucket(currentBucketsLists);
        if (maybeBucket.isEmpty()) {
            return SweepOutcome.NOTHING_AVAILABLE;
        }
        try (LockedItem<SweepableBucket> bucket = maybeBucket.get()) {
            task.accept(bucket.getItem());
        }
        return SweepOutcome.SWEPT;
    }

    private Optional<LockedItem<SweepableBucket>> chooseBucket(BucketsLists currentBucketsLists) {
        return getFirstUnlockedBucket(currentBucketsLists.firstBucketsOfEachShard().stream())
                .or(() -> randomUnlockedBucket(currentBucketsLists));
    }

    private Optional<LockedItem<SweepableBucket>> randomUnlockedBucket(BucketsLists currentBucketsLists) {
        return getFirstUnlockedBucket(iterationOrderGenerator.apply(currentBucketsLists.remainingBuckets()));
    }

    private Optional<LockedItem<SweepableBucket>> getFirstUnlockedBucket(Stream<Lockable<SweepableBucket>> lockables) {
        return lockables
                .filter(lockable -> !seenBuckets.contains(lockable))
                // This (1) is _not_ identical to seenBuckets::add (2), since (1) will use the set assigned to
                // seenBuckets at the time we call the dispose callback, while (2) will use the set assigned to
                // seenBuckets at the time we call tryLock (which may be an old set)
                // It doesn't really matter if we add it to an old set for correctness, but I'd prefer not holding a
                // reference to an old set so that it can be GC'd away.
                // Could alternatively use #clear, but assigning a new set is cheap and clear would be mutating
                // a concurrent set.
                .map(lockable -> lockable.tryLock(bucket -> seenBuckets.add(bucket)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private void updateBuckets(Set<SweepableBucket> newBuckets) {
        Map<Integer, List<SweepableBucket>> partition = newBuckets.stream()
                .sorted(SweepableBucketComparator.INSTANCE)
                .collect(Collectors.groupingBy(
                        bucket -> bucket.bucket().shardAndStrategy().shard()));

        List<Lockable<SweepableBucket>> firstBucketsOfEachShard = partition.values().stream()
                .filter(list -> !list.isEmpty())
                .map(list -> list.get(0))
                .map(lockableFactory::createLockable)
                .collect(Collectors.toUnmodifiableList());

        List<Lockable<SweepableBucket>> remainingBuckets = partition.values().stream()
                .flatMap(list -> list.stream().skip(1))
                .map(lockableFactory::createLockable)
                .collect(Collectors.toUnmodifiableList());

        // There's a delay between setting each variable, but we do not require (for correctness) that these two
        // variables are updated atomically.

        bucketsLists = ImmutableBucketsLists.builder()
                .firstBucketsOfEachShard(firstBucketsOfEachShard)
                .remainingBuckets(remainingBuckets)
                .build();
        seenBuckets = ConcurrentHashMap.newKeySet();
        log.info(
                "Updated sweepable buckets (first buckets per shard: {}, remaining buckets: {}).",
                SafeArg.of("firstBucketsOfEachShard", firstBucketsOfEachShard),
                SafeArg.of("remainingBuckets", remainingBuckets));
    }

    private enum SweepableBucketComparator implements Comparator<SweepableBucket> {
        INSTANCE;

        @Override
        public int compare(SweepableBucket firstBucket, SweepableBucket secondBucket) {
            int shardComparison = Integer.compare(
                    firstBucket.bucket().shardAndStrategy().shard(),
                    secondBucket.bucket().shardAndStrategy().shard());
            if (shardComparison != 0) {
                return shardComparison;
            }
            return Long.compare(
                    firstBucket.bucket().bucketIdentifier(),
                    secondBucket.bucket().bucketIdentifier());
            // We're explicitly not comparing timestamp range, because it's irrelevant to the algorithm
        }
    }

    @Value.Immutable
    interface BucketsLists {
        List<Lockable<SweepableBucket>> firstBucketsOfEachShard();

        List<Lockable<SweepableBucket>> remainingBuckets();
    }
}
