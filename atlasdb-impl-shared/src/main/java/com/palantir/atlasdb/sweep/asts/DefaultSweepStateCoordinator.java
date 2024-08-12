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

public final class DefaultSweepStateCoordinator implements SweepStateCoordinator {
    private final LockableFactory<SweepableBucket> lockableFactory;
    private final CandidateSweepableBucketRetriever candidateSweepableBucketRetriever;

    // Exists to facilitate testing, and to make switching out the shuffle strategy easier.
    private final Function<List<Lockable<SweepableBucket>>, Stream<Lockable<SweepableBucket>>> iterationOrderGenerator;

    private volatile Set<Lockable<SweepableBucket>> seenBuckets;
    private volatile List<Lockable<SweepableBucket>> firstBucketsOfEachShard;
    private volatile List<Lockable<SweepableBucket>> remainingBuckets;

    @VisibleForTesting
    DefaultSweepStateCoordinator(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory,
            Function<List<Lockable<SweepableBucket>>, Stream<Lockable<SweepableBucket>>> iterationOrderGenerator) {
        this.candidateSweepableBucketRetriever = candidateSweepableBucketRetriever;
        this.lockableFactory = lockableFactory;
        this.iterationOrderGenerator = iterationOrderGenerator;
        candidateSweepableBucketRetriever.subscribeToChanges(this::updateBuckets);
    }

    public static DefaultSweepStateCoordinator create(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory) {
        return new DefaultSweepStateCoordinator(
                candidateSweepableBucketRetriever,
                lockableFactory,
                buckets -> Streams.stream(new ProbingRandomIterator<>(buckets)));
    }

    @Override
    public SweepOutcome tryRunTaskWithBucket(Consumer<SweepableBucket> task) {
        // This is a loose check - since the variables are not updated atomically, seenBuckets could represent
        // buckets from a previous update. This doesn't really matter, since we _may_ have one NOTHING_TO_SWEEP,
        // and then once seenBuckets has updated, things will be fine again.
        if (seenBuckets.size() >= (firstBucketsOfEachShard.size() + remainingBuckets.size())) {
            candidateSweepableBucketRetriever.requestUpdate();
            return SweepOutcome.NOTHING_TO_SWEEP;
        }
        // It's possible that we pass the above check, but then there's a refresh and now there's no bucket
        // available. This means we'll return NOTHING_AVAILABLE instead of NOTHING_TO_SWEEP, but the accuracy of
        // the return value is not critical unless the wrong result is sustained.

        Optional<LockedItem<SweepableBucket>> maybeBucket = chooseBucket();
        if (maybeBucket.isEmpty()) {
            return SweepOutcome.NOTHING_AVAILABLE;
        }
        try (LockedItem<SweepableBucket> bucket = maybeBucket.get()) {
            task.accept(bucket.getItem());
        }
        return SweepOutcome.SWEPT;
    }

    private Optional<LockedItem<SweepableBucket>> chooseBucket() {
        return getFirstUnlockedBucket(firstBucketsOfEachShard.stream()).or(this::randomUnlockedBucket);
    }

    private Optional<LockedItem<SweepableBucket>> randomUnlockedBucket() {
        return getFirstUnlockedBucket(iterationOrderGenerator.apply(remainingBuckets));
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
                        bucket -> bucket.shardAndStrategy().shard()));

        List<Lockable<SweepableBucket>> headElements = partition.values().stream()
                .filter(list -> !list.isEmpty())
                .map(list -> list.get(0))
                .map(lockableFactory::createLockable)
                .collect(Collectors.toUnmodifiableList());

        List<Lockable<SweepableBucket>> otherElements = partition.values().stream()
                .flatMap(list -> list.stream().skip(1))
                .map(lockableFactory::createLockable)
                .collect(Collectors.toUnmodifiableList());

        // There's a delay between setting each variable, but we do not require (for correctness) that these three
        // variables are updated atomically.
        firstBucketsOfEachShard = headElements;
        remainingBuckets = otherElements;
        seenBuckets = ConcurrentHashMap.newKeySet();
    }

    private enum SweepableBucketComparator implements Comparator<SweepableBucket> {
        INSTANCE;

        @Override
        public int compare(SweepableBucket firstBucket, SweepableBucket secondBucket) {
            int shardComparison = Integer.compare(
                    firstBucket.shardAndStrategy().shard(),
                    secondBucket.shardAndStrategy().shard());
            if (shardComparison != 0) {
                return shardComparison;
            }
            return Long.compare(firstBucket.bucketIdentifier(), secondBucket.bucketIdentifier());
        }
    }
}
