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

import com.palantir.atlasdb.sweep.asts.locks.Lockable;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockableComparator;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class DefaultSweepStateCoordinator implements SweepStateCoordinator {

    private final LockableFactory<SweepableBucket> lockableFactory;
    private final CandidateSweepableBucketRetriever candidateSweepableBucketRetriever;

    // TODO: What I really want is the ability to: Determine a random bucket (repeatedly, so maybe shuffle),
    //  and for it to be ordered, and to be able to remove things from consideration. I don't want either of those
    // iterations to be too expensive.
    // Probably some combination of a list and an index set but that's a pain to update.
    // If we do a single object, this could be a refreshable!
    private volatile SortedSet<Lockable<SweepableBucket>> currentBuckets;

    private DefaultSweepStateCoordinator(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory) {
        this.candidateSweepableBucketRetriever = candidateSweepableBucketRetriever;
        this.lockableFactory = lockableFactory;
        candidateSweepableBucketRetriever.subscribeToChanges(this::updateBuckets);
    }

    public static DefaultSweepStateCoordinator create(
            CandidateSweepableBucketRetriever candidateSweepableBucketRetriever,
            LockableFactory<SweepableBucket> lockableFactory) {
        return new DefaultSweepStateCoordinator(candidateSweepableBucketRetriever, lockableFactory);
    }

    @Override
    public SweepOutcome tryRunTaskWithBucket(Consumer<SweepableBucket> task) {
        Optional<Lockable<SweepableBucket>.Inner> maybeBucket = chooseBucket();
        if (maybeBucket.isEmpty()) {
            return SweepOutcome.NOTHING_AVAILABLE; // TODO: Do a check for nothing to sweep!
        }
        try (Lockable<SweepableBucket>.Inner bucket = maybeBucket.get()) {
            task.accept(bucket.getInner());

            // I'm not even convinced this is sufficient, given that the same requires lock might not be in the set
            // but it will be because of the factory guarantee that's not documented.
            // Maybe the whole versioned thing might be useful?!?!! But indexed is annoying.

            // Maybe the returned thing gives us the inner and the outer? rather than doing it on the inner object
            // directly.
            currentBuckets.remove(bucket.getOuter());
            return SweepOutcome.SWEPT;
        }
    }

    private Optional<Lockable<SweepableBucket>.Inner> chooseBucket() {
        Optional<Lockable<SweepableBucket>> firstObject = Optional.ofNullable(currentBuckets.first());
        if (firstObject.isEmpty()) {
            candidateSweepableBucketRetriever.requestUpdate();
            return Optional.empty();
        }

        return firstObject.flatMap(Lockable::tryLock).or(this::randomUnlockedBucket);
    }

    private Optional<Lockable<SweepableBucket>.Inner> randomUnlockedBucket() {
        List<Lockable<SweepableBucket>> shuffledList = new ArrayList<>(currentBuckets);
        Collections.shuffle(shuffledList);
        // We must show that this doesn't try locking everything!!!!
        return shuffledList.stream()
                .map(Lockable::tryLock)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private void updateBuckets(List<SweepableBucket> newBuckets) {
        currentBuckets = newBuckets.stream()
                .map(lockableFactory::createLockable)
                .collect(Collectors.toCollection(() ->
                        new ConcurrentSkipListSet<>(new LockableComparator<>(SweepableBucketComparator.INSTANCE))));
    }

    private enum SweepableBucketComparator implements Comparator<SweepableBucket> {
        INSTANCE;

        @Override
        public int compare(SweepableBucket o1, SweepableBucket o2) {
            int shardComparison = Integer.compare(
                    o1.shardAndStrategy().shard(), o2.shardAndStrategy().shard());
            if (shardComparison != 0) {
                return shardComparison;
            }
            return Long.compare(o1.bucketIdentifier(), o2.bucketIdentifier());
        }
    }
}
