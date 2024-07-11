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

import com.palantir.atlasdb.sweep.asts.locks.RequiresLock;
import com.palantir.atlasdb.sweep.asts.locks.RequiresLock.Inner;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public class DefaultSweepStateCoordinator implements SweepStateCoordinator {
    private final ScheduledExecutorService scheduledExecutorService;
    private final Refreshable<Duration> automaticSweepRefreshDelay;
    private final CandidateSweepableBucketRetriever candidateSweepableBucketRetriever;

    // TODO: What I really want is the ability to: Determine a random bucket (repeatedly, so maybe shuffle),
    //  and for it to be ordered, and to be able to remove things from consideration. I don't want either of those iterations to be too expensive.
    // Probably some combination of a list and an index set.
    private volatile List<RequiresLock<SweepableBucket>> currentBuckets;

    @Override
    public State tryRunTaskWithSweep(Consumer<SweepableBucket> task) {
        return null;
    }

    private Optional<RequiresLock<SweepableBucket>.Inner> chooseBucket() {
        Optional<RequiresLock<SweepableBucket>> firstObject = Optional.ofNullable(currentBuckets.pollFirst());
        if (firstObject.isEmpty()) {
            candidateSweepableBucketRetriever.requestUpdate();
            return Optional.empty();
        }
        firstObject.map(RequiresLock::tryLock).orElseGet(() -> {

        })
    }

    private Optional<RequiresLock<SweepableBucket>.Inner> randomUnlockedBucket() {
        List<RequiresLock<SweepableBucket>> shuffledList = new ArrayList<>(currentBuckets);
        Collections.shuffle(shuffledList);
        // We must show that this doesn't try locking everything!!!!
        return shuffledList.stream()
                .map(RequiresLock::tryLock)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();

    }
}
