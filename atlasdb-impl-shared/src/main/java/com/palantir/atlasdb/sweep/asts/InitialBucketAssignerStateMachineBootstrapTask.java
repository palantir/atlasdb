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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketAssignerStateMachineTable;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class InitialBucketAssignerStateMachineBootstrapTask {
    private final ShardProgress progress;
    private final SweepBucketAssignerStateMachineTable stateMachineTable;
    private final LongSupplier freshTimestampSupplier;

    public InitialBucketAssignerStateMachineBootstrapTask(
            ShardProgress progress,
            SweepBucketAssignerStateMachineTable stateMachineTable,
            LongSupplier freshTimestampSupplier) {
        this.progress = progress;
        this.stateMachineTable = stateMachineTable;
        this.freshTimestampSupplier = freshTimestampSupplier;
    }

    public void run() {
        if (stateMachineTable.doesStateMachineStateExist()) {
            return;
        }

        Set<Long> lastSweptTimestamps = ImmutableSet.copyOf(
                progress.getLastSweptTimestamps(allShardsAndStrategies()).values());

        // If there's an initial timestamp, that means we haven't written / swept a given shard. We can't have a bucket
        // from -1, but equally, we likely don't want our first bucket to be from near -1 either, as that will
        // almost certainly start creating lots and lots and lots of buckets from when the service started using AtlasDB
        // until we catch up to our current wall clock time
        if ((lastSweptTimestamps.size() == 1 && lastSweptTimestamps.contains(SweepQueueUtils.RESET_TIMESTAMP))
                || lastSweptTimestamps.contains(SweepQueueUtils.INITIAL_TIMESTAMP)) {
            bootstrapFreshInstallOrReset();
        } else {
            boostrapExistingInstall(
                    lastSweptTimestamps.stream().min(Long::compareTo).orElse(0L));
        }
    }

    private void bootstrapFreshInstallOrReset() {
        stateMachineTable.setInitialStateForBucketAssigner(
                BucketAssignerState.immediatelyClosing(0, freshTimestampSupplier.getAsLong()));
    }

    private void boostrapExistingInstall(long minLastSweptTimestamp) {
        stateMachineTable.setInitialStateForBucketAssigner(BucketAssignerState.start(minLastSweptTimestamp));
    }

    private Set<ShardAndStrategy> allShardsAndStrategies() {
        int numShards = progress.getNumberOfShards();
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);

        return IntStream.range(0, numShards)
                .boxed()
                .flatMap(shard -> strategies.stream().map(strategy -> ShardAndStrategy.of(shard, strategy)))
                .collect(Collectors.toSet());
    }
}
