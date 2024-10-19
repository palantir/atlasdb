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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketAssignerStateMachineTable;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class InitialBucketAssignerStateMachineBootstrapTask {
    private static final SafeLogger log = SafeLoggerFactory.get(InitialBucketAssignerStateMachineBootstrapTask.class);
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
            log.info("Skipping bootstrap of bucket assigner state machine as it already exists.");
            return;
        }

        int numShards = progress.getNumberOfShards();

        Map<ShardAndStrategy, Long> lastSweptTimestampsByShardAndStrategy =
                progress.getLastSweptTimestamps(allShardsAndStrategies(numShards));
        log.info(
                "Last swept timestamps at time of bucket assigner bootstrap: {}",
                SafeArg.of("lastSweptTimestamps", lastSweptTimestampsByShardAndStrategy));
        Set<Long> lastSweptTimestamps = ImmutableSet.copyOf(lastSweptTimestampsByShardAndStrategy.values());

        // If there's an initial timestamp, that means we haven't written / swept a given shard. We can't have a bucket
        // from -1, but equally, we likely don't want our first bucket to be from near -1 either, as that will
        // almost certainly start creating lots and lots and lots of buckets from when the service started using AtlasDB
        // until we catch up to our current wall clock time
        if ((lastSweptTimestamps.size() == 1 && lastSweptTimestamps.contains(SweepQueueUtils.RESET_TIMESTAMP))
                || lastSweptTimestamps.contains(SweepQueueUtils.INITIAL_TIMESTAMP)) {
            bootstrapFreshInstallOrResetOrAmbiguousStartTime();
        } else {
            bootstrapExistingInstall(
                    lastSweptTimestamps.stream().min(Long::compareTo).orElse(0L));
        }

        // This must be done after calculating the min last swept timestamp _and_ bootstrapping to avoid the case
        // where you bump the shards, those shards now have INITIAL_TIMESTAMP, and then you bootstrap as a fresh install
        // or reset
        if (numShards < AtlasDbConstants.DEFAULT_BUCKET_BASED_SWEEP_SHARDS) {
            // The below will only increase it to 128 if the number of shards is below 128 via CAS, which avoids
            // the TOCTOU issue that would be caused by reading and then writing.
            progress.updateNumberOfShards(AtlasDbConstants.DEFAULT_BUCKET_BASED_SWEEP_SHARDS);
        }
    }

    private void bootstrapFreshInstallOrResetOrAmbiguousStartTime() {
        long freshTimestamp = freshTimestampSupplier.getAsLong();
        long clampedToCoarsePartition = clampToCoarsePartition(freshTimestamp);

        if (clampedToCoarsePartition == 0) {
            log.info("Bootstrapping bucket assigner with initial bucket starting from 0");
            stateMachineTable.setInitialStateForBucketAssigner(BucketAssignerState.start(0));
        } else {
            log.info(
                    "Bootstrapping bucket assigner with initial bucket closing from 0 to {}",
                    SafeArg.of("clampedTimestamp", clampedToCoarsePartition));
            stateMachineTable.setInitialStateForBucketAssigner(
                    BucketAssignerState.immediatelyClosing(0, clampedToCoarsePartition));
        }
    }

    private void bootstrapExistingInstall(long minLastSweptTimestamp) {
        long clampedToCoarsePartition = clampToCoarsePartition(minLastSweptTimestamp);
        log.info(
                "Bootstrapping bucket assigner with initial bucket starting from {}",
                SafeArg.of("clampedToCoarsePartition", clampedToCoarsePartition));
        stateMachineTable.setInitialStateForBucketAssigner(BucketAssignerState.start(clampedToCoarsePartition));
    }

    private long clampToCoarsePartition(long timestamp) {
        return SweepQueueUtils.minTsForCoarsePartition(SweepQueueUtils.tsPartitionCoarse(timestamp));
    }

    private Set<ShardAndStrategy> allShardsAndStrategies(int numShards) {
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);

        return IntStream.range(0, numShards)
                .boxed()
                .flatMap(shard -> strategies.stream().map(strategy -> ShardAndStrategy.of(shard, strategy)))
                .collect(Collectors.toSet());
    }
}
