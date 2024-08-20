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

import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class DefaultBucketAssigner {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketAssigner.class);
    private static final long TEN_MINUTES_IN_MILLIS = Duration.ofMinutes(10).toMillis();
    private final ShardCountStore shardCountStore;
    private final SweepBucketsTable sweepBucketsTable;
    private final SweepBucketWritePointerTable sweepBucketWritePointerTable;
    private final PuncherStore puncherStore;
    private final SweeperStrategy sweeperStrategy;

    private DefaultBucketAssigner(
            ShardCountStore shardCountStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketWritePointerTable sweepBucketWritePointerTable,
            PuncherStore puncherStore,
            SweeperStrategy sweeperStrategy) {
        this.shardCountStore = shardCountStore;
        this.sweepBucketsTable = sweepBucketsTable;
        this.sweepBucketWritePointerTable = sweepBucketWritePointerTable;
        this.puncherStore = puncherStore;
        this.sweeperStrategy = sweeperStrategy;
    }

    public static DefaultBucketAssigner create(
            ShardCountStore shardCountStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketWritePointerTable sweepBucketWritePointerTable,
            PuncherStore puncherStore,
            SweeperStrategy sweeperStrategy) {
        return new DefaultBucketAssigner(
                shardCountStore, sweepBucketsTable, sweepBucketWritePointerTable, puncherStore, sweeperStrategy);
    }

    private void runOneIteration() {
        BucketState bucketState = createNewBucket();
        while (bucketState == BucketState.CLOSED) {
            bucketState = createNewBucket();
        }
    }
    // We actually can use this to assign any bucket, not just one that's partially open.
    // We just need to have sweepBucketWritePointerTable track the highest _fully closed_ bucket.
    private BucketState createNewBucket() {
        // We need to prove what happens when this is run in parallel, or arbitrarily interrupted at any point
        long previousBucketIdentifier = sweepBucketWritePointerTable.getHighestBucketNumber();
        long latestBucketIdentifier =
                previousBucketIdentifier + 1; // We need to prove that this will always be contiguous.
        int numberOfShards = shardCountStore.getShardCount(sweeperStrategy);

        // TODO: In theory, we only need to read _one_ bucket.
        // TODO: WAIT This won't work. What happens if all the previous buckets are deleted? There's no previous 10
        //  minutes :( We have to store the wallclock time of the last complete bucket somewhere.
        // Nothing can rely on reading an existing bucket, because it might have been deleted before the next iteration
        // This model entirely assumes all the shards have the same timestamp range.
        // It was done this way for simplicity and to avoid unnecessary reads whilst we make that assumption, but
        // moving from this to different timestamp ranges per shard is feasible - will just involve a bunch more
        // writes and reads as you track the lastWallClockTimeForBucket per shardAndStrategy.
        // TODO: consider whether we do that anyway, because it means we can make changes without requiring a migration.
        long lastWallClockTimeForBucket =
                sweepBucketWritePointerTable
                        .getLastWallClockTime(); // TODO: Don't do this, just store the last logical timestamp.
        long lastLogicalTimestamp =
                puncherStore.get(lastWallClockTimeForBucket); // tODO: Store the last timestamp instead
        // going from time -> timestamp is dangerous for historic queries, but if we skip that, then it's fine.

        // We also need to reason about what happens if, after 10 minutes we close the bucket at X, but then there's
        // another entry
        // that moves the same query to Y > X. If we store the wallclock time, we would miss out on [X, Y), but if we
        // store X, then at most we have a bucket slightly bigger than 10 minutes.
        long newLogicalTimestamp = puncherStore.get(lastWallClockTimeForBucket + TEN_MINUTES_IN_MILLIS);
        // TODO: Explicitly document the start and end and how that corresponds with the punch table entries.

        Map<ShardAndStrategy, TimestampRange> newBucketRange = IntStream.range(0, numberOfShards)
                .mapToObj(i -> ShardAndStrategy.of(i, sweeperStrategy))
                .collect(Collectors.toMap(
                        shardAndStrategy -> shardAndStrategy, shardAndStrategy -> ImmutableTimestampRange.builder()
                                .startInclusive(lastLogicalTimestamp + 1)
                                .endExclusive(newLogicalTimestamp)
                                .build()));

        newBucketRange.forEach(((shardAndStrategy, timestampRange) -> {
            Bucket bucket = Bucket.of(shardAndStrategy, latestBucketIdentifier);
            sweepBucketsTable.putTimestampRangeForBucket(bucket, timestampRange);
        }));

        Long lastTimestamp = associatedMillisWithTimestamp.values().stream()
                .map(v -> v + TEN_MINUTES_IN_MILLIS)
                .max(Long::compareTo)
                .orElse(0L);
        if (lastTimestamp <= Instant.now().toEpochMilli()) {
            sweepBucketWritePointerTable.updateHighestBucketNumber(previousBucketIdentifier, latestBucketIdentifier);
            log.info("Bucket {} is now complete", SafeArg.of("bucketIdentifier", latestBucketIdentifier));
            return BucketState.CLOSED;
        } else {
            log.info(
                    "Bucket {} has progressed, but is not complete",
                    SafeArg.of("bucketIdentifier", latestBucketIdentifier));
            return BucketState.INCOMPLETE;
        }

        // We technically need to get the timestamp range for all buckets

        // So, closing the bucket involves updating sweep_buckets by writing the correct timestamp range for the 10
        // minute period before it. That means we need to:
        // Get the latest bucket we wrote.
        // Get the max timestamp for the bucket we wrote prior to that
        // Get the corresponding punch table entry for the max timestamp
        // Get the corresponding punch table entry for + 10 minutes
        // Write the timestamp range to sweep_buckets

        // We need to be very careful about inclusive and exclusive timestamps, especially given the API in the punch
        // table.
    }

    enum BucketState {
        INCOMPLETE,
        CLOSED
    }
}
