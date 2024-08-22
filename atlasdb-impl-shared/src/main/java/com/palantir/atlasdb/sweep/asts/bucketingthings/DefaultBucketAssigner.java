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
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
    // TODO: Add telemetry.
    public void runOneIteration() {
        // TODO: Take a lock.
        BucketState bucketState = createNewBucket();
        while (bucketState == BucketState.CLOSED) {
            bucketState = createNewBucket();
        }
    }
    // We actually can use this to assign any bucket, not just one that's partially open.
    // We just need to have sweepBucketWritePointerTable track the highest _fully closed_ bucket.
    private BucketState createNewBucket() {
        // TODO: We need to prove what happens when this is run in parallel, or arbitrarily interrupted at any point
        long previousBucketIdentifier = sweepBucketWritePointerTable.getHighestBucketNumber();
        long latestBucketIdentifier = previousBucketIdentifier + 1;
        int numberOfShards =
                shardCountStore.getShardCount(sweeperStrategy); // Could we just use ShardProgress#getNumberOfShards???

        // Nothing can rely on reading an existing bucket, because it might have been deleted before the next iteration
        // This model entirely assumes all the shards have the same timestamp range.
        // It was done this way for simplicity and to avoid unnecessary reads whilst we make that assumption, but
        // moving from this to different timestamp ranges per shard is feasible - will just involve a bunch more
        // writes and reads as you track the lastWallClockTimeForBucket per shardAndStrategy, but this is at most 512
        // (assuming two strategies)
        // cells - this could be easily a single row.
        // TODO: consider whether we do that anyway, because it means we can make changes without requiring a migration.
        long lastLogicalTimestamp = sweepBucketWritePointerTable.getLastTimestampForBucket();
        long lastWallClockTimeForBucket = puncherStore.getMillisForTimestamp(lastLogicalTimestamp);
        // going from time -> timestamp is dangerous for historic queries, but if we skip that, then it's fine.

        // We also need to reason about what happens if, after 10 minutes we close the bucket at X, but then there's
        // another entry
        // that moves the same query to Y > X. If we store the wallclock time, we would miss out on [X, Y), but if we
        // store X, then at most we have a bucket slightly bigger than 10 minutes.
        long lastWallClockTime = lastWallClockTimeForBucket + TEN_MINUTES_IN_MILLIS;
        long newLogicalTimestamp = puncherStore.get(lastWallClockTime);
        // TODO: Explicitly document the start and end and how that corresponds with the punch table entries.

        // TODO: Test that we CAS in a fixed order at all times. Explain why we do so (so we can differentiate failure
        // cases)
        TimestampRange range = ImmutableTimestampRange
                .builder()
                .startInclusive(lastLogicalTimestamp + 1)
                .endExclusive(newLogicalTimestamp)
                .build();
        List<SweepableBucket> sweepableBuckets = IntStream.range(0, numberOfShards)
                .mapToObj(i -> ShardAndStrategy.of(i, sweeperStrategy))
                .map(shardAndStrategy -> Bucket.of(shardAndStrategy, latestBucketIdentifier))
                .map(bucket -> SweepableBucket.of(bucket, range))
                .toList();

        // TODO: This specifically needs a consistent ordering, since we need to handle failures at the beginning vs
        //  failures midway through.
        boolean failedPreviously = false;
        for (int i = 0; i < sweepableBuckets.size(); i++) {
            SweepableBucket sweepableBucket = sweepableBuckets.get(i);
            log.info(
                    "Assigning bucket {} to shard {} with timestamp range {}",
                    SafeArg.of("bucket", sweepableBucket.bucket().bucketIdentifier()),
                    SafeArg.of(
                            "shard", sweepableBucket.bucket().shardAndStrategy().shard()),
                    SafeArg.of("timestampRange", sweepableBucket.timestampRange()));
            try {
                sweepBucketsTable.putTimestampRangeForBucket(
                        sweepableBucket.bucket(), sweepableBucket.timestampRange());
            } catch (Exception e) { // TODO: Should be a CAS exception specifically
                if (i == 0 || failedPreviously) {
                    failedPreviously = true;
                    // TODO: Log something on 0.
                    // This doesn't indicate anything problematic, other than someone has at least tried and potentially
                    // failed at some point.
                    // They may have got arbitrarily far, so we need to ignore failures from here on until the next
                    // success.
                } else {
                    // This implies we're contending with someone else, and they've gone ahead of us. We should stop.
                    log.warn(
                            "Failed to assign bucket {} to shard {} with timestamp range {}. We failed part way through, so we're likely contending with another writer."
                                    + "This isn't an issue for correctness, but if this happens repeatedly, it's worth investigating why we're losing locks so frequently.",
                            SafeArg.of("bucket", sweepableBucket.bucket().bucketIdentifier()),
                            SafeArg.of(
                                    "shard",
                                    sweepableBucket.bucket().shardAndStrategy().shard()),
                            SafeArg.of("timestampRange", sweepableBucket.timestampRange()),
                            e);
                    return BucketState.CONTENTION_ON_WRITES;
                }
            }
        }

        // What happens in the face of clock drift? In the scope of one node, this is fine, since the bucket might be
        // slightly unevenly sized, but that's fine
        // But what happens if two nodes are writing? I think it's still fine - one node will close the bucket, the
        // other will just stop. Closing the bucket is fine.
        if (lastWallClockTime <= Instant.now().toEpochMilli()) {
            sweepBucketWritePointerTable.updateHighestBucketNumber(previousBucketIdentifier, latestBucketIdentifier);
            // TODO: What happens if these start failing? I think the bucket number has to come first - if that succeeds but updateLastTimestampForBucket fails,
            // we'll have two buckets that overlap, but that itself isn't a problem outside of contention.
            // If we update the timestamp, it's possible we then overwrite the _existing bucket_ from [lastLogicalTimestamp, newLogicalTimestamp) -> [newLogicalTimestamp, evenNewerLogicalTimestamp) which would
            // lose all writes between [lastLogicalTimestamp, newLogicalTimestamp)
            sweepBucketWritePointerTable.updateLastTimestampForBucket(lastLogicalTimestamp, newLogicalTimestamp);
            log.info("Bucket {} is now complete", SafeArg.of("bucketIdentifier", latestBucketIdentifier));
            return BucketState.CLOSED;
        } else {
            log.info(
                    "Bucket {} has progressed, but is not complete",
                    SafeArg.of("bucketIdentifier", latestBucketIdentifier));
            return BucketState.INCOMPLETE;
        }

        // We need to be very careful about inclusive and exclusive timestamps, especially given the API in the punch
        // table.
    }

    enum BucketState {
        INCOMPLETE,
        CLOSED,
        CONTENTION_ON_WRITES
    }
}
