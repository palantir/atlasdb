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
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketWritePointerTable.TimestampForBucket;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class DefaultBucketAssigner implements Runnable {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketAssigner.class);
    private static final long TEN_MINUTES_IN_MILLIS = Duration.ofMinutes(10).toMillis();
    private static final int MAX_BUCKETS_PER_ITERATION = 5;
    private static final Duration CLOSE_BUCKET_LEEWAY = Duration.ofMinutes(3);
    private final ShardProgress shardProgressStore;
    private final SweepBucketsTable sweepBucketsTable;
    private final SweepBucketWritePointerTable sweepBucketWritePointerTable;
    private final PuncherStore puncherStore;
    private final SweeperStrategy sweeperStrategy;

    // TODO(mdaudali): The intent for this is metrics, but it can also be used to e.g trigger an eager refresh
    //  or, by looking at the rate that we're creating buckets, auto scale on the given node.
    private final Consumer<Long> newBucketIdentifierReporter;

    private DefaultBucketAssigner(
            ShardProgress shardProgressStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketWritePointerTable sweepBucketWritePointerTable,
            PuncherStore puncherStore,
            SweeperStrategy sweeperStrategy,
            Consumer<Long> newBucketIdentifierReporter) {
        this.shardProgressStore = shardProgressStore;
        this.sweepBucketsTable = sweepBucketsTable;
        this.sweepBucketWritePointerTable = sweepBucketWritePointerTable;
        this.puncherStore = puncherStore;
        this.sweeperStrategy = sweeperStrategy;
        this.newBucketIdentifierReporter = newBucketIdentifierReporter;
    }

    public static Runnable create(
            ShardProgress shardProgressStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketWritePointerTable sweepBucketWritePointerTable,
            PuncherStore puncherStore,
            SweeperStrategy sweeperStrategy,
            Consumer<Long> newBucketIdentifierReporter) {
        return new DefaultBucketAssigner(
                shardProgressStore,
                sweepBucketsTable,
                sweepBucketWritePointerTable,
                puncherStore,
                sweeperStrategy,
                newBucketIdentifierReporter);
    }

    @Override
    public void run() {
        // TODO: Take a lock, or use Lockable<ExclusiveTask> to externalise the locking.
        BucketState bucketState = createNewBucket();

        // We cap the number of buckets we create per iteration to avoid a single thread getting stuck creating
        // buckets. This is a safety measure in case there's a bug in the bucket creation logic that causes it to
        // infinitely
        // create the same bucket or return CLOSED over and over again.
        for (int i = 0; i < MAX_BUCKETS_PER_ITERATION && bucketState == BucketState.CLOSED; i++) {
            bucketState = createNewBucket();
            if (i == MAX_BUCKETS_PER_ITERATION - 1 && bucketState == BucketState.CLOSED) {
                // TODO(mdaudali): Add a metric for this. I want to monitor this for rollout.s
                log.warn(
                        "Reached the maximum number of buckets created per iteration. This is most likely because we're very far behind, or because"
                                + " there's a bug in the bucket creation logic that causes it to infinitely create the same bucket or return CLOSED over and over again.");
            }
        }
    }

    // Okay. So we need to indicate a -1 to represent an open bucket. THIS CANNOT BE DELETED.
    // You read (bucket identifier, A, B) from the first timestamp.
    // If bucket identifier = current, then this is what you'll CAS over everyone with
    // If bucket identifier = next, you have already CASd. You skip this, and just update the bucket number.
    // This is actually impossible. Consider cassing at the end to bucket identifier + 1, B, -1? But then you have
    // to do
    // two cass when catching up, but this might be better than 256 wasted CAS (one for each bucket)? If you do
    // this, then you can never have previous.
    // If bucket identifier = previous, then:
    // If we cannot close now, then you CAS the buckets with [B, -1) and hold.
    // If you can close now, you CAS to (bucket identifier, B, C).
    // You then CAS all buckets.
    // You then update the highest bucket number.

    private BucketState newCreateNewBucket() {
        long previousBucketIdentifier = sweepBucketWritePointerTable.getHighestBucketNumber();
        long latestBucketIdentifier = previousBucketIdentifier + 1;
        int numberOfShards = shardProgressStore.getNumberOfShards();

        TimestampForBucket lastTimestampForBucket = sweepBucketWritePointerTable.getLastTimestampForBucket();
        if (lastTimestampForBucket.bucketIdentifier() == latestBucketIdentifier) {
            TimestampRange timestampRangeToApply;
            if (lastTimestampForBucket.timestampRange().endExclusive() == -1) {
                // This is potentially an open bucket.
                // Get the new end timestamp
                long endWallClockTime = puncherStore.getMillisForTimestamp(
                                lastTimestampForBucket.timestampRange().startInclusive())
                        + TEN_MINUTES_IN_MILLIS;
                boolean isClosed = endWallClockTime <= Instant.now().toEpochMilli();
                if (isClosed) {
                    timestampRangeToApply = ImmutableTimestampRange.builder()
                            .startInclusive(
                                    lastTimestampForBucket.timestampRange().startInclusive())
                            .endExclusive(puncherStore.get(endWallClockTime))
                            .build();
                    // HOW DO I KNOW WHAT TO CAS WITH?
                    sweepBucketWritePointerTable.updateLastTimestampForBucket(
                            lastTimestampForBucket,
                            TimestampForBucket.of(lastTimestampForBucket.bucketIdentifier(), timestampRangeToApply));
                } else {
                    // This is an open bucket, so we don't need to update the last timestamp.
                    timestampRangeToApply = lastTimestampForBucket.timestampRange();
                }
            } else {
                // We've already started on this bucket
                timestampRangeToApply = lastTimestampForBucket.timestampRange();
            }
            // This is the current bucket. This is what we'll CAS with
        } // Otherwise, we've actually already done the CAS, and so we can just update the bucket number.
        sweepBucketWritePointerTable.updateHighestBucketNumber(previousBucketIdentifier, latestBucketIdentifier);
    }

    // We actually can use this to assign any bucket, not just one that's partially open.
    // We just need to have sweepBucketWritePointerTable track the highest _fully closed_ bucket.
    private BucketState createNewBucket() {
        // TODO: We need to prove what happens when this is run in parallel, or arbitrarily interrupted at any point
        long previousBucketIdentifier = sweepBucketWritePointerTable.getHighestBucketNumber();
        long latestBucketIdentifier = previousBucketIdentifier + 1;
        int numberOfShards =
                shardProgressStore.getNumberOfShards(); // Could we just use ShardProgress#getNumberOfShards???

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
        TimestampRange range = ImmutableTimestampRange.builder()
                .startInclusive(lastLogicalTimestamp + 1)
                .endExclusive(newLogicalTimestamp)
                .build();
        List<SweepableBucket> sweepableBuckets = IntStream.range(0, numberOfShards)
                .mapToObj(i -> ShardAndStrategy.of(i, sweeperStrategy))
                .map(shardAndStrategy -> Bucket.of(shardAndStrategy, latestBucketIdentifier))
                .map(bucket -> SweepableBucket.of(bucket, range))
                .collect(Collectors.toList());

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

        // Suppose we fail to update the highestBucketNumber, and then we stamp the punch store such that we get a new
        // timestamp that's _greater_ than the existing buckets. Then, we retry, trying to overwrite the existing bucket
        // that has [startTs, originalEndTs) with [startTs, newEndTs). These writes _fail_ because we're contending on
        // the CAS, but because they're all present, we succeed the above check. We then write newEndTs as the last
        // timestamp,
        // but that's a BUG because we've lost all the writes between [originalEndTs, newEndTs).
        // _EXCEPT_ we decided that you cannot delete buckets that are open, and so we COULD notice this case and handle
        // it separately
        // IN FACT, we have to, because we need to deal with closing open buckets!!!! This is exactly equivalent to
        // the whole CLOSE A BUCKET problem. Simply not writing until a bucket is closeable isn't sufficient, because
        // it's possible for things to fail.

        // I think if we read the first bucket. If it exists, it implies one of two cases:
        // 1. This is an open bucket. Read the last timestamp. If it does not match what you would put, it's likely open
        // and CAS using that.
        // It's possible it was closed but the punch table corresponding entry is _greater_ and you're overwriting it
        // with a larger range and you have a stale read. That's fine, because you'll end up with bucket X [startTs,
        // newEndTs) and bucket X+1 [oldEndTs, newEndTs')
        // where importantly oldEndTs < newEndTs, so just have overlapping ranges. You'll then fail to update the
        // highestBucketNumber and abort.
        // You can never read a punch table timestamp entry _smaller_ than the last read, unless the punch table has
        // entries being deleted from it.
        //  We can even pass a flag for the first call that says allowClosingOpenBucket.
        // 2. This is a closed bucket. Read the last timestamp. If it _matches_, then CAS it against null and complete
        // the row.
        //   Note - it is then possible that we re-add entries that were swept away. In an absolutely terrible
        // situation, it's possible to
        // READ the first bucket, make a decision on CASing. Between reading and writing again, the bucket gets swept
        // away.
        // The background task then pushes us forward, and then we write _behind_ the first bucket, which will never get
        // cleaned away and entirely orphaned.
        // This isn't solved at all with a flag - it's caused by the discrepency between reading the current bucket
        // number and writing the bucket.

        // Equally, if the bucket was written but incomplete (i.e., it's not been 10 minutes yet), _and_ somethings
        // have swept at the beginning of the list, then we cannot differentiate between that and CAS contention.
        // so have to give up, meaning we can't make progress on writing new buckets until the very last bucket is
        // complete across all shards.
        // EXCEPT WE decided that you cannot delete buckets that are open - YAY.

        // What happens in the face of clock drift? In the scope of one node, this is fine, since the bucket might be
        // slightly unevenly sized, but that's fine
        // But what happens if two nodes are writing? I think it's still fine - one node will close the bucket, the
        // other will just stop. Closing the bucket is fine.

        // THIS IS BS
        // Okay. So we need to indicate a -1 to represent an open bucket. THIS CANNOT BE DELETED.
        // You read (bucket identifier, A, B) from the first timestamp.
        // If bucket identifier = current, then this is what you'll CAS over everyone with
        // If bucket identifier = next, you have already CASd. You skip this, and just update the bucket number.
        // This is actually impossible. Consider cassing at the end to bucket identifier + 1, B, -1? But then you have
        // to do
        // two cass when catching up, but this might be better than 256 wasted CAS (one for each bucket)? If you do
        // this, then you can never have previous.
        // If bucket identifier = previous, then:
        // If we cannot close now, then you CAS the buckets with [B, -1) and hold.
        // If you can close now, you CAS to (bucket identifier, B, C).
        // You then CAS all buckets.
        // You then update the highest bucket number.

        // You then store a tuple (X, Y), representing what the buckets should be. This should go (X, Y) -> (Y, Z)
        // THEN WE read the last timestmap first. WE ALSO must read the FIRST BUCKET to get the correct first timestamp,
        // and check that it's open at this time.
        // If the last timestamp is Y and the first bucket end timestamp is Y, then we use Y everywhere.
        // If the last timestamp is X (which would be the last bucket end timestamp) and the first bucket does not
        // exist, then we must
        // Load Y, then store the first bucket with Y, then update the last timestamp to Y.
        // If the last timestamp is Y and the first bucket is OPEN, then we use Y everywhere.
        // TODO: Add tests about what happens at each failure point.

        if (lastWallClockTime <= Instant.now().plus(CLOSE_BUCKET_LEEWAY).toEpochMilli()) {
            sweepBucketWritePointerTable.updateHighestBucketNumber(previousBucketIdentifier, latestBucketIdentifier);
            // TODO: What happens if these start failing? I think the bucket number has to come first - if that succeeds
            // but updateLastTimestampForBucket fails,
            // we'll have two buckets that overlap, but that itself isn't a problem outside of contention.
            // If we update the timestamp, it's possible we then overwrite the _existing bucket_ from
            // [lastLogicalTimestamp, newLogicalTimestamp) -> [newLogicalTimestamp, evenNewerLogicalTimestamp) which
            // would
            // lose all writes between [lastLogicalTimestamp, newLogicalTimestamp)
            sweepBucketWritePointerTable.updateLastTimestampForBucket(lastLogicalTimestamp, newLogicalTimestamp);
            newBucketIdentifierReporter.accept(latestBucketIdentifier);
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
        INCOMPLETE, // TODO: should log the state / metric it
        CLOSED,
        CONTENTION_ON_WRITES
    }
}
