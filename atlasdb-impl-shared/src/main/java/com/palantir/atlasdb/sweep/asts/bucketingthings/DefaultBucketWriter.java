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

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Optional;

final class DefaultBucketWriter implements BucketWriter {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketWriter.class);
    private final SweepBucketsTable sweepBucketsTable;
    private final List<SweeperStrategy> sweeperStrategies;
    private final int numberOfShards;

    // The list ordering needs to be consistent, even when adding new sweeper strategies. New strategies must be added
    // to the end.
    private DefaultBucketWriter(
            SweepBucketsTable sweepBucketsTable, List<SweeperStrategy> sweeperStrategies, int numberOfShards) {
        this.sweepBucketsTable = sweepBucketsTable;
        this.sweeperStrategies = sweeperStrategies;
        this.numberOfShards = numberOfShards;
    }

    // A static number of shards, since it will not be configurable anymore.
    static BucketWriter create(
            SweepBucketsTable sweepBucketsTable, List<SweeperStrategy> sweeperStrategies, int numberOfShards) {
        return new DefaultBucketWriter(sweepBucketsTable, sweeperStrategies, numberOfShards);
    }

    @Override
    public WriteState writeToAllBuckets(
            long bucketIdentifier, Optional<TimestampRange> oldTimestampRange, TimestampRange newTimestampRange) {

        log.info(
                "Assigning bucket ID {} timestamp range {} from old range {}",
                SafeArg.of("bucketIdentifier", bucketIdentifier),
                SafeArg.of("newTimestampRange", newTimestampRange),
                SafeArg.of("oldTimestampRange", oldTimestampRange));
        boolean failedForContiguousPrefixOfBuckets = false;
        // A consistent ordering (by shard number and strategy), so that retries and concurrent threads are CASing
        // in the same order, making it easier to determine if this is potentially a retry (there was a failure at the
        // start, implying there was a previous write), or contention (we were successful at the beginning so no
        // previous successful attempts at writing, but now failing implying someone else is updating).
        for (SweeperStrategy sweeperStrategy : sweeperStrategies) {
            for (int shard = 0; shard < numberOfShards; shard++) {
                Bucket bucket = Bucket.of(ShardAndStrategy.of(shard, sweeperStrategy), bucketIdentifier);
                try {
                    sweepBucketsTable.putTimestampRangeForBucket(bucket, oldTimestampRange, newTimestampRange);
                    // reset the flag after a successful attempt, so we can differentiate between a potential abort
                    // and contention.
                    failedForContiguousPrefixOfBuckets = false;
                } catch (CheckAndSetException e) {
                    // We know there's an element in the list, otherwise we wouldn't even be here.
                    if (shard == 0 && sweeperStrategies.get(0) == sweeperStrategy) {
                        failedForContiguousPrefixOfBuckets = true;
                        // This doesn't indicate anything problematic, other than someone has at least tried and
                        // potentially failed at some point.
                        // They may have got arbitrarily far, so we need to ignore failures from here on until the next
                        // success
                        log.info(
                                "We tried to CAS the first bucket on {} with timestamp range {} from old range"
                                        + " {}, but it failed due to a CheckAndSetException. We will continue"
                                        + " attempting  the CAS on the rest of the buckets, as it's possible that this"
                                        + " is the remnant of a previous attempt",
                                SafeArg.of("shardAndStrategy", bucket.shardAndStrategy()),
                                SafeArg.of("newTimestampRange", newTimestampRange),
                                SafeArg.of("oldTimestampRange", oldTimestampRange),
                                e);
                    } else if (!failedForContiguousPrefixOfBuckets) {
                        // This implies we're contending with someone else, and they're ahead of us. We should
                        // stop.
                        log.warn(
                                "Failed to assign bucket {} to shard {} with timestamp range {} from old range {}."
                                        + " We failed part way through, so we're likely contending with another writer."
                                        + " This isn't an issue for correctness, but if this happens repeatedly,"
                                        + " it's possibly worth investigating why we're losing locks so frequently.",
                                SafeArg.of("bucket", bucket.bucketIdentifier()),
                                SafeArg.of("shardAndStrategy", bucket.shardAndStrategy()),
                                SafeArg.of("newTimestampRange", newTimestampRange),
                                SafeArg.of("oldTimestampRange", oldTimestampRange),
                                e);
                        return WriteState.FAILED_CAS;
                    }
                }
            }
        }
        return WriteState.SUCCESS;
    }
}
