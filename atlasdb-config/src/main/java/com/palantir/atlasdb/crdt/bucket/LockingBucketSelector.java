/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.crdt.bucket;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.crdt.Series;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

public final class LockingBucketSelector implements SeriesBucketSelector {
    private final BucketRangeLockingService lockingService;

    // TODO (jkong): Better handle cases where concurrency on a local node exceeds the bucketing quantum
    private final AtomicReference<BucketRangeAndToken> activeBucketRangeAndToken = new AtomicReference<>();
    private final Set<Long> usedOffsets = Sets.newConcurrentHashSet();

    public LockingBucketSelector(BucketRangeLockingService lockingService) {
        this.lockingService = lockingService;
    }

    @Override
    public long getBucket(Series series) {
        BucketRange range = getActiveBucketRange();
        return range.startInclusive() + acquireOffset();
    }

    @Override
    public void releaseBucket(Series series, long bucket) {
        BucketRange range = getActiveBucketRange();
        if (range.startInclusive() <= bucket && bucket < range.endExclusive()) {
            usedOffsets.remove(bucket % BucketRange.BUCKET_BUCKETING);
            if (usedOffsets.isEmpty()) {
                // TODO (jkong): Add some time delay here?
                releaseBucketRange();
            }
        }
    }

    private void releaseBucketRange() {
        BucketRangeAndToken activeRange = activeBucketRangeAndToken.get();
        synchronized (this) {
            lockingService.unlock(activeRange.lockToken());
            activeBucketRangeAndToken.set(null);
        }
    }

    private long acquireOffset() {
        // This really can be done in O(log N), where N is the set of used offsets.
        for (long i = 0; i < BucketRange.BUCKET_BUCKETING; i++) {
            if (!usedOffsets.contains(i)) {
                if (usedOffsets.add(i)) {
                    return i;
                }
            }
        }
        // womp
        return ThreadLocalRandom.current().nextLong(BucketRange.BUCKET_BUCKETING);
    }

    public BucketRange getActiveBucketRange() {
        BucketRangeAndToken currentRange = activeBucketRangeAndToken.get();
        if (currentRange != null) {
            if (lockingService.checkTokenStillValid(currentRange.lockToken())) {
                return currentRange.bucketRange();
            }
        }
        // Maybe need to acquire a new bucket. Watch out for concurrency!
        synchronized (this) {
            currentRange = activeBucketRangeAndToken.get();
            if (currentRange != null) {
                if (lockingService.checkTokenStillValid(currentRange.lockToken())) {
                    return currentRange.bucketRange();
                }
            }

            // In this case, we need to acquire a new bucket
            BucketRangeAndToken newRange = attemptToAcquireNewBucket();
            usedOffsets.clear();
            activeBucketRangeAndToken.set(newRange);
            return newRange.bucketRange();
        }
    }

    private BucketRangeAndToken attemptToAcquireNewBucket() {
        for (long bucketGroup = 0; bucketGroup < 100; bucketGroup++) {
            try {
                BucketRange candidateBucketRange = ImmutableBucketRange.of(bucketGroup * BucketRange.BUCKET_BUCKETING);
                Optional<LockToken> lockToken = lockingService.lockBuckets(candidateBucketRange);
                if (lockToken.isPresent()) {
                    return ImmutableBucketRangeAndToken.builder()
                            .bucketRange(candidateBucketRange)
                            .lockToken(lockToken.get())
                            .build();
                }
            } catch (Exception e) {
                // Failed to get something from the lock service. But that could be an rpc failure etc.
                // Try the next bucket in line.
            }
        }
        throw new SafeRuntimeException("Tried too many bucket groups and wasn't able to land on any");
    }

    @Value.Immutable
    interface BucketRangeAndToken {
        BucketRange bucketRange();

        LockToken lockToken();
    }
}
