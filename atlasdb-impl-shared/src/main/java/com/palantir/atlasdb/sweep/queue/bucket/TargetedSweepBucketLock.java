/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue.bucket;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;

// Shamelessly copied from TargetedSweeperLock, but with required changes.
public final class TargetedSweepBucketLock {
    private final TimelockService timeLock;
    private final LockToken lockToken;

    private TargetedSweepBucketLock(TimelockService timeLock, LockToken lockToken) {
        this.timeLock = timeLock;
        this.lockToken = lockToken;
    }

    public static Optional<TargetedSweepBucketLock> tryAcquire(SweepQueueBucket bucket, TimelockService timeLock) {
        // We do not want the timeout to be too low to avoid a race condition where we give up too soon
        LockRequest request = LockRequest.of(ImmutableSet.of(bucket.toLockDescriptor()), 100L);
        return timeLock.lock(request)
                .getTokenOrEmpty()
                .map(lockToken -> new TargetedSweepBucketLock(timeLock, lockToken));
    }

    public void unlock() {
        timeLock.unlock(ImmutableSet.of(lockToken));
    }
}
