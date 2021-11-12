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

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;

public final class BucketRangeLockingService {
    private static final long DO_NOT_BLOCK = 0L;

    private final TimelockService timelockService;

    public BucketRangeLockingService(TimelockService timelockService) {
        this.timelockService = timelockService;
    }

    public Optional<LockToken> lockBuckets(BucketRange bucketRange) {
        return timelockService
                .lock(LockRequest.of(ImmutableSet.of(getLockDescriptor(bucketRange)), DO_NOT_BLOCK))
                .getTokenOrEmpty();
    }

    public void unlock(LockToken lockToken) {
        timelockService.unlock(ImmutableSet.of(lockToken));
    }

    private static LockDescriptor getLockDescriptor(BucketRange bucketRange) {
        return StringLockDescriptor.of("__bucketing" + bucketRange.startInclusive());
    }

    public boolean checkTokenStillValid(LockToken activeRangeToken) {
        return !timelockService
                .refreshLockLeases(ImmutableSet.of(activeRangeToken))
                .isEmpty();
    }
}
