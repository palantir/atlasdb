/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceImpl implements AsyncTimelockService {

    private final AsyncLockService lockService;
    private final ManagedTimestampService timestampService;

    public AsyncTimelockServiceImpl(AsyncLockService lockService, ManagedTimestampService timestampService) {
        this.lockService = lockService;
        this.timestampService = timestampService;
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        long timestamp = timestampService.getFreshTimestamp();

        // this will always return synchronously
        LockTokenV2 token = lockService.lockImmutableTimestamp(request.getRequestId(), timestamp).join();
        long immutableTs = lockService.getImmutableTimestamp().orElse(timestamp);

        return LockImmutableTimestampResponse.of(immutableTs, LockRefreshTokens.fromLockTokenV2(token));
    }

    @Override
    public long getImmutableTimestamp() {
        long timestamp = timestampService.getFreshTimestamp();
        return lockService.getImmutableTimestamp().orElse(timestamp);
    }

    @Override
    public CompletableFuture<LockRefreshToken> lock(LockRequestV2 request) {
        return lockService.lock(request.getRequestId(), request.getLockDescriptors())
                .thenApply(LockRefreshTokens::fromLockTokenV2);
    }

    @Override
    public CompletableFuture<Void> waitForLocks(WaitForLocksRequest request) {
        return lockService.waitForLocks(request.getRequestId(), request.getLockDescriptors());
    }

    @Override
    public Set<LockRefreshToken> refreshLockLeases(Set<LockRefreshToken> tokens) {
        Set<LockTokenV2> transformed = transform(tokens, LockRefreshTokens::toLockTokenV2);
        Set<LockTokenV2> refreshed = lockService.refresh(transformed);
        return transform(refreshed, LockRefreshTokens::fromLockTokenV2);
    }

    @Override
    public Set<LockRefreshToken> unlock(Set<LockRefreshToken> tokens) {
        Set<LockTokenV2> transformed = transform(tokens, LockRefreshTokens::toLockTokenV2);
        Set<LockTokenV2> refreshed = lockService.unlock(transformed);
        return transform(refreshed, LockRefreshTokens::fromLockTokenV2);
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        timestampService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public String ping() {
        return timestampService.ping();
    }

    private static <T, U> Set<U> transform(Set<T> input, Function<T, U> transform) {
        if (input.size() == 1) {
            return ImmutableSet.of(transform.apply(input.iterator().next()));
        }

        Set<U> transformed = Sets.newHashSetWithExpectedSize(input.size());
        for (T original : input) {
            transformed.add(transform.apply(original));
        }
        return transformed;
    }
}
