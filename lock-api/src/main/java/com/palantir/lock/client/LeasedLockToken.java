/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;

import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

final class LeasedLockToken implements LockToken {
    private final LockToken serverToken;
    private final UUID requestId;

    @GuardedBy("this")
    private Lease lease;

    @GuardedBy("this")
    private boolean invalidated = false;

    @Override
    public String toString() {
        return "LeasedLockToken@" + System.identityHashCode(this)
                + "{" +
                "serverToken=" + serverToken +
                ", requestId=" + requestId +
                ", lease=" + lease +
                ", invalidated=" + invalidated +
                '}';
    }

    static LeasedLockToken of(LockToken serverToken, Lease lease) {
        return new LeasedLockToken(serverToken, UUID.randomUUID(), lease);
    }

    private LeasedLockToken(LockToken serverToken, UUID requestId, Lease lease) {
        this.serverToken = serverToken;
        this.requestId = requestId;
        this.lease = lease;
    }

    LockToken serverToken() {
        return serverToken;
    }

    synchronized Lease getLease() {
        return lease;
    }

    synchronized void updateLease(Lease newLease) {
        Preconditions.checkArgument(lease.leaderTime().isComparableWith(newLease.leaderTime()),
                "Lock leases can only be refreshed by lease owners. Current leader id: {}, new leader id: {}",
                SafeArg.of("currentLeaderId", lease.leaderTime().id()),
                SafeArg.of("newLeaderId", newLease.leaderTime().id()));

        if (this.lease.expiry().isBefore(newLease.expiry())) {
            this.lease = newLease;
        }
    }

    synchronized boolean isValid(LeaderTime currentLeaderTime) {
        return !invalidated && lease.isValid(currentLeaderTime);
    }

    synchronized void invalidate() {
        invalidated = true;
    }

    @Override
    public UUID getRequestId() {
        return requestId;
    }
}
