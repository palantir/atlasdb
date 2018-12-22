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

package com.palantir.leader.lease;

import java.time.Duration;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

final class LeadershipLeaseLeadershipToken implements LeadershipToken {
    // there could be a delay of refreshDuration between finding 'now' and the end of isStillLeading.get().
    private static final int MAX_UNLIKELY_SPIN_ATTEMPTS = 3;

    private final Supplier<NanoTime> clock;
    private final Duration leaseRefreshDuration;
    private final LeadershipToken wrapped;
    private final Supplier<StillLeadingStatus> isStillLeading;

    private volatile LeadershipState leadershipState = null;

    @VisibleForTesting
    LeadershipLeaseLeadershipToken(
            Supplier<NanoTime> clock,
            LeadershipToken wrapped,
            Duration leaseRefreshDuration,
            Supplier<StillLeadingStatus> isStillLeading) {
        this.clock = clock;
        this.leaseRefreshDuration = leaseRefreshDuration;
        this.wrapped = wrapped;
        this.isStillLeading = isStillLeading;
    }

    LeadershipLeaseLeadershipToken(
            LeadershipToken wrapped,
            Duration leaseRefreshDuration,
            Supplier<StillLeadingStatus> isStillLeading) {
        this(NanoTime::now, wrapped, leaseRefreshDuration, isStillLeading);
    }

    StillLeadingStatus getLeadershipStatus() {
        if (!ClockReversalDetector.canUseLeaseBasedOptimizations()) {
            return isStillLeading.get();
        }
        for (int i = 0; i < MAX_UNLIKELY_SPIN_ATTEMPTS &&
                (leadershipState == null || leadershipState.validUntil.isBefore(clock.get()));
             i++) {
            fetchNewState();
        }
        return leadershipState.status;
    }

    private synchronized void fetchNewState() {
        // take the time before fetching; this is critical and guarantees that it's conservative
        NanoTime now = clock.get();
        StillLeadingStatus status = isStillLeading.get();
        leadershipState = new LeadershipState(now.plus(leaseRefreshDuration), status);
    }

    @Override
    public boolean sameAs(LeadershipToken obj) {
        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }
        LeadershipLeaseLeadershipToken other = (LeadershipLeaseLeadershipToken) obj;
        return wrapped.sameAs(other.wrapped);
    }

    private static final class LeadershipState {
        private final NanoTime validUntil;
        private final StillLeadingStatus status;

        private LeadershipState(NanoTime validUntil, StillLeadingStatus status) {
            this.validUntil = validUntil;
            this.status = status;
        }
    }

}
