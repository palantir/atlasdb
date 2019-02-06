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
import com.palantir.common.time.NanoTime;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

final class LeasingLeadershipToken implements LeadershipToken {
    private final LeasingRequirements leasingRequirements;
    private final Supplier<NanoTime> clock;
    private final Duration leaseRefreshDuration;
    private final LeadershipToken wrapped;
    private final Supplier<StillLeadingStatus> isStillLeading;

    private volatile LeadershipState leadershipState = null;

    @VisibleForTesting
    LeasingLeadershipToken(
            LeasingRequirements leasingRequirements,
            Supplier<NanoTime> clock,
            LeadershipToken wrapped,
            Duration leaseRefreshDuration,
            Supplier<StillLeadingStatus> isStillLeading) {
        this.leasingRequirements = leasingRequirements;
        this.clock = clock;
        this.leaseRefreshDuration = leaseRefreshDuration;
        this.wrapped = wrapped;
        this.isStillLeading = isStillLeading;
    }

    LeasingLeadershipToken(
            LeadershipToken wrapped,
            Duration leaseRefreshDuration,
            Supplier<StillLeadingStatus> isStillLeading,
            LeasingRequirements leasingRequirements) {
        this(leasingRequirements, NanoTime::now, wrapped, leaseRefreshDuration, isStillLeading);
    }

    StillLeadingStatus getLeadershipStatus() {
        if (!leasingRequirements.canUseLeadershipLeases()) {
            return isStillLeading.get();
        }
        if (isStateInvalid()) {
            fetchNewState();
        }
        return leadershipState.status;
    }

    private boolean isStateInvalid() {
        return leadershipState == null || leadershipState.validUntil.isBefore(clock.get());
    }

    private synchronized void fetchNewState() {
        if (isStateInvalid()) {
            // take the time before fetching; this is critical and guarantees that it's a safe optimization
            NanoTime now = clock.get();
            StillLeadingStatus status = isStillLeading.get();
            leadershipState = new LeadershipState(now.plus(leaseRefreshDuration), status);
        }
    }

    @Override
    public boolean sameAs(LeadershipToken obj) {
        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }
        LeasingLeadershipToken other = (LeasingLeadershipToken) obj;
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
