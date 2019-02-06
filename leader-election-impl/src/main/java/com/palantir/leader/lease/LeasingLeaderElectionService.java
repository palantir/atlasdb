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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.palantir.common.time.NanoTime;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PingableLeader;

/**
 * A leadership election service which implements 'leadership leases' on top of an underlying leader election service.
 * <p>
 * Naively, one must verify their leadership before every call against a quorum of nodes. This works, but adds constant
 * extra RPC load. One can improve this status quo by caching leadership. Nodes take a time X as their 'expiry' time
 * and a time 'Y' << 'X' as their refresh time. After becoming leader in the underlying service, one must wait for
 * at least 'X' time before returning their result. Now, once leader, one may check that they are still the leader
 * by polling every 'Y' time rather than every single check.
 * <p>
 * While this does rely on wall clock time, it uses the high precision now counters rather than the usual mutable
 * wall clock time. This means that breaking the leadership lease would require (on default settings) one computer's
 * clock to run twice as fast as another's.
 */
public final class LeasingLeaderElectionService implements LeaderElectionService {
    private static final Duration LEASE_REFRESH = Duration.ofSeconds(1);
    private static final Duration LEASE_EXPIRY = Duration.ofSeconds(2);

    private final LeasingRequirements leasingRequirements;
    private final LeaderElectionService delegate;
    private final LoadingCache<LeadershipToken, LeasingLeadershipToken> leaseTokens = Caffeine.newBuilder()
            .maximumSize(1)
            .build(this::createLeaseToken);

    private final Duration leaseRefresh;
    private final Duration leaseExpiry;

    @VisibleForTesting
    LeasingLeaderElectionService(
            LeasingRequirements leasingRequirements,
            LeaderElectionService delegate,
            Duration leaseRefresh,
            Duration leaseExpiry) {
        this.leasingRequirements = leasingRequirements;
        this.delegate = delegate;
        this.leaseRefresh = leaseRefresh;
        this.leaseExpiry = leaseExpiry;
    }

    public static LeaderElectionService wrap(LeaderElectionService delegate) {
        LeasingRequirements weHaventRolledOutTheLeadershipDelayYet = () -> false;
        return new LeasingLeaderElectionService(
                weHaventRolledOutTheLeadershipDelayYet, delegate, LEASE_REFRESH, LEASE_EXPIRY);
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        try {
            return leaseTokens.get(delegate.blockOnBecomingLeader());
        } catch (CompletionException e) {
            if (e.getCause() instanceof InterruptedException) {
                throw new InterruptedException();
            }
            throw e;
        }
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return delegate.getCurrentTokenIfLeading().map(leaseTokens::getIfPresent);
    }

    @Override
    public StillLeadingStatus isStillLeading(LeadershipToken token) {
        return ((LeasingLeadershipToken) token).getLeadershipStatus();
    }

    @Override
    public Optional<HostAndPort> getSuspectedLeaderInMemory() {
        return delegate.getSuspectedLeaderInMemory();
    }

    @Override
    public Set<PingableLeader> getPotentialLeaders() {
        return delegate.getPotentialLeaders();
    }

    private LeasingLeadershipToken createLeaseToken(LeadershipToken token) throws InterruptedException {
        LeasingLeadershipToken wrapped =
                new LeasingLeadershipToken(token, leaseRefresh, () -> delegate.isStillLeading(token),
                        leasingRequirements);
        NanoTime.sleepUntil(NanoTime.now().plus(leaseExpiry));
        return wrapped;
    }
}
