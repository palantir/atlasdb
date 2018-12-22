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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
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
public final class LeadershipLeaseLeaderElectionService implements LeaderElectionService {
    private final LeaderElectionService delegate;
    private final LoadingCache<LeadershipToken, LeadershipLeaseLeadershipToken> leaseTokens = Caffeine.newBuilder()
            .maximumSize(1)
            .build(this::createLeaseToken);

    private final Duration leaseRefresh;
    private final Duration leaseExpiry;

    @VisibleForTesting
    LeadershipLeaseLeaderElectionService(
            LeaderElectionService delegate,
            Duration leaseRefresh,
            Duration leaseExpiry) {
        this.delegate = delegate;
        this.leaseRefresh = leaseRefresh;
        this.leaseExpiry = leaseExpiry;
    }

    // this method exists in order to provide a rolling migration mechanism for migrating to schema leases
    public static LeaderElectionService wrapWithoutLeasing(LeaderElectionService delegate) {
        return new LeadershipLeaseLeaderElectionService(delegate, Duration.ZERO, Duration.ofSeconds(2));
    }

    public static LeaderElectionService wrap(LeaderElectionService delegate) {
        return new LeadershipLeaseLeaderElectionService(delegate, Duration.ofSeconds(1), Duration.ofSeconds(2));
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        return leaseTokens.get(delegate.blockOnBecomingLeader());
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return delegate.getCurrentTokenIfLeading().map(leaseTokens::get);
    }

    @Override
    public StillLeadingStatus isStillLeading(LeadershipToken token) {
        return ((LeadershipLeaseLeadershipToken) token).getLeadershipStatus();
    }

    @Override
    public Optional<HostAndPort> getSuspectedLeaderInMemory() {
        return delegate.getSuspectedLeaderInMemory();
    }

    @Override
    public Set<PingableLeader> getPotentialLeaders() {
        return delegate.getPotentialLeaders();
    }

    private LeadershipLeaseLeadershipToken createLeaseToken(LeadershipToken token) throws InterruptedException {
        LeadershipLeaseLeadershipToken wrapped =
                new LeadershipLeaseLeadershipToken(token, leaseRefresh, () -> delegate.isStillLeading(token));
        NanoTime.sleepUntil(NanoTime.now().plus(leaseExpiry));
        return wrapped;
    }
}
