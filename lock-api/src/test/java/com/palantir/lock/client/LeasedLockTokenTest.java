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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import java.time.Duration;
import java.util.UUID;
import org.junit.Test;

public class LeasedLockTokenTest {
    private static final ConjureLockToken LOCK_TOKEN = ConjureLockToken.of(UUID.randomUUID());
    private static final LeadershipId LEADER_ID = LeadershipId.random();
    private static final LeadershipId OTHER_LEADER_ID = LeadershipId.random();

    private static final Duration LEASE_TIMEOUT = Duration.ofMillis(1234);

    private NanoTime currentTime = NanoTime.createForTests(123);

    @Test
    public void shouldCreateValidTokensUntilExpiry() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertThat(token.isValid(getIdentifiedTime())).isTrue();

        advance(LEASE_TIMEOUT.minus(Duration.ofNanos(1)));
        assertThat(token.isValid(getIdentifiedTime())).isTrue();
    }

    @Test
    public void shouldBeInvalidAfterExpiry() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_TIMEOUT);
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }

    @Test
    public void shouldBeInvalidAfterInvalidation() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        token.invalidate();
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }

    @Test
    public void shouldBeInvalidAfterInvalidationAndExpiry() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_TIMEOUT);
        token.invalidate();
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }

    @Test
    public void refreshShouldExtendValidity() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_TIMEOUT.minus(Duration.ofNanos(1)));

        assertValid(token);

        token.updateLease(getLease());
        advance(LEASE_TIMEOUT.minus(Duration.ofNanos(1)));

        assertValid(token);
    }

    @Test
    public void refreshShouldExtendValidityOfExpiredToken() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_TIMEOUT);

        assertInvalid(token);

        token.updateLease(getLease());
        assertExpiresExactlyAfter(token, LEASE_TIMEOUT);
    }

    @Test
    public void ignoreRefreshIfNewLeaseHasShorterLife() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertExpiresExactlyAfter(token, LEASE_TIMEOUT);

        token.updateLease(Lease.of(getIdentifiedTime(), LEASE_TIMEOUT.minus(Duration.ofNanos(123))));
        assertExpiresExactlyAfter(token, LEASE_TIMEOUT);
    }

    @Test
    public void ignoreRefreshIfNewLeaseGoesBackInTime() {
        Lease oldLease = getLease();
        advance(Duration.ofNanos(123));

        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertExpiresExactlyAfter(token, LEASE_TIMEOUT);

        token.updateLease(oldLease);
        assertExpiresExactlyAfter(token, LEASE_TIMEOUT);
    }

    @Test
    public void throwIfRefreshedWithDifferentLeaderId() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());

        Lease otherLease = Lease.of(LeaderTime.of(OTHER_LEADER_ID, currentTime), LEASE_TIMEOUT);
        assertThatThrownBy(() -> token.updateLease(otherLease))
                .hasMessageStartingWith("Lock leases can only be refreshed by lease owners.");
    }

    private void advance(Duration duration) {
        currentTime = currentTime.plus(duration);
    }

    private Lease getLease() {
        return Lease.of(getIdentifiedTime(), LEASE_TIMEOUT);
    }

    private LeaderTime getIdentifiedTime() {
        return LeaderTime.of(LEADER_ID, currentTime);
    }

    private void assertExpiresExactlyAfter(LeasedLockToken token, Duration duration) {
        LeadershipId tokenLeaderId = token.getLease().leaderTime().id();
        assertThat(token.isValid(LeaderTime.of(tokenLeaderId, currentTime.plus(duration.minus(Duration.ofNanos(1))))))
                .isTrue();
        assertThat(token.isValid(LeaderTime.of(tokenLeaderId, currentTime.plus(duration)))).isFalse();
    }

    private void assertValid(LeasedLockToken token) {
        assertThat(token.isValid(getIdentifiedTime())).isTrue();
    }

    private void assertInvalid(LeasedLockToken token) {
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }
}
