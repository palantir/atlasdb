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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockToken;

public class LockLeaseManager {
    private final Supplier<LeaderTime> leaderTimeSupplier;
    private final Cache<LockToken, Long> leasedTokens;
    private final AtomicReference<UUID> currentLeaderId;

    public static LockLeaseManager create(Supplier<LeaderTime> clock) {
        return new LockLeaseManager(clock, Duration.ofSeconds(1));
    }

    @VisibleForTesting
    LockLeaseManager(Supplier<LeaderTime> leaderTimeSupplier, Duration leaseExpiry) {
        this.leaderTimeSupplier = leaderTimeSupplier;
        this.leasedTokens = Caffeine.newBuilder()
                .expireAfterWrite(leaseExpiry.toNanos(), TimeUnit.NANOSECONDS)
                .weakKeys()
                .build();
        currentLeaderId = new AtomicReference<>(leaderTimeSupplier.get().getLeaderUUID());
    }

    public void updateLease(LockToken lockToken, long expiry) {
        leasedTokens.put(lockToken, expiry);
    }

    public void invalidate(LockToken lockToken) {
        leasedTokens.invalidate(lockToken);
    }

    public boolean isValid(LockToken lockToken) {
        return !isValid(ImmutableSet.of(lockToken)).isEmpty();
    }

    public Set<LockToken> isValid(Set<LockToken> lockTokens) {
        LeaderTime leaderTime = leaderTimeSupplier.get();
        checkClockId(leaderTime.getLeaderUUID());
        return leasedTokens.getAllPresent(lockTokens).entrySet().stream()
                .filter(leasedToken -> leasedToken.getValue() > leaderTime.currentTimeNanos())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }


    private synchronized void checkClockId(UUID id) { //does this need to be synchronized?
        UUID previousId = currentLeaderId.getAndSet(id);
        if (previousId != id) {
            leasedTokens.invalidateAll();
        }
    }
}
