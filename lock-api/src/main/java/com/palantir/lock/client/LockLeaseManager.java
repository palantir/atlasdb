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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.lock.v2.LockToken;

public class LockLeaseManager {
    private final Supplier<Long> clock;
    private final Cache<LockToken, Long> leasedTokens;

    public static LockLeaseManager create() {
        return new LockLeaseManager(System::nanoTime, Duration.ofSeconds(1));
    }

    @VisibleForTesting
    LockLeaseManager(Supplier<Long> clock, Duration leaseExpiry) {
        this.clock = clock;
        this.leasedTokens = Caffeine.newBuilder()
                .expireAfterWrite(leaseExpiry.toNanos(), TimeUnit.NANOSECONDS)
                .build();
    }

    public void updateLease(LockToken lockToken, long expiry) {
        leasedTokens.put(lockToken, expiry);
    }

    public void invalidate(LockToken lockToken) {
        leasedTokens.invalidate(lockToken);
    }

    public boolean isValid(LockToken lockToken) {
        Long expiry = leasedTokens.getIfPresent(lockToken);
        return expiry != null && expiry > clock.get();
    }
}
