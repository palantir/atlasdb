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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import com.palantir.lock.v2.LockToken;

public class LockLeaseManagerTest {
    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());

    private static final long CACHE_EXPIRY = 10L; //setting this to be a high value, as it is only used for GC purposes

    private MockClock clock;
    private LockLeaseManager lockLeaseManager;

    @Before
    public void setUp() {
        clock = new MockClock();
        lockLeaseManager = new LockLeaseManager(clock, Duration.ofSeconds(CACHE_EXPIRY));
    }

    @Test
    public void lockIsValidBeforeExpiry() {
        lockLeaseManager.updateLease(TOKEN_1, 10L);
        clock.setCurrentTime(5);
        assertTrue(lockLeaseManager.isValid(TOKEN_1));
    }

    @Test
    public void lockIsNotValidAfterExpiry() {
        lockLeaseManager.updateLease(TOKEN_1, 10L);
        clock.setCurrentTime(11);
        assertFalse(lockLeaseManager.isValid(TOKEN_1));
    }

    @Test
    public void shouldKeepMultipleLocks() {
        lockLeaseManager.updateLease(TOKEN_1, 10L);
        lockLeaseManager.updateLease(TOKEN_2, 10L);

        assertTrue(lockLeaseManager.isValid(TOKEN_1));
        assertTrue(lockLeaseManager.isValid(TOKEN_2));
    }

    @Test
    public void multipleExpiredLocks() {
        lockLeaseManager.updateLease(TOKEN_1, 10L);
        lockLeaseManager.updateLease(TOKEN_2, 10L);

        clock.setCurrentTime(11);

        assertFalse(lockLeaseManager.isValid(TOKEN_1));
        assertFalse(lockLeaseManager.isValid(TOKEN_2));
    }

    @Test
    public void invalidatesLocks() {
        lockLeaseManager.updateLease(TOKEN_1, 10L);

        lockLeaseManager.invalidate(TOKEN_1);

        assertFalse(lockLeaseManager.isValid(TOKEN_1));
    }

    private class MockClock implements Supplier<Long> {
        private long currentTime = 0;

        @Override
        public Long get() {
            return currentTime;
        }

        public synchronized void setCurrentTime(long target) {
            currentTime = target;
        }
    }
}
