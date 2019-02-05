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

import java.time.Duration;
import java.util.UUID;

import org.junit.Test;

import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;

public class LeasedLockTokenTest {
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final UUID LEADER_ID = UUID.randomUUID();

    @Test
    public void shouldCreateValidTokensUntilExpiry() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, Lease.of(getIdentifiedTime(), Duration.ofSeconds(1)));
        assertThat(token.isValid(getIdentifiedTime())).isTrue();
    }

    @Test
    public void shouldBeInvalidAfterExpiry() throws Exception {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, Lease.of(getIdentifiedTime(), Duration.ofMillis(10)));
        Thread.sleep(15);
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }

    @Test
    public void shouldBeInvalidAfterInvalidation() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, Lease.of(getIdentifiedTime(), Duration.ofSeconds(1)));
        token.inValidate();
        assertThat(token.isValid(getIdentifiedTime())).isFalse();
    }

    private IdentifiedTime getIdentifiedTime() {
        return IdentifiedTime.of(LEADER_ID, NanoTime.now());
    }

}