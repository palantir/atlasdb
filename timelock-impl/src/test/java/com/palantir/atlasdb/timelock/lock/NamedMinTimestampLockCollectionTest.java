/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public final class NamedMinTimestampLockCollectionTest {
    private final NamedMinTimestampLockCollection locks = new NamedMinTimestampLockCollection();

    @Test
    public void immutableTimestampGetLockDescriptorMatchesFormat() {
        assertThat(getLock(1).getDescriptor().getBytes())
                .isEqualTo("ImmutableTimestamp:1".getBytes(StandardCharsets.UTF_8));

        assertThat(getLock(91).getDescriptor().getBytes())
                .isEqualTo("ImmutableTimestamp:91".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void immutableTimestampIsEmptyWhenNoLocksAreActive() {
        assertThat(getImmutableTimestamp()).isEmpty();

        AsyncLock lock = getLock(1);
        UUID requestId = UUID.randomUUID();
        lock.lock(requestId);
        lock.unlock(requestId);

        assertThat(getImmutableTimestamp()).isEmpty();
    }

    @Test
    public void immutableTimestampReturnsMinimumLockedValueAllThroughout() {
        AsyncLock lock1 = getLock(100);
        UUID requestId1 = UUID.randomUUID();
        lock1.lock(requestId1);
        assertThat(getImmutableTimestamp()).hasValue(100L);

        AsyncLock lock2 = getLock(50);
        UUID requestId2 = UUID.randomUUID();
        lock2.lock(requestId2);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        AsyncLock lock3 = getLock(75);
        UUID requestId3 = UUID.randomUUID();
        lock3.lock(requestId3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock3.unlock(requestId3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock2.unlock(requestId2);
        assertThat(getImmutableTimestamp()).hasValue(100L);
    }

    private Optional<Long> getImmutableTimestamp() {
        return locks.getImmutableTimestamp();
    }

    private AsyncLock getLock(long timestamp) {
        return locks.getImmutableTimestampLock(timestamp);
    }
}
