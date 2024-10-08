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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public final class NamedMinTimestampLockTest {
    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();
    private static final long TIMESTAMP_1 = 5L;
    private static final long TIMESTAMP_2 = 10L;

    private final NamedMinTimestampTracker tracker = new NamedMinTimestampTracker("ts");

    @Test
    public void registersTimestampWhenLockIsLocked() {
        AsyncLock lock = createLock(TIMESTAMP_1);
        lock.lock(REQUEST_1);
        assertThat(tracker.getMinimumTimestamp()).hasValue(TIMESTAMP_1);
    }

    @Test
    public void returnsEmptyWhenLockIsNotLocked() {
        createLock(TIMESTAMP_1);
        assertThat(tracker.getMinimumTimestamp()).isEmpty();
    }

    @Test
    public void unRegistersTimestampWhenLockIsUnlocked() {
        AsyncLock lock = createLock(TIMESTAMP_1);
        lock.lock(REQUEST_1);
        lock.unlock(REQUEST_1);

        assertThat(tracker.getMinimumTimestamp()).isEmpty();
    }

    @Test
    public void tracksTimestampsInOrderAroundLockAndUnlockEvents() {
        AsyncLock lock2 = createLock(TIMESTAMP_2);
        lock2.lock(REQUEST_1);
        AsyncLock lock1 = createLock(TIMESTAMP_1);
        lock1.lock(REQUEST_2);

        assertThat(tracker.getMinimumTimestamp()).hasValue(TIMESTAMP_1);

        lock1.unlock(REQUEST_2);
        assertThat(tracker.getMinimumTimestamp()).hasValue(TIMESTAMP_2);
    }

    @Test
    public void lockDescriptorMatchesFormat() {
        assertThat(createLock(5).getDescriptor().getBytes()).isEqualTo("ts:5".getBytes());

        assertThat(createLock(10).getDescriptor().getBytes()).isEqualTo("ts:10".getBytes());
    }

    private AsyncLock createLock(long timestamp) {
        return NamedMinTimestampLock.create(tracker, timestamp);
    }
}
