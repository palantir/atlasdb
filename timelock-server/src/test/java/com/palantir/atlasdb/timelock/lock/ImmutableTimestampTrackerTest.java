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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.Test;

public class ImmutableTimestampTrackerTest {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private static final long TIMESTAMP_1 = 1L;
    private static final long TIMESTAMP_2 = 2L;

    private final ImmutableTimestampTracker tracker = new ImmutableTimestampTracker();

    @Test
    public void registersTimestampWhenLocked() {
        lock(TIMESTAMP_1, REQUEST_1);

        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void returnsEmptyWhenNoTimestampsAreRegistered() {
        assertThat(tracker.getImmutableTimestamp()).isNotPresent();
    }

    @Test
    public void unRegistersTimestampWhenUnlocked() {
        AsyncLock lock1 = tracker.getLockFor(TIMESTAMP_1);
        lock1.lock(REQUEST_1);
        lock1.unlock(REQUEST_1);

        assertThat(tracker.getImmutableTimestamp()).isNotPresent();
    }

    @Test
    public void tracksTimestampsInOrder() {
        AsyncLock lock2 = tracker.getLockFor(TIMESTAMP_2);
        lock2.lock(REQUEST_1);
        AsyncLock lock1 = tracker.getLockFor(TIMESTAMP_1);
        lock1.lock(REQUEST_2);

        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_1);

        lock1.unlock(REQUEST_2);
        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_2);
    }

    @Test
    public void lockingSameTimestampTwiceThrows() {
        lock(TIMESTAMP_1, REQUEST_1);

        assertThatThrownBy(() -> lock(TIMESTAMP_1, REQUEST_1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> lock(TIMESTAMP_1, REQUEST_2)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unlockingATimestampThatIsNotLockedThrows() {
        assertThatThrownBy(() -> unlock(TIMESTAMP_1, REQUEST_1)).isInstanceOf(IllegalStateException.class);
    }

    private AsyncResult<Void> lock(long timestamp, UUID requestId) {
        return tracker.getLockFor(timestamp).lock(requestId);
    }

    private void unlock(long timestamp, UUID requestId) {
        tracker.getLockFor(timestamp).unlock(requestId);
    }
}
