/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

public class ImmutableTimestampTrackerTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private static final long TIMESTAMP_1 = 1L;
    private static final long TIMESTAMP_2 = 2L;
    private static final long TIMESTAMP_3 = 3L;

    private final ImmutableTimestampTracker tracker = new ImmutableTimestampTracker();

    @Test
    public void registersTimestampWhenLocked() {
        tracker.getLockFor(TIMESTAMP_1).lock(REQUEST_ID);

        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void returnsEmptyWhenNoTimestampsAreRegistered() {
        assertThat(tracker.getImmutableTimestamp()).isEqualTo(Optional.empty());
    }

    @Test
    public void unRegistersTimestampWhenUnlocked() {
        AsyncLock lock1 = tracker.getLockFor(TIMESTAMP_1);
        lock1.lock(REQUEST_ID);
        lock1.unlock(REQUEST_ID);

        assertThat(tracker.getImmutableTimestamp()).isEqualTo(Optional.empty());
    }

    @Test
    public void tracksTimestampsInOrder() {
        AsyncLock lock2 = tracker.getLockFor(TIMESTAMP_2);
        lock2.lock(REQUEST_ID);
        AsyncLock lock1 = tracker.getLockFor(TIMESTAMP_1);
        lock1.lock(REQUEST_ID);

        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_1);

        lock1.unlock(REQUEST_ID);
        assertThat(tracker.getImmutableTimestamp().get()).isEqualTo(TIMESTAMP_2);
    }

}
