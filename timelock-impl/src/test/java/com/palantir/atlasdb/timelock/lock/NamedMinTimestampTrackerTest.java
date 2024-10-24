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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public final class NamedMinTimestampTrackerTest {
    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private static final long TIMESTAMP_1 = 5L;

    private final NamedMinTimestampTracker tracker =
            NamedMinTimestampTrackerImpl.create("ts", TimestampLeaseMetrics.of(new DefaultTaggedMetricRegistry()));

    @Test
    public void registersTimestampWhenLocked() {
        lock(TIMESTAMP_1, REQUEST_1);
        assertThat(tracker.getMinimumTimestamp()).hasValue(TIMESTAMP_1);
    }

    @Test
    public void returnsEmptyWhenNoTimestampsAreRegistered() {
        assertThat(tracker.getMinimumTimestamp()).isEmpty();
    }

    @Test
    public void unRegistersTimestampWhenUnlocked() {
        lock(TIMESTAMP_1, REQUEST_1);
        unlock(TIMESTAMP_1, REQUEST_1);
        assertThat(tracker.getMinimumTimestamp()).isEmpty();
    }

    @Test
    public void lockingSameTimestampTwiceThrows() {
        lock(TIMESTAMP_1, REQUEST_1);

        assertThatLoggableExceptionThrownBy(() -> lock(TIMESTAMP_1, REQUEST_1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("A request attempted to lock a timestamp that was already locked")
                .hasExactlyArgs(
                        SafeArg.of("timestamp", TIMESTAMP_1),
                        SafeArg.of("requestId", REQUEST_1),
                        SafeArg.of("currentHolder", REQUEST_1));

        assertThatLoggableExceptionThrownBy(() -> lock(TIMESTAMP_1, REQUEST_2))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("A request attempted to lock a timestamp that was already locked")
                .hasExactlyArgs(
                        SafeArg.of("timestamp", TIMESTAMP_1),
                        SafeArg.of("requestId", REQUEST_2),
                        SafeArg.of("currentHolder", REQUEST_1));
    }

    @Test
    public void unlockingATimestampThatIsNotLockedThrows() {
        assertThatLoggableExceptionThrownBy(() -> unlock(TIMESTAMP_1, REQUEST_1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("A request attempted to unlock a timestamp that was not locked or was locked by another"
                        + " request")
                .hasExactlyArgs(
                        SafeArg.of("timestamp", TIMESTAMP_1),
                        SafeArg.of("requestId", REQUEST_1),
                        SafeArg.of("currentHolder", null));
    }

    @Test
    public void returnsSmallestLockedTimestampAllThroughout() {
        UUID request1 = UUID.randomUUID();
        lock(5, request1);

        UUID request2 = UUID.randomUUID();
        lock(3, request2);

        assertThat(tracker.getMinimumTimestamp()).hasValue(3L);

        UUID request3 = UUID.randomUUID();
        lock(4, request3);

        assertThat(tracker.getMinimumTimestamp()).hasValue(3L);

        unlock(3, request2);
        assertThat(tracker.getMinimumTimestamp()).hasValue(4L);

        unlock(4, request3);
        assertThat(tracker.getMinimumTimestamp()).hasValue(5L);
    }

    @Test
    public void lockDescriptorMatchesFormat() {
        assertThat(tracker.getDescriptor(10).getBytes()).isEqualTo("ts:10".getBytes(StandardCharsets.UTF_8));

        assertThat(tracker.getDescriptor(910).getBytes()).isEqualTo("ts:910".getBytes(StandardCharsets.UTF_8));
    }

    private void lock(long timestamp, UUID requestId) {
        tracker.lock(timestamp, requestId);
    }

    private void unlock(long timestamp, UUID requestId) {
        tracker.unlock(timestamp, requestId);
    }
}
