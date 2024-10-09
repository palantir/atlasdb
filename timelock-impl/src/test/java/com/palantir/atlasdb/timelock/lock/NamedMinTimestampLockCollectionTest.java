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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class NamedMinTimestampLockCollectionTest {
    private static final UUID REQUEST_ID_1 = UUID.randomUUID();
    private static final UUID REQUEST_ID_2 = UUID.randomUUID();
    private static final UUID REQUEST_ID_3 = UUID.randomUUID();

    private final NamedMinTimestampLockCollection locks = new NamedMinTimestampLockCollection();

    // It is especially important to block interactions where the timestamp name is ImmutableTimestamp
    @ValueSource(strings = {"ImmutableTimestamp", "timestamp", "foo", "", "immutable"})
    @ParameterizedTest
    public void doesNotAllowNamedMinTimestampInteractionsWhereNameIsNotCommitImmutableTimestamp(String timestampName) {
        assertThatLoggableExceptionThrownBy(() -> locks.getNamedMinTimestampLock(timestampName, 1))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Unknown named timestamp")
                .hasExactlyArgs(SafeArg.of("name", timestampName));

        assertThatLoggableExceptionThrownBy(() -> locks.getNamedMinTimestamp(timestampName))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Unknown named timestamp")
                .hasExactlyArgs(SafeArg.of("name", timestampName));
    }

    @ValueSource(strings = {"CommitImmutableTimestamp"})
    @ParameterizedTest
    public void namedMinTimestampLockDescriptorMatchesFormatForAllowedTimestampNames(String timestampName) {
        assertThat(getNamedMinTimestampLock(timestampName, 1).getDescriptor().getBytes())
                .isEqualTo((timestampName + ":1").getBytes(StandardCharsets.UTF_8));

        assertThat(getNamedMinTimestampLock(timestampName, 91).getDescriptor().getBytes())
                .isEqualTo((timestampName + ":91").getBytes(StandardCharsets.UTF_8));
    }

    @ValueSource(strings = {"CommitImmutableTimestamp"})
    @ParameterizedTest
    public void namedMinTimestampIsEmptyWhenNoLocksAreActiveForAllowedTimestampNames(String timestampName) {
        assertThat(getNamedMinTimestamp(timestampName)).isEmpty();

        AsyncLock lock = getNamedMinTimestampLock(timestampName, 1);
        lock.lock(REQUEST_ID_1);
        lock.unlock(REQUEST_ID_1);

        assertThat(getNamedMinTimestamp(timestampName)).isEmpty();
    }

    @ValueSource(strings = {"CommitImmutableTimestamp"})
    @ParameterizedTest
    public void getNamedMinTimestampReturnsMinimumLockedValueAllThroughoutForAllowedTimestampNames(
            String timestampName) {
        AsyncLock lock1 = getNamedMinTimestampLock(timestampName, 100);
        lock1.lock(REQUEST_ID_1);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(100L);

        AsyncLock lock2 = getNamedMinTimestampLock(timestampName, 50);
        lock2.lock(REQUEST_ID_2);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(50L);

        AsyncLock lock3 = getImmutableTimestampLock(75);
        lock3.lock(REQUEST_ID_3);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(50L);

        lock3.unlock(REQUEST_ID_3);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(50L);

        lock2.unlock(REQUEST_ID_2);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(100L);
    }

    @Test
    public void immutableTimestampLockDescriptorMatchesFormat() {
        assertThat(getImmutableTimestampLock(1).getDescriptor().getBytes())
                .isEqualTo("ImmutableTimestamp:1".getBytes(StandardCharsets.UTF_8));

        assertThat(getImmutableTimestampLock(91).getDescriptor().getBytes())
                .isEqualTo("ImmutableTimestamp:91".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void immutableTimestampIsEmptyWhenNoLocksAreActive() {
        assertThat(getImmutableTimestamp()).isEmpty();

        AsyncLock lock = getImmutableTimestampLock(1);
        lock.lock(REQUEST_ID_1);
        lock.unlock(REQUEST_ID_1);

        assertThat(getImmutableTimestamp()).isEmpty();
    }

    @Test
    public void immutableTimestampReturnsMinimumLockedValueAllThroughout() {
        AsyncLock lock1 = getImmutableTimestampLock(100);
        lock1.lock(REQUEST_ID_1);
        assertThat(getImmutableTimestamp()).hasValue(100L);

        AsyncLock lock2 = getImmutableTimestampLock(50);
        lock2.lock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        AsyncLock lock3 = getImmutableTimestampLock(75);
        lock3.lock(REQUEST_ID_3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock3.unlock(REQUEST_ID_3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock2.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(100L);
    }

    @Test
    public void concurrentLockingAndUnlockOfDifferentTimestampsDoNotInterfereWithOneAnother() {
        AsyncLock immutableTimestampLock1 = getImmutableTimestampLock(100);
        immutableTimestampLock1.lock(REQUEST_ID_1);
        AsyncLock namedMinTimestampLock1 = getNamedMinTimestampLock("CommitImmutableTimestamp", 80);
        namedMinTimestampLock1.lock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(100L);
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).hasValue(80L);

        // Intentionally re-using the request ids
        AsyncLock immutableTimestampLock2 = getImmutableTimestampLock(90);
        immutableTimestampLock2.lock(REQUEST_ID_2);
        AsyncLock namedMinTimestampLock2 = getNamedMinTimestampLock("CommitImmutableTimestamp", 90);
        namedMinTimestampLock2.lock(REQUEST_ID_1);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).hasValue(80L);

        namedMinTimestampLock2.unlock(REQUEST_ID_1);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).hasValue(80L);

        immutableTimestampLock1.unlock(REQUEST_ID_1);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).hasValue(80L);

        namedMinTimestampLock1.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).isEmpty();

        immutableTimestampLock2.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).isEmpty();
        assertThat(getNamedMinTimestamp("CommitImmutableTimestamp")).isEmpty();
    }

    private Optional<Long> getNamedMinTimestamp(String timestampName) {
        return locks.getNamedMinTimestamp(timestampName);
    }

    private AsyncLock getNamedMinTimestampLock(String timestampName, long timestamp) {
        return locks.getNamedMinTimestampLock(timestampName, timestamp);
    }

    private Optional<Long> getImmutableTimestamp() {
        return locks.getImmutableTimestamp();
    }

    private AsyncLock getImmutableTimestampLock(long timestamp) {
        return locks.getImmutableTimestampLock(timestamp);
    }
}
