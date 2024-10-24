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

import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class NamedMinTimestampLockCollectionTest {
    private static final UUID REQUEST_ID_1 = UUID.randomUUID();
    private static final UUID REQUEST_ID_2 = UUID.randomUUID();
    private static final UUID REQUEST_ID_3 = UUID.randomUUID();

    private final NamedMinTimestampLockCollection locks = new NamedMinTimestampLockCollection();

    @MethodSource("getTimestampLeaseNames")
    @ParameterizedTest
    public void namedMinTimestampLockDescriptorMatchesFormat(TimestampLeaseName timestampName) {
        assertThat(getNamedTimestampLock(timestampName, 1).getDescriptor().getBytes())
                .isEqualTo((timestampName.name() + ":1").getBytes(StandardCharsets.UTF_8));

        assertThat(getNamedTimestampLock(timestampName, 91).getDescriptor().getBytes())
                .isEqualTo((timestampName.name() + ":91").getBytes(StandardCharsets.UTF_8));
    }

    @MethodSource("getTimestampLeaseNames")
    @ParameterizedTest
    public void namedMinTimestampIsEmptyWhenNoLocksAreActive(TimestampLeaseName timestampName) {
        assertThat(getNamedMinTimestamp(timestampName)).isEmpty();

        AsyncLock lock = getNamedTimestampLock(timestampName, 1);
        lock.lock(REQUEST_ID_1);
        lock.unlock(REQUEST_ID_1);

        assertThat(getNamedMinTimestamp(timestampName)).isEmpty();
    }

    @MethodSource("getTimestampLeaseNames")
    @ParameterizedTest
    public void getNamedMinTimestampReturnsMinimumLockedValueAllThroughout(TimestampLeaseName timestampName) {
        AsyncLock lock1 = getNamedTimestampLock(timestampName, 100);
        lock1.lock(REQUEST_ID_1);
        assertThat(getNamedMinTimestamp(timestampName)).hasValue(100L);

        AsyncLock lock2 = getNamedTimestampLock(timestampName, 50);
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
        UUID requestId = UUID.randomUUID();
        lock.lock(requestId);
        lock.unlock(requestId);

        assertThat(getImmutableTimestamp()).isEmpty();
    }

    @Test
    public void immutableTimestampReturnsMinimumLockedValueAllThroughout() {
        AsyncLock lock1 = getImmutableTimestampLock(100);
        UUID requestId1 = UUID.randomUUID();
        lock1.lock(requestId1);
        assertThat(getImmutableTimestamp()).hasValue(100L);

        AsyncLock lock2 = getImmutableTimestampLock(50);
        UUID requestId2 = UUID.randomUUID();
        lock2.lock(requestId2);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        AsyncLock lock3 = getImmutableTimestampLock(75);
        UUID requestId3 = UUID.randomUUID();
        lock3.lock(requestId3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock3.unlock(requestId3);
        assertThat(getImmutableTimestamp()).hasValue(50L);

        lock2.unlock(requestId2);
        assertThat(getImmutableTimestamp()).hasValue(100L);
    }

    @Test
    public void concurrentLockingAndUnlockOfDifferentTimestampsDoNotInterfereWithOneAnother() {
        TimestampLeaseName timestampName1 = TimestampLeaseName.of("CommitImmutableTimestamp");
        TimestampLeaseName timestampName2 = TimestampLeaseName.of("ToyTimestamp");

        AsyncLock immutableTimestampLock1 = getImmutableTimestampLock(100);
        immutableTimestampLock1.lock(REQUEST_ID_1);
        AsyncLock namedMinTimestampLock1 = getNamedTimestampLock(timestampName1, 80);
        namedMinTimestampLock1.lock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(100L);
        assertThat(getNamedMinTimestamp(timestampName1)).hasValue(80L);
        assertThat(getNamedMinTimestamp(timestampName2)).isEmpty();

        // Intentionally re-using the request ids
        AsyncLock immutableTimestampLock2 = getImmutableTimestampLock(90);
        immutableTimestampLock2.lock(REQUEST_ID_2);
        AsyncLock namedMinTimestampLock2 = getNamedTimestampLock(timestampName1, 90);
        namedMinTimestampLock2.lock(REQUEST_ID_1);
        AsyncLock namedMinTimestampLock3 = getNamedTimestampLock(timestampName2, 70);
        namedMinTimestampLock3.lock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp(timestampName1)).hasValue(80L);
        assertThat(getNamedMinTimestamp(timestampName2)).hasValue(70L);

        namedMinTimestampLock2.unlock(REQUEST_ID_1);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp(timestampName1)).hasValue(80L);
        assertThat(getNamedMinTimestamp(timestampName2)).hasValue(70L);

        immutableTimestampLock1.unlock(REQUEST_ID_1);
        namedMinTimestampLock3.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp(timestampName1)).hasValue(80L);
        assertThat(getNamedMinTimestamp(timestampName2)).isEmpty();

        namedMinTimestampLock1.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).hasValue(90L);
        assertThat(getNamedMinTimestamp(timestampName1)).isEmpty();
        assertThat(getNamedMinTimestamp(timestampName2)).isEmpty();

        immutableTimestampLock2.unlock(REQUEST_ID_2);
        assertThat(getImmutableTimestamp()).isEmpty();
        assertThat(getNamedMinTimestamp(timestampName1)).isEmpty();
        assertThat(getNamedMinTimestamp(timestampName2)).isEmpty();
    }

    private Optional<Long> getNamedMinTimestamp(TimestampLeaseName timestampName) {
        return locks.getNamedMinTimestamp(timestampName);
    }

    private AsyncLock getNamedTimestampLock(TimestampLeaseName timestampName, long timestamp) {
        return locks.getNamedTimestampLock(timestampName, timestamp);
    }

    private Optional<Long> getImmutableTimestamp() {
        return locks.getImmutableTimestamp();
    }

    private AsyncLock getImmutableTimestampLock(long timestamp) {
        return locks.getImmutableTimestampLock(timestamp);
    }

    private static Set<TimestampLeaseName> getTimestampLeaseNames() {
        return Set.of(
                TimestampLeaseName.of("CommitImmutableTimestamp"),
                TimestampLeaseName.of("ToyTimestamp"),
                TimestampLeaseName.of("AnotherTimestamp"));
    }
}
