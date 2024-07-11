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

package com.palantir.atlasdb.sweep.asts.locks;

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockableComparator;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

@ExtendWith(MockitoExtension.class)
public final class LockableTest {
    private static final TestLockable TEST_LOCKABLE = TestLockable.of(1);
    private final SettableRefreshable<Duration> lockTimeout = Refreshable.create(Duration.ofSeconds(1));
    private Lockable<TestLockable> lockable;

    @Mock
    private TimelockService timelockService;

    @BeforeEach
    public void setup() {
        lockable = createLockable(TEST_LOCKABLE);
    }

    @Test
    public void comparatorUsesInnerObjectForComparison() {
        TestLockable lockableOne = mock(TestLockable.class);
        TestLockable lockableTwo = mock(TestLockable.class);
        Lockable<TestLockable> lockOne = createLockable(lockableOne);
        Lockable<TestLockable> lockTwo = createLockable(lockableTwo);
        LockableComparator<TestLockable> comparator = new LockableComparator<>();

        int _result = comparator.compare(lockOne, lockTwo);
        verify(lockableOne).compareTo(lockableTwo);
    }

    @Test
    public void tryLockLocksObjectLocally() {
        withLockMock().thenReturn(lockResponse(lockToken()));
        Optional<Lockable<TestLockable>.Inner> lockedValue = lockable.tryLock();
        assertThatValueIsLocked(lockedValue);

        Optional<Lockable<TestLockable>.Inner> attemptTwo = lockable.tryLock();
        assertThat(attemptTwo).isEmpty();
    }

    @Test
    public void tryLockLocksObjectInTimelock() {
        withLockMock().thenReturn(lockResponse(lockToken()));
        Optional<Lockable<TestLockable>.Inner> lockedValue = lockable.tryLock();
        verify(timelockService)
                .lock(LockRequest.of(
                        ImmutableSet.of(TEST_LOCKABLE.lockDescriptor()),
                        lockTimeout.get().toMillis()));
    }

    @Test
    public void tryLockReturnsEmptyAndUnlocksLocallyIfTimelockCallFails() {
        withLockMock().thenThrow(new RuntimeException("Timelock is down")).thenReturn(lockResponse(lockToken()));
        Optional<Lockable<TestLockable>.Inner> lockedValue = lockable.tryLock();
        assertThat(lockedValue).isEmpty();

        Optional<Lockable<TestLockable>.Inner> attemptTwo = lockable.tryLock();
        assertThatValueIsLocked(attemptTwo);
    }

    @Test
    public void closingInnerCallsTimelockUnlock() {
        LockToken lockToken = lockToken();
        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<Lockable<TestLockable>.Inner> lockedValue = lockable.tryLock();
        assertThatValueIsLocked(lockedValue);

        withUnlockMock(lockToken).thenReturn(ImmutableSet.of(lockToken));
        lockedValue.orElseThrow().close();
        verify(timelockService).unlock(ImmutableSet.of(lockToken));
    }

    @Test
    public void closingInnerRemovesIsLockedFlagEvenIfTimelockUnlockFails() {
        LockToken lockToken = lockToken();

        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<Lockable<TestLockable>.Inner> lockedValue = lockable.tryLock();
        assertThatValueIsLocked(lockedValue);

        RuntimeException exception = new RuntimeException("Timelock is down");
        withUnlockMock(lockToken).thenThrow(exception);
        assertThatThrownBy(lockedValue.orElseThrow()::close).isEqualTo(exception);
        assertThat(lockable.tryLock()).isPresent();
    }

    @Test
    public void requestsUseCurrentLockTimeout() {
        lockTimeout.update(Duration.ofSeconds(2));
        LockToken lockToken = lockToken();
        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<Lockable<TestLockable>.Inner> lockedValueTwoSeconds = lockable.tryLock();

        verify(timelockService)
                .lock(LockRequest.of(
                        ImmutableSet.of(TEST_LOCKABLE.lockDescriptor()),
                        Duration.ofSeconds(2).toMillis()));

        lockedValueTwoSeconds.orElseThrow().close();

        lockTimeout.update(Duration.ofSeconds(3));
        withLockMock().thenReturn(lockResponse(lockToken()));
        Optional<Lockable<TestLockable>.Inner> _lockedValueThreeSeconds = lockable.tryLock();

        verify(timelockService)
                .lock(LockRequest.of(
                        ImmutableSet.of(TEST_LOCKABLE.lockDescriptor()),
                        Duration.ofSeconds(3).toMillis()));
    }

    private Lockable<TestLockable> createLockable(TestLockable value) {
        return Lockable.create(value, value.lockDescriptor(), timelockService, lockTimeout);
    }

    private OngoingStubbing<LockResponse> withLockMock() {
        return when(timelockService.lock(LockRequest.of(
                ImmutableSet.of(LockableTest.TEST_LOCKABLE.lockDescriptor()),
                lockTimeout.get().toMillis())));
    }

    private OngoingStubbing<Set<LockToken>> withUnlockMock(LockToken lockToken) {
        return when(timelockService.unlock(ImmutableSet.of(lockToken)));
    }

    private LockToken lockToken() {
        return LockToken.of(UUID.randomUUID());
    }

    private LockResponse lockResponse(LockToken lockToken) {
        return LockResponse.successful(lockToken);
    }

    private void assertThatValueIsLocked(Optional<Lockable<TestLockable>.Inner> maybeLocked) {
        assertThat(maybeLocked).hasValueSatisfying(inner -> Assertions.assertThat(inner.getInner())
                .isEqualTo(TEST_LOCKABLE));
    }
}
