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
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockedItem;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    private static final Consumer<Lockable<TestLockable>> NO_OP = _ignored -> {};
    private final SettableRefreshable<Duration> lockTimeout = Refreshable.create(Duration.ofSeconds(1));
    private Lockable<TestLockable> lockable;

    @Mock
    private TimelockService timelockService;

    @BeforeEach
    public void setup() {
        lockable = createLockable(TEST_LOCKABLE);
    }

    @Test
    public void comparatorUsesProvidedComparatorForComparison() {
        TestLockable otherLockable = TestLockable.of(2);
        Lockable<TestLockable> lockOne = createLockable(TEST_LOCKABLE);
        Lockable<TestLockable> lockTwo = createLockable(otherLockable);
        Comparator<TestLockable> comparator = mock(Comparator.class);
        LockableComparator<TestLockable> lockableComparator = new LockableComparator<>(comparator);

        lockableComparator.compare(lockOne, lockTwo);
        verify(comparator).compare(TEST_LOCKABLE, otherLockable);
    }

    @Test
    public void tryLockLocksObjectLocally() {
        withLockMock().thenReturn(lockResponse(lockToken()));
        Optional<LockedItem<TestLockable>> lockedValue = lockable.tryLock(NO_OP);
        assertThatValueIsLocked(lockedValue);

        Optional<LockedItem<TestLockable>> attemptTwo = lockable.tryLock(NO_OP);
        assertThat(attemptTwo).isEmpty();
    }

    @Test
    public void tryLockLocksObjectInTimelock() {
        withLockMock().thenReturn(lockResponse(lockToken()));

        // We have to assign to an unused variable as we explicitly mark the output of tryLock as @CheckReturnValue
        Optional<LockedItem<TestLockable>> _lockedValue = lockable.tryLock(NO_OP);
        verify(timelockService)
                .lock(LockRequest.of(
                        ImmutableSet.of(TEST_LOCKABLE.lockDescriptor()),
                        lockTimeout.get().toMillis()));
    }

    @Test
    public void tryLockReturnsEmptyAndUnlocksLocallyIfTimelockCallFails() {
        withLockMock().thenThrow(new RuntimeException("Timelock is down")).thenReturn(lockResponse(lockToken()));
        Optional<LockedItem<TestLockable>> lockedValue = lockable.tryLock(NO_OP);
        assertThat(lockedValue).isEmpty();

        Optional<LockedItem<TestLockable>> attemptTwo = lockable.tryLock(NO_OP);
        assertThatValueIsLocked(attemptTwo);
    }

    @Test
    public void closingLockedItemCallsTimelockUnlock() {
        LockToken lockToken = lockToken();
        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<LockedItem<TestLockable>> lockedValue = lockable.tryLock(NO_OP);
        assertThatValueIsLocked(lockedValue);

        withUnlockMock(lockToken).thenReturn(ImmutableSet.of(lockToken));
        lockedValue.orElseThrow().close();
        verify(timelockService).unlock(ImmutableSet.of(lockToken));
    }

    @Test
    public void closingLockedItemRemovesIsLockedFlagEvenIfTimelockUnlockFails() {
        LockToken lockToken = lockToken();

        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<LockedItem<TestLockable>> lockedValue = lockable.tryLock(NO_OP);
        assertThatValueIsLocked(lockedValue);

        RuntimeException exception = new RuntimeException("Timelock is down");
        withUnlockMock(lockToken).thenThrow(exception);
        assertThatThrownBy(lockedValue.orElseThrow()::close).isEqualTo(exception);
        assertThat(lockable.tryLock(NO_OP)).isPresent();
    }

    @Test
    public void closingLockedItemCallsDisposeCallbackWithSameLockableItem() {
        withLockMock().thenReturn(lockResponse(lockToken()));
        AtomicReference<Lockable<TestLockable>> disposedLockable = new AtomicReference<>();
        Optional<LockedItem<TestLockable>> lockedValue =
                lockable.tryLock(item -> assertThat(disposedLockable.compareAndSet(null, item))
                        .as("We should only be running the dispose callback once")
                        .isTrue());
        assertThatValueIsLocked(lockedValue);

        lockedValue.orElseThrow().close();
        // It must be the exact same object (not object equality, but _reference_ equality)
        assertThat(disposedLockable.get()).isSameAs(lockable);
    }

    @Test
    public void requestsUseCurrentLockTimeout() {
        lockTimeout.update(Duration.ofSeconds(2));
        LockToken lockToken = lockToken();
        withLockMock().thenReturn(lockResponse(lockToken));
        Optional<LockedItem<TestLockable>> lockedValueTwoSeconds = lockable.tryLock(NO_OP);

        verify(timelockService)
                .lock(LockRequest.of(
                        ImmutableSet.of(TEST_LOCKABLE.lockDescriptor()),
                        Duration.ofSeconds(2).toMillis()));

        lockedValueTwoSeconds.orElseThrow().close();

        lockTimeout.update(Duration.ofSeconds(3));
        withLockMock().thenReturn(lockResponse(lockToken()));

        Optional<LockedItem<TestLockable>> _lockedValueThreeSeconds = lockable.tryLock(NO_OP);

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

    private void assertThatValueIsLocked(Optional<LockedItem<TestLockable>> maybeLocked) {
        assertThat(maybeLocked).hasValueSatisfying(lockedItem -> Assertions.assertThat(lockedItem.getItem())
                .isEqualTo(TEST_LOCKABLE));
    }
}
