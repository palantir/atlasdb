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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LockableFactoryTest {
    @Mock
    private TimelockService timelockService;

    private LockableFactory<TestLockable> factory;

    @BeforeEach
    public void setup() {
        factory = LockableFactory.create(
                timelockService, Refreshable.create(Duration.ofSeconds(1)), TestLockable::lockDescriptor);
    }

    @Test
    public void createReturnsSameInstanceIfUnderlyingObjectEqualEvenIfNotSameObject() {
        TestLockable lockable = TestLockable.of(1);
        TestLockable lockable2 = TestLockable.of(1);
        Lockable<TestLockable> lock1 = factory.createLockable(lockable);
        Lockable<TestLockable> lock2 = factory.createLockable(lockable2);
        assertThat(lock1).isSameAs(lock2);
    }

    @Test
    public void createReturnsDifferentInstanceIfUnderlyingObjectDifferent() {
        TestLockable lockable1 = TestLockable.of(1);
        TestLockable lockable2 = TestLockable.of(2);
        Lockable<TestLockable> lock1 = factory.createLockable(lockable1);
        Lockable<TestLockable> lock2 = factory.createLockable(lockable2);
        assertThat(lock1).isNotSameAs(lock2);
    }

    @Test
    public void createReturnsDifferentObjectIfValueIsGarbageCollected() {
        TestLockable lockable = TestLockable.of(1);
        mockLockRequest(lockable);
        Lockable<TestLockable> lock1 = factory.createLockable(lockable);
        Optional<Lockable<TestLockable>.Inner> inner = lock1.tryLock();
        assertThat(inner).isPresent(); // We've got a lock

        lock1 = null; // Lose the references
        inner = Optional.empty(); // inner maintains a reference to the lockable object.

        System.gc();
        Lockable<TestLockable> lock2 = factory.createLockable(lockable);
        Optional<Lockable<TestLockable>.Inner> newInner = lock2.tryLock();
        assertThat(newInner).isPresent(); // We've got a lock again, even though the original object was locked, which
        // means, provided the local locking works, this is a new object.s
    }

    private void mockLockRequest(TestLockable lockable) {
        when(timelockService.lock(LockRequest.of(ImmutableSet.of(lockable.lockDescriptor()), 1000)))
                .thenReturn(LockResponse.successful(LockToken.of(UUID.randomUUID())));
    }

    // TODO: I need to test that we don't lose the reference if we construct a new lockable with the same underlying
    // value
}
