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

import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
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
        TestLockable item1 = TestLockable.of(1);
        TestLockable item2 = TestLockable.of(1);
        Lockable<TestLockable> lockable1 = factory.createLockable(item1);
        Lockable<TestLockable> lockable2 = factory.createLockable(item2);
        assertThat(lockable1).isSameAs(lockable2);
        assertThat(item1).isNotSameAs(item2);
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
        Lockable<TestLockable> lock1 = factory.createLockable(lockable);
        int lock1HashCode = lock1.hashCode();
        lock1 = null; // Lose the references

        System.gc();
        Lockable<TestLockable> lock2 = factory.createLockable(lockable);
        int lock2HashCode = lock2.hashCode();
        assertThat(lock1HashCode).isNotEqualTo(lock2HashCode);
    }
}
