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
package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.palantir.util.MutuallyExclusiveSetLock.LockState;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MutuallyExclusiveSetLockTest {
    /** True iff test threads should release all their resources. */
    volatile boolean unlock;

    @BeforeEach
    public void setUp() {
        unlock = false;
    }

    @Test
    public void testInterface() {
        MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        LockState<String> lockOnObjects = mutuallyExclusiveSetLock.lockOnObjects(Arrays.asList("whatev", "dog"));
        try {
            // stuff
        } finally {
            //            assertEquals(2, mutuallyExclusiveSetLock.syncMap.size());
            //            assertEquals(1, mutuallyExclusiveSetLock.threadSet.size());
            mutuallyExclusiveSetLock.unlock(lockOnObjects);
            //            assertFalse(mutuallyExclusiveSetLock.syncMap.get("whatev").isHeldByCurrentThread());
            //            assertEquals(0, mutuallyExclusiveSetLock.threadSet.size());
        }
    }

    @Test
    public void testSimpleBlock() throws Exception {
        final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        LockState<String> lockOnObjects = mutuallyExclusiveSetLock.lockOnObjects(Arrays.asList("whatup", "dog"));
        final Thread thread;
        try {
            thread = createThread(mutuallyExclusiveSetLock, Arrays.asList("whatup"));
            thread.setDaemon(true);
            thread.start();
            Thread.sleep(100);
            assertThat(thread.isAlive()).isTrue();
            //            assertEquals(2, mutuallyExclusiveSetLock.syncMap.size());
        } finally {
            mutuallyExclusiveSetLock.unlock(lockOnObjects);
        }
        thread.join(10 * 1000);
        //        assertFalse(mutuallyExclusiveSetLock.syncMap.get("dog").isLocked());
        //        assertEquals(0, mutuallyExclusiveSetLock.threadSet.size());
    }

    @Test
    public void testSimpleNotBlock() throws Exception {
        final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        LockState<String> lockOnObjects = mutuallyExclusiveSetLock.lockOnObjects(Arrays.asList("whatup", "dog"));
        final Thread thread;
        try {
            thread = createThread(mutuallyExclusiveSetLock, Arrays.asList("heyo"));
            thread.setDaemon(true);
            thread.start();
            thread.join(10 * 1000);
        } finally {
            mutuallyExclusiveSetLock.unlock(lockOnObjects);
        }
    }

    @Test
    public void testDoubleLock() {
        final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        LockState<String> lockOnObjects = mutuallyExclusiveSetLock.lockOnObjects(Arrays.asList("whatup", "dog"));
        try {
            mutuallyExclusiveSetLock.lockOnObjects(Arrays.asList("anything"));
        } catch (Exception e) {
            return; // expected
        } finally {
            mutuallyExclusiveSetLock.unlock(lockOnObjects);
        }
        fail("fail"); // should have thrown
    }

    /* test that the current thread owns stuff it locks. */
    @Test
    public void testThreadOwnsLocks() {
        final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        List<String> asList = Arrays.asList("whatup", "dog");
        LockState<String> lockOnObjects = mutuallyExclusiveSetLock.lockOnObjects(asList);
        try {
            assertThat(mutuallyExclusiveSetLock.isLocked(asList)).isTrue();
        } finally {
            mutuallyExclusiveSetLock.unlock(lockOnObjects);
        }
    }

    /* test that the current thread does not own stuff that is not locked. */
    @Test
    public void testThreadDoesNotOwnUnlocked() {
        final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock = new MutuallyExclusiveSetLock<>();
        List<String> asList = Arrays.asList("whatup", "dog");
        assertThat(mutuallyExclusiveSetLock.isLocked(asList)).isFalse();
    }

    /* test that the current thread does not own stuff locked by another thread. */
    @Test
    public void testThreadDoesNotOwnOtherLocked() {
        final MutuallyExclusiveSetLock<String> setLock = new MutuallyExclusiveSetLock<>();
        final List<String> toLock = Arrays.asList("whatup", "dog");

        // Spawn a thread to lock and hold a lock until a variable is toggled
        Thread locker = new Thread(() -> {
            LockState<String> locked = setLock.lockOnObjects(toLock);
            try {
                while (!unlock) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                        // Ignored.
                    }
                }
            } finally {
                setLock.unlock(locked);
            }
        });

        locker.start();
        try {
            Thread.sleep(1000);
            assertThat(setLock.isLocked(toLock))
                    .describedAs("locks should be held by other thread")
                    .isFalse();
            unlock = true;
            locker.join();
        } catch (InterruptedException e) {
            unlock = true;
            fail("unexpected interruption: " + e);
        }
        assertThat(setLock.isLocked(toLock))
                .describedAs("locks should be held by other thread")
                .isFalse();
    }

    private Thread createThread(
            final MutuallyExclusiveSetLock<String> mutuallyExclusiveSetLock, final Collection<String> toLock) {
        final Thread thread;
        thread = new Thread(() -> {
            LockState<String> locked = mutuallyExclusiveSetLock.lockOnObjects(toLock);
            try {
                // stuff
            } finally {
                mutuallyExclusiveSetLock.unlock(locked);
            }
        });
        return thread;
    }
}
