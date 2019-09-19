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
package com.palantir.lock.impl;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A reentrant lock linked to a specific {@link LockClient}.
 *
 * @author jtamer
 */
public interface KnownClientLock {

    /** Returns the lock mode (read or write). */
    LockMode getMode();

    /**
     * Acquires the lock. If the lock is not available, then the current thread
     * becomes disabled for thread scheduling purposes and lies dormant until
     * the lock has been acquired.
     */
    void lock();

    /** Acquires the lock unless the current thread is interrupted. */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Acquires the lock only if it is free at the time of invocation. This
     * method does not block.
     *
     * @return {@code null} if the lock was acquired, or one of the current
     *         holders of the lock if it could not be acquired immediately.
     */
    @Nullable LockClient tryLock();

    /**
     * Acquires the lock if it is free within the given waiting time and the
     * current thread has not been interrupted.
     *
     * @return {@code null} if the lock was acquired, or one of the current
     *         holders of the lock if it could not be acquired immediately.
     */
    @Nullable LockClient tryLock(long time, TimeUnit unit) throws InterruptedException;

    /**
     * Changes the state of this lock so that it is held by {@code newClient}
     * instead of by the registered client. Note that the registered
     * "known client" for this lock object does not change. This method does not
     * block.
     *
     * @throws IllegalMonitorStateException if the registered client does not
     *         currently hold this lock, or if the registered client is
     *         simultaneously holding both the read lock and the write lock, or
     *         if this is a write lock which has been locked reentrantly (i.e.
     *         the hold count is currently greater than one), or if the lock is
     *         frozen.
     */
    void changeOwner(LockClient newOwner);

    /** Releases the lock. */
    void unlock();

    /**
     * Calls {@link #unlock()} and changes this lock to a "frozen" state. All
     * future calls (including reentrant calls) to {@link #lock()} or related
     * methods will not be granted until this lock is unfrozen, which happens
     * when the read and write hold counts both drop to zero. (If this lock was
     * not opened reentrantly, then this method is equivalent to
     * {@link #unlock()} except for error conditions.) This method may only be
     * called if {@link #getMode()} is {@link LockMode#WRITE} and if the
     * registered "known client" is not anonymous.
     *
     * @throws IllegalMonitorStateException if the registered client is
     *         anonymous or if this is a read lock instead of a write lock.
     */
    void unlockAndFreeze();

    /**
     * Returns true if and only if the lock is believed to be held by this lock client.
     */
    boolean isHeld();
}
