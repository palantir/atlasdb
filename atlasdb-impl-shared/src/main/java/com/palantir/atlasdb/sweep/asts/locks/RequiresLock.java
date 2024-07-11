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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.atlasdb.sweep.asts.locks.RequiresLock.Lockable;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

// This is _different_ to TargetedSweeperLock. I need to justify this approach, but I'm curious for opinions.
// I, personally, am much more used to working with locks being attached to the data object. It's, imo, harder to screw
// up and forget to lock / unlock things when a) you can't access the data without the lock and b) the error checker
// will shout at you for not unlocking
// It also made the local isLocked check easier to manage rather than having a separate mapping (or, you don't have it
// at
// all, but with so many threads and relatively high chance of some collisions, it seems like wasted RPCs otherwise)
public final class RequiresLock<T extends Lockable> {
    private final T inner;
    private final TimelockService timeLock;

    // Consider whether this should be a lock. It's simply an optimisation to avoid a check for timelock
    // each time you want to test if it's locked by something locally, but we must make sure there are no false
    // negatives. It's allowed that something local is working on this item and Timelock has released the lock
    // and this is still True (since that still lets us minimise conflicts by not working on it locally),
    // but not if something local has stopped working on this item.
    private final AtomicBoolean isLocked = new AtomicBoolean(false);

    private RequiresLock(T inner, TimelockService timeLock) {
        this.timeLock = timeLock;
        this.inner = inner;
    }

    public static <T extends Lockable> RequiresLock<T> create(T inner, TimelockService timeLock) {
        return new RequiresLock<>(inner, timeLock);
    }

    // For code owners:
    // Do not use this for multiple locks!! If you're not careful, you can make it really easy to deadlock
    // If you end up using this class and have a need for batch locks, implement a separate method that uses
    // Timelocks batch locking methods and ensure there's a consistent ordering when taking out local locks
    // to avoid circular waiting.
    public Optional<Inner> tryLock() {
        if (isLocked.compareAndSet(false, true)) {
            // We do not want the timeout to be too low to avoid a race condition where we give up too soon
            LockRequest request = LockRequest.of(ImmutableSet.of(inner.getLockDescriptor()), 100L);

            // I assume if this succeeds but fails, we won't try and refresh and will eventually give up.
            try {
                return timeLock.lock(request).getTokenOrEmpty().map(lockToken -> new Inner(inner, lockToken));
            } catch (Exception e) {
                isLocked.set(false);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    public interface Lockable {
        LockDescriptor getLockDescriptor();
    }

    // Expected to be used in a try closeable.
    public class Inner implements AutoCloseable {
        private final T inner;
        private final LockToken lockToken;

        @MustBeClosed
        public Inner(T inner, LockToken lockToken) {
            this.inner = inner;
            this.lockToken = lockToken;
        }

        public T getInner() {
            return inner;
        }

        @Override
        public void close() {
            try {
                timeLock.unlock(ImmutableSet.of(lockToken));
            } finally {
                isLocked.set(false); // We end up holding the local lock ever so slightly longer than necessary.
                // TODO: unsure whether to set isLocked false first or the other way around.
            }
        }
    }
}
