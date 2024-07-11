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
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

// See the LockableFactory comment for why keeping the value in the class ends up making things simpler from a local
// lock management perspective.
// Separately:
// I, personally, am much more used to working with locks being attached to the data object. It's, imo, harder to screw
// up and forget to lock / unlock things when a) you can't access the data without the lock and b) the error checker
// will shout at you for not unlocking
// TODO: Make the error checker shout at you
// It also made the local isLocked check easier to manage rather than having a separate mapping (or, you don't have it
// at all, but with so many threads and relatively high chance of some collisions, it seems like wasted RPCs otherwise)
public final class Lockable<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(Lockable.class);
    private final LockDescriptor lockDescriptor;
    private final T inner;
    private final TimelockService timeLock;
    private final Refreshable<Duration> lockTimeout;

    // Consider whether this should be a lock. It's simply an optimisation to avoid a check for timelock
    // each time you want to test if it's locked by something locally, but we must make sure there are no false
    // negatives. It's allowed that something local is working on this item and Timelock has released the lock
    // and this is still True (since that still lets us minimise conflicts by not working on it locally),
    // but not if something local has stopped working on this item.
    private final AtomicBoolean isLocked = new AtomicBoolean(false);

    private Lockable(
            T inner, LockDescriptor lockDescriptor, TimelockService timeLock, Refreshable<Duration> lockTimeout) {
        this.timeLock = timeLock;
        this.inner = inner;
        this.lockTimeout = lockTimeout;
        this.lockDescriptor = lockDescriptor;
    }

    public static <T> Lockable<T> create(
            T inner, LockDescriptor lockDescriptor, TimelockService timeLock, Refreshable<Duration> lockTimeout) {
        return new Lockable<>(inner, lockDescriptor, timeLock, lockTimeout);
    }

    // For code owners:
    // Do not use this for multiple locks!! If you're not careful, you can make it really easy to deadlock
    // If you end up using this class and have a need for batch locks, implement a separate method that uses
    // Timelocks batch locking methods and ensure there's a consistent ordering when taking out local locks
    // to avoid circular waiting.
    //    @MustBeClosed TODO: How do I get this working??
    public Optional<Inner> tryLock() {
        if (isLocked.compareAndSet(false, true)) {
            // We do not want the timeout to be too low to avoid a race condition where we give up too soon
            LockRequest request = LockRequest.of(
                    ImmutableSet.of(lockDescriptor), lockTimeout.get().toMillis());

            // I assume if this succeeds but fails, we won't try and refresh and will eventually give up.
            try {
                return timeLock.lock(request).getTokenOrEmpty().map(lockToken -> new Inner(inner, lockToken));
            } catch (Exception e) {
                log.error("Failed to acquire lock", UnsafeArg.of("lockDescriptor", lockDescriptor), e);
                isLocked.set(false);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    // Expected to be used in a try closeable.
    public class Inner implements AutoCloseable {
        private final T inner;
        private final LockToken lockToken;

        public Inner(T inner, LockToken lockToken) {
            this.inner = inner;
            this.lockToken = lockToken;
        }

        public T getInner() {
            return inner;
        }

        public Lockable<T> getOuter() {
            return Lockable.this;
        }

        @Override
        public void close() {
            try {
                timeLock.unlock(ImmutableSet.of(lockToken));
            } finally {
                isLocked.set(false); // We end up holding the local lock ever so slightly longer than necessary.
            }
        }
    }

    // Trait bounds!!
    public static class LockableComparator<T extends Comparable<T>> implements Comparator<Lockable<T>> {
        @Override
        public int compare(Lockable<T> o1, Lockable<T> o2) {
            return o1.inner.compareTo(o2.inner);
        }
    }
}
