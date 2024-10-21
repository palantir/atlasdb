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
import com.google.errorprone.annotations.CheckReturnValue;
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
import java.util.function.Consumer;

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
    private final TimelockService timelockService;
    private final Refreshable<Duration> lockAcquisitionTimeout;

    // Consider whether this should be a lock. It's simply an optimisation to avoid a check for timelock
    // each time you want to test if it's locked by something locally, but we must make sure there are no false
    // negatives. It's allowed that something local is working on this item and Timelock has released the lock due to
    // expiry and this is still True (since that still lets us minimise conflicts by not working on it locally),
    // but it should not be true if something local has stopped working on this item and closed the inner class
    // successfully.
    private final AtomicBoolean isLocked = new AtomicBoolean(false);

    private Lockable(
            T inner,
            LockDescriptor lockDescriptor,
            TimelockService timelockService,
            Refreshable<Duration> lockAcquisitionTimeout) {
        this.timelockService = timelockService;
        this.inner = inner;
        this.lockAcquisitionTimeout = lockAcquisitionTimeout;
        this.lockDescriptor = lockDescriptor;
    }

    public static <T> Lockable<T> create(
            T inner,
            LockDescriptor lockDescriptor,
            TimelockService timeLock,
            Refreshable<Duration> lockAcquisitionTimeout) {
        return new Lockable<>(inner, lockDescriptor, timeLock, lockAcquisitionTimeout);
    }

    // For code owners:
    // Do not use this for multiple locks!! If you're not careful, you can make it really easy to deadlock
    // If you end up using this class and have a need for batch locks, implement a separate method that uses
    // Timelocks batch locking methods and ensure there's a consistent ordering when taking out local locks
    // to avoid circular waiting.
    //    @MustBeClosed TODO: How do I get this working?? I'll track looking into this for another time.
    // Would be good to have an IfPresentMustBeClosed annotation if it exists
    @CheckReturnValue
    public Optional<LockedItem<T>> tryLock(Consumer<Lockable<T>> disposeCallback) {
        if (isLocked.compareAndSet(false, true)) {
            // We do not want the timeout to be too low to avoid a race condition where we give up too soon
            LockRequest request = LockRequest.of(
                    ImmutableSet.of(lockDescriptor),
                    lockAcquisitionTimeout.get().toMillis());

            try {
                return timelockService
                        .lock(request)
                        .getTokenOrEmpty()
                        .map(lockToken -> new LockedItem<>(inner, this, () -> unlock(lockToken, disposeCallback)));
            } catch (Exception e) {
                log.warn("Failed to acquire lock", UnsafeArg.of("lockDescriptor", lockDescriptor), e);
                isLocked.set(false);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private void unlock(LockToken lockToken, Consumer<Lockable<T>> disposeCallback) {
        try {
            timelockService.unlock(ImmutableSet.of(lockToken));
        } finally {
            isLocked.set(false); // We end up holding the local lock ever so slightly longer than necessary.
            disposeCallback.accept(this);
        }
    }

    @Override
    public String toString() {
        return "Lockable{inner=" + inner + ", isLocked=" + isLocked + "}";
    }

    // Expected to be used in a try closeable.
    public static final class LockedItem<T> implements AutoCloseable {
        private final T item;

        // We need to capture the outer Lockable<T> as a strong reference so that LockableFactory doesn't GC it away
        // while we're using the inner value. While the dispose callback does hold a reference, we're making it
        // more explicit by actually holding outer.
        private final Lockable<T> outer;
        private final Runnable disposeCallback;

        private LockedItem(T item, Lockable<T> outer, Runnable disposeCallback) {
            this.item = item;
            this.disposeCallback = disposeCallback;
            this.outer = outer;
        }

        public T getItem() {
            return item;
        }

        @Override
        public void close() {
            disposeCallback.run();
        }
    }

    public static class LockableComparator<T> implements Comparator<Lockable<T>> {
        private final Comparator<T> comparator;

        public LockableComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Lockable<T> o1, Lockable<T> o2) {
            return comparator.compare(o1.inner, o2.inner);
        }
    }
}
