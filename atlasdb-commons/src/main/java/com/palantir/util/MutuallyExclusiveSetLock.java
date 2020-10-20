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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class accepts a collection of <code>Comparable</code> objects, creates locks
 * for each object, and then locks each object in order.
 * This can be used to provide exclusive access at a per-object level instead of with broad locking.
 *
 * <p><code>MutuallyExclusiveSetLock</code> locks the objects in the set in their natural
 * order, or in the order determined by the <code>Comparator</code> you pass as a parameter.
 *
 * <p>If you have an <code>equals()</code> method that is not consistent with your
 * <code>compareTo()</code> method, then you will most likely get a deadlock.
 *
 * <p>This class assumes that the thread that is doing the locking is doing the work.
 * If this thread blocks on another thread that tries to use this class, then deadlock may ensue.
 *
 * <p>Use a try-finally to unlock the classes and do not spawn
 * threads or tasks that you block on that might run on other threads.
 *
 * @author carrino
 */
public class MutuallyExclusiveSetLock<T> {
    private final boolean fair;
    private final Comparator<? super T> comparator;
    private final Set<Thread> threadSet = Sets.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
    private final LoadingCache<T, ReentrantLock> syncMap = CacheBuilder.newBuilder()
            .weakValues()
            .build(new CacheLoader<T, ReentrantLock>() {
                @Override
                public ReentrantLock load(T key) {
                    return new ReentrantLock(fair);
                }
            });

    /**
     * Constructs a new <code>MutuallyExclusiveSetLock</code>
     * with the fairness bit set to <code>false</code>.
     * When locks are under contention, no access order
     * is guaranteed.
     * @see #MutuallyExclusiveSetLock(boolean)
     * @deprecated use factory method {@link #create(boolean)}
     */
    @Deprecated
    public MutuallyExclusiveSetLock() {
        this(false);
    }

    /**
     * Constructs a new <code>MutuallyExclusiveSetLock</code>.
     * @param fair when <code>true</code>, the class favors granting access to the
     *             longest-waiting thread when there is any contention.
     *             When <code>false</code>, no access order is guaranteed.
     * @deprecated use factory method {@link #create(boolean)}
     */
    @Deprecated
    public MutuallyExclusiveSetLock(boolean fair) {
        this(fair, null);
    }

    /**
     * Constructs a new <code>MutuallyExclusiveSetLock</code> that will
     * lock the objects in the order determined by <code>comparator</code>.
     * @param fair when <code>true</code>, the class favors granting access to the
     *             longest-waiting thread when there is any contention.
     *             When <code>false</code>, no access order is guaranteed.
     * @param comparator a <code>java.util.Comparator</code> to use in determining lock order.
     * @deprecated use factory method {@link #createWithComparator(boolean, Comparator)}
     */
    @Deprecated
    public MutuallyExclusiveSetLock(boolean fair, Comparator<? super T> comparator) {
        this.fair = fair;
        this.comparator = comparator;
    }

    public static <T extends Comparable<? super T>> MutuallyExclusiveSetLock<T> create(boolean fair) {
        return new MutuallyExclusiveSetLock<T>(fair);
    }

    /**
     * Constructs a new <code>MutuallyExclusiveSetLock</code> that will
     * lock the objects in the order determined by <code>comparator</code>.
     * @param fair when <code>true</code>, the class favors granting access to the
     *             longest-waiting thread when there is any contention.
     *             When <code>false</code>, no access order is guaranteed.
     * @param comparator a <code>java.util.Comparator</code> to use in determining lock order.
     */
    public static <T> MutuallyExclusiveSetLock<T> createWithComparator(boolean fair, Comparator<? super T> comparator) {
        return new MutuallyExclusiveSetLock<T>(fair, comparator);
    }

    /**
     * Returns <code>true</code> if all the items are locked on the current
     * thread.
     *
     * @param items collection of items, not null
     */
    public boolean isLocked(Iterable<T> items) {
        for (T t : items) {
            ReentrantLock lock = syncMap.getUnchecked(t);
            if (!lock.isHeldByCurrentThread()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to acquire the locks in increasing order and may block.
     *
     * <p>Be sure that the <code>Comparator&lt;T&gt;</code> or <code>T.compareTo()</code>
     * is consistent with <code>T.equals()</code>. You can only lock once on a thread
     * with a set of objects. If you wish to lock on more objects,
     * you must unlock then pass the new set of objects to be locked.
     * @return <code>LockState</code> instance with the information required to
     *         unlock these same objects.
     * @see #unlock(LockState)
     */
    public LockState<T> lockOnObjects(Iterable<T> lockObjects) {
        ImmutableSet<T> hashSet = validateLockInput(lockObjects);

        final SortedMap<T, ReentrantLock> sortedLocks = getSortedLocks(hashSet);
        for (ReentrantLock lock : sortedLocks.values()) {
            lock.lock();
        }
        threadSet.add(Thread.currentThread());
        return new LockState<T>(sortedLocks.values(), this);
    }

    public LockState<T> lockOnObjectsInterruptibly(Iterable<T> lockObjects) throws InterruptedException {
        ImmutableSet<T> hashSet = validateLockInput(lockObjects);

        List<ReentrantLock> toUnlock = new ArrayList<>();
        try {
            final SortedMap<T, ReentrantLock> sortedLocks = getSortedLocks(hashSet);
            for (ReentrantLock lock : sortedLocks.values()) {
                lock.lockInterruptibly();
                toUnlock.add(lock);
            }
            LockState<T> ret = new LockState<T>(sortedLocks.values(), this);
            threadSet.add(Thread.currentThread());
            toUnlock.clear();
            return ret;
        } finally {
            for (ReentrantLock reentrantLock : toUnlock) {
                reentrantLock.unlock();
            }
        }
    }

    private ImmutableSet<T> validateLockInput(Iterable<T> lockObjects) {
        if (lockObjects == null) {
            throw new SafeIllegalArgumentException("lockObjects is null");
        }

        if (threadSet.contains(Thread.currentThread())) {
            throw new SafeIllegalStateException("You must not synchronize twice in the same thread");
        }

        ImmutableSet<T> hashSet = ImmutableSet.copyOf(lockObjects);
        if (comparator == null) {
            for (T t : hashSet) {
                if (!(t instanceof Comparable)) {
                    throw new SafeIllegalArgumentException(
                            "you must either specify a comparator or pass in comparable objects");
                }
            }
        }

        // verify that the compareTo and equals are consistent in that we are always locking on all objects
        SortedSet<T> treeSet = new TreeSet<T>(comparator);
        treeSet.addAll(hashSet);
        if (treeSet.size() != hashSet.size()) {
            throw new SafeIllegalArgumentException("The number of elements using .equals and compareTo differ. "
                    + "This means that compareTo and equals are not consistent "
                    + "which will cause some objects to not be locked");
        }
        return hashSet;
    }

    /**
     * Unlocks the objects acquired from locking.
     * This method should always be in a finally block immediately after the lock.
     * If you try to unlock from another thread, no objects are unlocked.
     * @param lockState object that was returned by the
     *        <code>lockOnObjects()</code> method when you locked the objects
     * @see #lockOnObjects(Iterable)
     * @deprecated use {@link LockState#unlock()}
     */
    @Deprecated
    public void unlock(LockState<T> lockState) {
        if (lockState == null) {
            throw new SafeIllegalArgumentException("lockState is null");
        }

        if (lockState.setLock != this) {
            throw new SafeIllegalArgumentException("The lockState passed was not from this instance");
        }

        if (lockState.thread != Thread.currentThread()) {
            throw new SafeIllegalArgumentException(
                    "The thread that created this lockState is not the same as the one unlocking it");
        }

        threadSet.remove(Thread.currentThread());
        for (ReentrantLock lock : lockState.locks) {
            lock.unlock();
        }
    }

    private SortedMap<T, ReentrantLock> getSortedLocks(Collection<T> lockObjects) {
        final TreeMap<T, ReentrantLock> sortedLocks = new TreeMap<T, ReentrantLock>(comparator);
        for (T t : lockObjects) {
            sortedLocks.put(t, syncMap.getUnchecked(t));
        }
        return sortedLocks;
    }

    /**
     * An instance of this class is returned by the
     * <code>MutuallyExclusiveSetLock.lockOnObjects()</code>
     * method. You need this object to use as a parameter to that class's
     * <code>unlock()</code> method.
     */
    public static class LockState<K> {
        final ImmutableSet<ReentrantLock> locks;
        final MutuallyExclusiveSetLock<K> setLock;
        final Thread thread;

        LockState(Collection<ReentrantLock> locks, MutuallyExclusiveSetLock<K> setLock) {
            this.locks = ImmutableSet.copyOf(locks);
            this.setLock = setLock;
            thread = Thread.currentThread();
        }

        /**
         * Unlocks the objects acquired from locking.
         * This method should always be in a try/finally block immediately after the lock.
         * If you try to unlock from another thread, no objects are unlocked.
         * @see #lockOnObjects(Iterable)
         */
        public void unlock() {
            setLock.unlock(this);
        }
    }
}
