/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.transaction.service;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;

/**
 * This class holds a reference to an object of type V that can be atomically swapped.
 * Swapping the reference returns a lock that is released once all functions currently
 * running against the old value of the reference have completed.
 */
public class LockableReference<V> {
    public static class SwapResult<V> {
        private final Lock lock;
        private final V value;

        private SwapResult(Lock lock, V value) {
            this.lock = lock;
            this.value = value;
        }

        public Lock getLock() {
            return lock;
        }

        public V getValue() {
            return value;
        }
    }

    private final ReadWriteLock swapLock;
    private final AtomicReference<V> valueRef;
    private final AtomicReference<ReadWriteLock> valueLockRef;

    public LockableReference(V value) {
        this.swapLock = new ReentrantReadWriteLock();
        this.valueRef = new AtomicReference<V>(value);
        this.valueLockRef = new AtomicReference<ReadWriteLock>(new ReentrantReadWriteLock());
    }

    // Returns a lock that is released once all functions currently running against
    // the old value of the reference have completed.
    public SwapResult<V> swap(V newValue) {
        swapLock.writeLock().lock();
        try {
            ReadWriteLock oldLock = valueLockRef.get();
            V oldValue = valueRef.get();
            valueLockRef.set(new ReentrantReadWriteLock());
            valueRef.set(newValue);
            return new SwapResult<V>(oldLock.writeLock(), oldValue);
        } finally {
            swapLock.writeLock().unlock();
        }
    }

    // The function in the argument is run in the same thread.
    public <E> E runAgainstCurrentValue(Function<V, E> function) {
        V currentValue;
        Lock currentLock;
        swapLock.readLock().lock();
        try {
            currentValue = valueRef.get();
            currentLock = valueLockRef.get().readLock();
            currentLock.lock();
        } finally {
            swapLock.readLock().unlock();
        }

        try {
            return function.apply(currentValue);
        } finally {
            currentLock.unlock();
        }
    }

    public V getValue() {
        return valueRef.get();
    }
}
