/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Like a {@link ReadWriteLock}, but ensures memory visibility between readers and writers.
 */
public class VisibleReadWriteLock {
    private final ReadWriteLock readWriteLock;
    private volatile int counter = 0;
    private int enforcer = 1;

    public VisibleReadWriteLock(ReadWriteLock readWriteLock) {
        this.readWriteLock = readWriteLock;
    }

    public void readLock() {
        readWriteLock.readLock().lock();
    }

    public void readUnlock() {
        readWriteLock.readLock().unlock();
    }

    public void writeLock() {
        readWriteLock.writeLock().lock();
    }

    public void writeUnlock() {
        readWriteLock.writeLock().unlock();
    }
}
