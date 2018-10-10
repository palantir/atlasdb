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

package com.palantir.timestamp.utils;

import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;


/**
 *  A test utilization class that shuffles the order of returned timestamps. It is meant to be used only for testing.
 */
public class ShufflingTimestampService implements TimestampService {
    private TimestampService delegate;
    private static int WINDOW_SIZE = 3;
    private Object signal = new Object();
    private volatile boolean growing = true;
    private Stack<Long> tsStack = new Stack<>();
    Lock lock = new ReentrantLock();

    public ShufflingTimestampService(TimestampService delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getFreshTimestamp() {
        try {
            putToStack();
            while (tsStack.size() < WINDOW_SIZE && growing) {
                synchronized (signal) {
                    signal.wait();
                }
            }
            lock.lock();
            return popFromStack();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestamps) {
        return delegate.getFreshTimestamps(numTimestamps);
    }

    private synchronized void putToStack() throws InterruptedException {
        long ts = delegate.getFreshTimestamp();
        while (!growing) {
            synchronized (signal) {
                signal.wait();
            }
        }
        tsStack.push(ts);
        if (tsStack.size() >= WINDOW_SIZE) {
            growing = false;
        }
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    private synchronized long popFromStack() throws InterruptedException {
        long valueToReturn = tsStack.pop();
        if (tsStack.isEmpty()) {
            growing = true;
        }
        synchronized (signal) {
            signal.notifyAll();
        }
        return valueToReturn;
    }
}
