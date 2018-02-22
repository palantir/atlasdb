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

package com.palantir.async.initializer;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Callback {
    public static final Callback NO_OP = new NoOp();

    private volatile boolean shutdownSignal = false;
    private Lock lock = new ReentrantLock();

    /**
     * The method to be executed. If init() returns, the callback is considered to be successful.
     */
    public abstract void init();

    /**
     * Cleanup to be done if init() throws, before init() can be attempted again.
     * @param initException Exception thrown by init()
     */
    public abstract void cleanup(Exception initException);

    /**
     * Keep retrying init(), performing any necessary cleanup, until it succeeds unless cleanup() throws or a shutdown
     * signal has been sent.
     */
    public void runWithRetry() {
        while (!shutdownSignal) {
            try {
                lock.lock();
                if (!shutdownSignal) {
                    init();
                }
                return;
            } catch (Exception e) {
                cleanup(e);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Send a shutdown signal and block until potential cleanup has finished running.
     */
    public void blockUntilSafeToShutdown() {
        shutdownSignal = true;
        lock.lock();
        lock.unlock();
    }

    private static class NoOp extends Callback {
        @Override
        public void init() {
        }

        @Override
        public void cleanup(Exception initException) {
        }
    }
}
