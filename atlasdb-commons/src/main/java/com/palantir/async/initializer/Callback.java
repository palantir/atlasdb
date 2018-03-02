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

import com.palantir.common.base.Throwables;

public abstract class Callback<R> {
    private volatile boolean shutdownSignal = false;
    private Lock lock = new ReentrantLock();

    /**
     * The method to be executed. If init() returns, the callback is considered to be successful.
     */
    public abstract void init(R resource);

    /**
     * Cleanup to be done if init() throws, before init() can be attempted again. If this method throws, runWithRetry()
     * will fail and init() will not be retried. The default implementation assumes that init() throwing is terminal
     * and there is no cleanup necessary. This should be overridden by specifying any cleanup steps necessary, and the
     * method must return instead of throwing if init() can be retried
     *
     * @param initException Throwable thrown by init()
     */
    public void cleanup(R resource, Throwable initException) {
        Throwables.rewrapAndThrowUncheckedException(initException);
    }

    /**
     * Keep retrying init(), performing any necessary cleanup, until it succeeds unless cleanup() throws or a shutdown
     * signal has been sent.
     */
    public void runWithRetry(R resource) {
        while (!shutdownSignal) {
            try {
                lock.lock();
                if (!shutdownSignal) {
                    init(resource);
                }
                return;
            } catch (Throwable e) {
                cleanup(resource, e);
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

    public static class NoOp<R> extends Callback<R> {
        @Override
        public void init(R resource) {
        }

        @Override
        public void cleanup(R resource, Throwable initException) {
        }
    }
}
