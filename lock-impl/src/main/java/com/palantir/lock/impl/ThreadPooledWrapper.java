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

import com.palantir.common.base.FunctionCheckedException;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPooledWrapper<F> {
    private static final Logger log = LoggerFactory.getLogger(ThreadPooledWrapper.class);

    private final Semaphore localThreadPool;
    private final Semaphore sharedThreadPool;
    private final F delegate;

    public ThreadPooledWrapper(F delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
        this.sharedThreadPool = sharedThreadPool;
        this.localThreadPool = new Semaphore(localThreadPoolSize);
        this.delegate = delegate;
    }

    public F delegate() {
        return delegate;
    }

    public <T, K extends Exception> T applyWithPermit(FunctionCheckedException<F, T, K> function) throws K {
        if (localThreadPool.tryAcquire()) {
            return applyAndRelease(localThreadPool, function);
        }
        if (sharedThreadPool.tryAcquire()) {
            return applyAndRelease(sharedThreadPool, function);
        }
        throw new TooManyRequestsException(
                "ThreadPooledLockService was unable to acquire a permit to assign a server thread to the request.");
    }

    private <T, K extends Exception> T applyAndRelease(Semaphore semaphore, FunctionCheckedException<F, T, K> function)
            throws K {
        try {
            return function.apply(delegate);
        } finally {
            semaphore.release();
        }
    }
}
