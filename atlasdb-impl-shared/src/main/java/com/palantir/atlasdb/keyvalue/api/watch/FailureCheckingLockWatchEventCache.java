/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.watch.LockWatchEventCache;

final class FailureCheckingLockWatchEventCache extends AbstractInvocationHandler {

    static LockWatchEventCache newProxyInstance(LockWatchEventCache defaultCache, LockWatchEventCache fallbackCache) {
        return (LockWatchEventCache) Proxy.newProxyInstance(
                LockWatchEventCache.class.getClassLoader(),
                new Class<?>[] {LockWatchEventCache.class},
                new FailureCheckingLockWatchEventCache(defaultCache, fallbackCache));
    }

    private final LockWatchEventCache fallbackCache;

    @GuardedBy("this")
    private LockWatchEventCache delegate;

    private FailureCheckingLockWatchEventCache(LockWatchEventCache defaultCache, LockWatchEventCache fallbackCache) {
        this.delegate = defaultCache;
        this.fallbackCache = fallbackCache;
    }

    @Override
    protected synchronized Object handleInvocation(Object proxy, Method method, Object[] args) {
        try {
            return method.invoke(delegate, args);
        } catch (TransactionLockWatchFailedException e) {
            throw e;
        } catch (Throwable t) {
            delegate = fallbackCache;
            throw new TransactionLockWatchFailedException("Unexpected failure in the lock watch cache", t);
        }
    }
}
