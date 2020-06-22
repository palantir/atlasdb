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

import com.codahale.metrics.Counter;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.watch.LockWatchEventCache;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ResilientLockWatchEventCache extends AbstractInvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(ResilientLockWatchEventCache.class);

    static LockWatchEventCache newProxyInstance(
            LockWatchEventCache defaultCache,
            LockWatchEventCache fallbackCache,
            MetricsManager metricsManager) {
        return (LockWatchEventCache) Proxy.newProxyInstance(
                LockWatchEventCache.class.getClassLoader(),
                new Class<?>[] {LockWatchEventCache.class},
                new ResilientLockWatchEventCache(defaultCache, fallbackCache, metricsManager));
    }

    private final LockWatchEventCache fallbackCache;
    private final Counter fallbackCacheSelectedCounter;

    @GuardedBy("this")
    private LockWatchEventCache delegate;

    private ResilientLockWatchEventCache(LockWatchEventCache defaultCache, LockWatchEventCache fallbackCache,
            MetricsManager metricsManager) {
        this.delegate = defaultCache;
        this.fallbackCache = fallbackCache;
        this.fallbackCacheSelectedCounter =
                metricsManager.registerOrGetCounter(ResilientLockWatchEventCache.class, "fallbackCacheSelectedCounter");
    }

    @Override
    protected synchronized Object handleInvocation(Object proxy, Method method, Object[] args) {
        try {
            return method.invoke(delegate, args);
        } catch (TransactionLockWatchFailedException e) {
            throw e;
        } catch (Throwable t) {
            log.warn("Unexpected failure occurred when trying to use the default cache. Switching to the fallback "
                    + "implementation", t);
            fallbackCacheSelectedCounter.inc();
            delegate = fallbackCache;
            throw new TransactionLockWatchFailedException("Unexpected failure in the lock watch cache", t);
        }
    }
}
