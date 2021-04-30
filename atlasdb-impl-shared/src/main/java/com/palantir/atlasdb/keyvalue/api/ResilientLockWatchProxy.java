/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import com.codahale.metrics.Counter;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public final class ResilientLockWatchProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ResilientLockWatchProxy.class);

    public static LockWatchEventCache newEventCacheProxy(
            LockWatchEventCache defaultCache, LockWatchEventCache fallbackCache, MetricsManager metricsManager) {
        return (LockWatchEventCache) Proxy.newProxyInstance(
                LockWatchEventCache.class.getClassLoader(),
                new Class<?>[] {LockWatchEventCache.class},
                new ResilientLockWatchProxy<>(defaultCache, fallbackCache, metricsManager));
    }

    public static LockWatchValueScopingCache newValueCacheProxy(
            LockWatchValueScopingCache defaultCache,
            LockWatchValueScopingCache fallbackCache,
            MetricsManager metricsManager) {
        return (LockWatchValueScopingCache) Proxy.newProxyInstance(
                LockWatchValueScopingCache.class.getClassLoader(),
                new Class<?>[] {LockWatchValueScopingCache.class},
                new ResilientLockWatchProxy<>(defaultCache, fallbackCache, metricsManager));
    }

    private final T fallbackCache;
    private final Counter fallbackCacheSelectedCounter;

    private volatile T delegate;

    private ResilientLockWatchProxy(T defaultCache, T fallbackCache, MetricsManager metricsManager) {
        this.delegate = defaultCache;
        this.fallbackCache = fallbackCache;
        this.fallbackCacheSelectedCounter =
                metricsManager.registerOrGetCounter(ResilientLockWatchProxy.class, "fallbackCacheSelectedCounter");
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws IllegalAccessException {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw handleException(e);
        }
    }

    private synchronized RuntimeException handleException(InvocationTargetException rethrow) {
        try {
            throw rethrow.getCause();
        } catch (TransactionLockWatchFailedException e) {
            throw e;
        } catch (Throwable t) {
            if (delegate == fallbackCache) {
                throw new SafeRuntimeException("Fallback cache threw an exception", t);
            } else {
                log.warn(
                        "Unexpected failure occurred when trying to use the default cache. "
                                + "Switching to the fallback implementation",
                        t);
                fallbackCacheSelectedCounter.inc();
                delegate = fallbackCache;
                throw new TransactionLockWatchFailedException("Unexpected failure in the default lock watch cache", t);
            }
        }
    }
}
