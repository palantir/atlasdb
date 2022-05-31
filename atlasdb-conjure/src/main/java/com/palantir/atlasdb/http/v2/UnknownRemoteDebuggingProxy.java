/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http.v2;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings("ProxyNonConstantType")
public final class UnknownRemoteDebuggingProxy<T, V> extends AbstractInvocationHandler {
    private static final SafeLogger log = SafeLoggerFactory.get(UnknownRemoteDebuggingProxy.class);
    private final Refreshable<T> safeLoggableRefreshable;
    private final RateLimiter rateLimiter;
    private final V delegate;

    private UnknownRemoteDebuggingProxy(Refreshable<T> safeLoggableRefreshable, V delegate) {
        this.safeLoggableRefreshable = safeLoggableRefreshable;
        this.delegate = delegate;
        this.rateLimiter = RateLimiter.create(0.05);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (UnknownRemoteException e) {
            if (rateLimiter.tryAcquire()) {
                log.warn(
                        "Encountered UnknownRemoteException; logging current state of refreshable",
                        SafeArg.of("safeLoggableRefreshable", safeLoggableRefreshable.get()),
                        e);
            }
            throw e;
        }
    }

    public static <T, U> U newProxyInstance(
            Class<U> interfaceClass, Refreshable<T> safeLoggableRefreshable, U delegate) {
        UnknownRemoteDebuggingProxy<T, U> proxy = new UnknownRemoteDebuggingProxy<>(safeLoggableRefreshable, delegate);

        return (U) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass}, proxy);
    }
}
