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

package com.palantir.common.proxy;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.reflect.AbstractInvocationHandler;

/**
 * Proxy that creates a new underlying instance on the first request after a specified amount of time has elapsed on
 * the existing instance. Once the refresh interval has expired, a new instance is not guaranteed to be created until
 * a method is invoked on the proxy.
 */
public class SelfRefreshingProxy<T> extends AbstractInvocationHandler {
    private static final Duration REFRESH_INTERVAL = Duration.ofMinutes(30);

    private final Supplier<T> proxySupplier;

    private SelfRefreshingProxy(Supplier<T> proxy) {
        this.proxySupplier = proxy;
    }

    public static <T> T create(Supplier<T> proxyFactory, Class<T> clazz) {
        return create(proxyFactory, clazz, REFRESH_INTERVAL);
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    static <T> T create(Supplier<T> proxyFactory, Class<T> clazz, Duration refreshInterval) {
        SelfRefreshingProxy<T> proxy = new SelfRefreshingProxy<>(Suppliers.memoizeWithExpiration(
                proxyFactory::get, refreshInterval.toNanos(), TimeUnit.NANOSECONDS));
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[] {clazz},
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(proxySupplier.get(), args);
    }
}
