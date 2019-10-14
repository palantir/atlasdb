/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.AbstractInvocationHandler;

/**
 * This proxy exists to support "fast failover" behaviour with no limit as to the number of attempts made; instead,
 * we use a time limit. This exists to support perpetual {@code 308} responses from servers, which may happen if
 * leader election is still taking place.
 */
public class FastFailoverProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(FastFailoverProxy.class);
    private static final Duration TIME_LIMIT = Duration.ofSeconds(10);

    private final T delegate;
    private final Clock clock;

    @VisibleForTesting
    FastFailoverProxy(T delegate, Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
    }

    public static <U> U newProxyInstance(Class<U> interfaceClass, U delegate) {
        FastFailoverProxy<U> proxy = new FastFailoverProxy<>(delegate, Clock.systemUTC());

        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                proxy);
    }


    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
    }
}
