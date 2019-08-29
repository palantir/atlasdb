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

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.NotCurrentLeaderException;

import feign.RetryableException;

public class RetryOtherRetryingProxy<T> extends AbstractInvocationHandler {
    private static final Duration MAXIMUM_RETRY_DURATION = Duration.ofSeconds(10);

    private final T delegate;
    private final Clock clock;

    private RetryOtherRetryingProxy(T delegate, Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
    }

    static <U> U newProxyInstance(Class<U> interfaceClass,
            U delegate,
            Clock clock) {
        RetryOtherRetryingProxy proxy = new RetryOtherRetryingProxy(delegate, clock);
        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        Instant deadline = clock.instant().plus(MAXIMUM_RETRY_DURATION);
        while (clock.instant().compareTo(deadline) < 0) {
            try {
                Object response = method.invoke(delegate, args);
                return response;
            } catch (InvocationTargetException e) {
                if (!isRetryOtherException(e.getTargetException())) {
                    throw e.getTargetException();
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }
        throw new NotCurrentLeaderException("Could not find a leader after " + MAXIMUM_RETRY_DURATION);
    }

    // TODO (jkong): Evil hackery
    private static boolean isRetryOtherException(Throwable t) {
        if (t instanceof RetryableException) {
            return t.getMessage().contains("Exceeded the maximum number of allowed redirects");
        }
        return false;
    }
}
