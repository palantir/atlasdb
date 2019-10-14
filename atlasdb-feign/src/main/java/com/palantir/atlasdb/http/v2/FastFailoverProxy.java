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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.conjure.java.api.errors.QosException;

import feign.RetryableException;

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
        ResultOrThrowable attempt = singleInvocation(method, args);
        Instant lastRetryInstant = clock.instant().plus(TIME_LIMIT);
        while (clock.instant().isBefore(lastRetryInstant) && !attempt.isSuccessful()) {
            Throwable cause = attempt.failure().get();
            if (cause instanceof InvocationTargetException) {
                Throwable ex = ((InvocationTargetException) cause).getTargetException();
                if (!(ex instanceof RetryableException) || !isCausedByRetryOther((RetryableException) ex) ) {
                    throw ex;
                }
            }
            attempt = singleInvocation(method, args);
        }
        if (attempt.isSuccessful()) {
            return attempt.result().orElse(null);
        }
        throw attempt.failure().get();
    }

    private ResultOrThrowable singleInvocation(Method method, Object[] args) {
        try {
            return ResultOrThrowable.success(method.invoke(delegate, args));
        } catch (Throwable th) {
            return ResultOrThrowable.exception(th);
        }
    }

    private static boolean isCausedByRetryOther(RetryableException ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof QosException.RetryOther) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Value.Immutable
    interface ResultOrThrowable {
        boolean isSuccessful();
        Optional<Object> result();
        Optional<Throwable> failure();

        @Value.Check
        default void exactlyOneSet() {
            Preconditions.checkState(result().isPresent() ^ failure().isPresent()
                    || isSuccessful() && !failure().isPresent());
        }

        static ResultOrThrowable success(Object result) {
            return ImmutableResultOrThrowable.builder()
                    .isSuccessful(true)
                    .result(result == null ? Optional.empty() : Optional.of(result))
                    .build();
        }

        static ResultOrThrowable exception(Throwable throwable) {
            return ImmutableResultOrThrowable.builder().isSuccessful(false).failure(throwable).build();
        }
    }
}
