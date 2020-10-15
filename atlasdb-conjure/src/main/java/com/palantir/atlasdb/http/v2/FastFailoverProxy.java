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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.base.Throwables;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import org.immutables.value.Value;

/**
 * This proxy exists to support "fast failover" behaviour with no limit as to the number of attempts made; instead,
 * we use a time limit. This exists to support perpetual {@code 308} responses from servers, which may happen if
 * leader election is still taking place.
 */
public final class FastFailoverProxy<T> extends AbstractInvocationHandler {
    private static final Duration TIME_LIMIT = Duration.ofSeconds(10);

    private final Supplier<T> delegate;
    private final Clock clock;

    private FastFailoverProxy(Supplier<T> delegate, Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
    }

    public static <U> U newProxyInstance(Class<U> interfaceClass, Supplier<U> delegate) {
        return newProxyInstance(interfaceClass, delegate, Clock.systemUTC());
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked") // Class guaranteed to be correct
    static <U> U newProxyInstance(Class<U> interfaceClass, Supplier<U> delegate, Clock clock) {
        FastFailoverProxy<U> proxy = new FastFailoverProxy<>(delegate, clock);

        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        Instant lastRetryInstant = clock.instant().plus(TIME_LIMIT);
        ResultOrThrowable attempt = singleInvocation(method, args);
        while (clock.instant().isBefore(lastRetryInstant) && !attempt.isSuccessful()) {
            Throwable cause = attempt.throwable().get();
            ResultOrThrowable fastFailoverCheck = isRetriable(cause);
            if (!fastFailoverCheck.isSuccessful()) {
                throw fastFailoverCheck.throwable().get();
            }
            attempt = singleInvocation(method, args);
        }
        if (attempt.isSuccessful()) {
            return attempt.result().orElse(null);
        }
        throw Throwables.unwrapIfPossible(attempt.throwable().get());
    }

    private ResultOrThrowable singleInvocation(Method method, Object[] args) {
        try {
            return ResultOrThrowable.success(method.invoke(delegate.get(), args));
        } catch (Throwable th) {
            return ResultOrThrowable.failure(th);
        }
    }

    private ResultOrThrowable isRetriable(Throwable throwable) {
        if (!(throwable instanceof InvocationTargetException)) {
            return ResultOrThrowable.failure(throwable);
        }
        InvocationTargetException exception = (InvocationTargetException) throwable;
        Throwable cause = exception.getCause();
        if (!isCausedByRetryOther(cause)) {
            return ResultOrThrowable.failure(cause);
        }
        return ResultOrThrowable.success(null);
    }

    @VisibleForTesting
    static boolean isCausedByRetryOther(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof QosException.RetryOther) {
                return true;
            }
            if (cause instanceof UnknownRemoteException && ((UnknownRemoteException) cause).getStatus() == 308) {
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
        Optional<Throwable> throwable();

        @Value.Check
        default void exactlyOneSet() {
            Preconditions.checkState(result().isPresent() ^ throwable().isPresent()
                    || isSuccessful() && !throwable().isPresent());
        }

        static ResultOrThrowable success(Object result) {
            return ImmutableResultOrThrowable.builder()
                    .isSuccessful(true)
                    .result(Optional.ofNullable(result))
                    .build();
        }

        static ResultOrThrowable failure(Throwable throwable) {
            return ImmutableResultOrThrowable.builder()
                    .isSuccessful(false)
                    .throwable(throwable)
                    .build();
        }
    }
}
