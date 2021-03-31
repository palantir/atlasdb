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
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.base.Throwables;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.util.function.Supplier;

/**
 * This proxy exists to retry in case of a SocketTimeoutException (something that isn't retried natively by Dialogue).
 */
public final class RetryOnSocketTimeoutExceptionProxy<T> extends AbstractInvocationHandler {
    private static final int MAX_NUM_RETRIES = 5;
    private final Supplier<T> delegate;

    private RetryOnSocketTimeoutExceptionProxy(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    public static <U> U newProxyInstance(Class<U> interfaceClass, Supplier<U> delegate) {
        RetryOnSocketTimeoutExceptionProxy<U> proxy = new RetryOnSocketTimeoutExceptionProxy<>(delegate);

        return (U) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass}, proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        ResultOrThrowable attempt = singleInvocation(method, args);
        int numRetries = 0;
        while ((numRetries < MAX_NUM_RETRIES) && !attempt.isSuccessful()) {
            numRetries++;
            Throwable cause = attempt.throwable().get();
            ResultOrThrowable retriableExceptionCheck = isRetriable(cause);
            if (!retriableExceptionCheck.isSuccessful()) {
                throw retriableExceptionCheck.throwable().get();
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
        if (isCausedBySocketTimeoutException(cause)) {
            return ResultOrThrowable.success(null);
        } else {
            return ResultOrThrowable.failure(cause);
        }
    }

    @VisibleForTesting
    static boolean isCausedBySocketTimeoutException(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof SocketTimeoutException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
