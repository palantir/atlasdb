/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.Suppliers;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.PalantirInterruptedException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Proxy that calls the requested method in another thread waits on a Future.
 * If the calling thread is interrupted, this proxy will throw a PalantirInterruptedException.
 * If given the CancelDelgate#Cancel option, it will also interrupt the delegated thread.
 *
 * @author dcohen
 */
@SuppressWarnings("ProxyNonConstantType")
public final class InterruptibleProxy implements DelegatingInvocationHandler {
    private static final Supplier<ExecutorService> defaultExecutor =
            Suppliers.memoize(() -> PTExecutors.newCachedThreadPool("Interruptible Proxy"));

    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, CancelDelegate cancel) {
        return newProxyInstance(interfaceClass, delegate, cancel, defaultExecutor.get());
    }

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(
            Class<T> interfaceClass, T delegate, CancelDelegate cancel, ExecutorService executor) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] {interfaceClass},
                new InterruptibleProxy(delegate, cancel, executor));
    }

    private final Object delegate;
    private final CancelDelegate cancel;
    private final ExecutorService executor;

    private InterruptibleProxy(Object delegate, CancelDelegate cancel, ExecutorService executor) {
        this.delegate = delegate;
        this.cancel = cancel;
        this.executor = executor;
    }

    @Override
    public Object invoke(final Object _proxy, final Method method, final Object[] args) throws Throwable {
        Future<Object> future = executor.submit(() -> {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                Throwables.rewrapAndThrowIfInstance(e.getCause(), Exception.class);
                Throwables.rewrapAndThrowIfInstance(e.getCause(), Error.class);
                throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        });
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw Throwables.rewrap(e.getCause());
        } catch (InterruptedException e) {
            throw new PalantirInterruptedException(e);
        } finally {
            future.cancel(cancel.equals(CancelDelegate.CANCEL));
        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }
}
