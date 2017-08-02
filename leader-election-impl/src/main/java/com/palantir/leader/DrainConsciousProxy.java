/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.leader;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class DrainConsciousProxy<T> extends AbstractInvocationHandler {
    private final T delegate;
    private volatile AtomicBoolean draining;
    private final AtomicLong remainingOperations;

    private CompletableFuture<Void> drainFuture;

    public DrainConsciousProxy(T delegate, Class<T> clazz) {
        this.delegate = delegate;
        draining = new AtomicBoolean(false);
        remainingOperations = new AtomicLong(0);
    }

    public static <U> U newProxyInstance(Class<U> interfaceClass,
            U delegate) {
        DrainConsciousProxy<U> proxy = new DrainConsciousProxy<>(
                delegate,
                interfaceClass);

        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] {interfaceClass, Closeable.class, Drainable.class},
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (draining.get()) {
            return new NotCurrentLeaderException("draining!");
        }
        if (method.getName().equals("drain") && args.length == 0) {
            if (delegate instanceof Closeable) {
                ((Closeable) delegate).close();
            }
            if (delegate instanceof Drainable) {
                ((Drainable) delegate).drain();
            }
            if (draining.compareAndSet(false, true)) {
                drainFuture = new CompletableFuture<>();
            }
            return drainFuture;
        }

        remainingOperations.incrementAndGet();
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof ServiceNotAvailableException) {
                throw new NotCurrentLeaderException("service unavailable");
            } else {
                throw Throwables.propagate(e.getCause());
            }
        } finally {
            long remaining = remainingOperations.decrementAndGet();
            if (draining.get() && remaining == 0) {
                drainFuture.complete(null);
            }
        }
    }
}
