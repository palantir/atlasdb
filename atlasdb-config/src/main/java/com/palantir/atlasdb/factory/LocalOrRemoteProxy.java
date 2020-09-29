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
package com.palantir.atlasdb.factory;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A proxy that chooses between using a local or remote proxy based on a task that may take a while to complete.
 */
final class LocalOrRemoteProxy<T> extends AbstractInvocationHandler {

    static <U> U newProxyInstance(
            Class<U> interfaceClass,
            U localService,
            U remoteService,
            Future<Boolean> useLocalServiceFuture) {
        @SuppressWarnings("unchecked")
        U proxy = (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new LocalOrRemoteProxy<>(localService, remoteService, useLocalServiceFuture));
        return proxy;
    }

    private T localService;
    private T remoteService;
    private Future<Boolean> useLocalServiceFuture;

    private LocalOrRemoteProxy(T localService, T remoteService, Future<Boolean> useLocalServiceFuture) {
        this.localService = localService;
        this.remoteService = remoteService;
        this.useLocalServiceFuture = useLocalServiceFuture;
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                // do not clear the interrupt flag
                throw new SafeIllegalStateException("interrupted");
            }
            try {
                return method.invoke(delegate(), args);
            } catch (InvocationTargetException e) {
                Throwable targetException = e.getTargetException();
                if (targetException instanceof NotCurrentLeaderException) {
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    continue;
                }
                throw targetException;
            }
        }
    }

    private T delegate() {
        try {
            if (useLocalServiceFuture.get(0, TimeUnit.SECONDS)) {
                return localService;
            } else {
                return remoteService;
            }
        } catch (TimeoutException e) {
            // Use the remote service while we're still determining if it's safe to use the local service.
            return remoteService;
        } catch (InterruptedException e) {
            // Use the remote service while we're still determining if it's safe to use the local service.
            Thread.currentThread().interrupt();
            return remoteService;
        } catch (ExecutionException e) {
            // Using the remote service is always safe; fall back to it if anything went wrong
            return remoteService;
        }
    }
}
