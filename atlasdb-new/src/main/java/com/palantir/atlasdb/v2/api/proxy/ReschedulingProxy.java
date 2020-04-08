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

package com.palantir.atlasdb.v2.api.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public final class ReschedulingProxy implements InvocationHandler {
    private final Executor reschedulingExecutor;
    private final Object delegate;

    private ReschedulingProxy(Executor reschedulingExecutor, Object delegate) {
        this.reschedulingExecutor = reschedulingExecutor;
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!ListenableFuture.class.equals(method.getReturnType())) {
            try {
                method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
        try {
            return Futures.transform((ListenableFuture) method.invoke(delegate, args),
                    x -> x,
                    reschedulingExecutor);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Executor executor, Class<T> iface, T instance) {
        return (T) Proxy.newProxyInstance(
                iface.getClassLoader(), new Class[] {iface}, new ReschedulingProxy(executor, instance));
    }
}
