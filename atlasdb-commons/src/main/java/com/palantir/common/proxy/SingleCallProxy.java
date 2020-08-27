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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ensure that only a single method is ever called.  Throw {@link IllegalStateException} on all subsequent calls.
 */
public class SingleCallProxy implements DelegatingInvocationHandler {

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate) {
        return (T)Proxy.newProxyInstance(interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass}, new SingleCallProxy(delegate));
    }

    final private Object delegate;
    final private AtomicBoolean hasBeenCalled = new AtomicBoolean(false);

    private SingleCallProxy(Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!hasBeenCalled.compareAndSet(false, true)) {
            throw new SafeIllegalStateException("This class has already been called once before");
        }

        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }
}
