/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.leader.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.Validate;

import com.palantir.common.base.Throwables;
import com.palantir.common.proxy.DelegatingInvocationHandler;

public final class ToggleableExceptionProxy implements DelegatingInvocationHandler {

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass,
                                         T delegate,
                                         AtomicBoolean throwException,
                                         Exception exception) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new ToggleableExceptionProxy(delegate, throwException, exception));
    }

    final Object delegate;
    final AtomicBoolean throwException;
    final Exception exception;

    private ToggleableExceptionProxy(Object delegate,
                                     AtomicBoolean throwException,
                                     Exception exception) {
        Validate.notNull(delegate, "delegate should not be null.");
        Validate.notNull(throwException, "thrownException should not be null.");
        Validate.notNull(exception, "exception should not be null.");
        this.delegate = delegate;
        this.throwException = throwException;
        this.exception = exception;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!isJavaLangObjectMethod(method) && throwException.get()) {
            throw Throwables.rewrap(exception);
        }
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private boolean isJavaLangObjectMethod(Method method) {
        return Arrays.asList(Object.class.getMethods()).contains(method);
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }

}
