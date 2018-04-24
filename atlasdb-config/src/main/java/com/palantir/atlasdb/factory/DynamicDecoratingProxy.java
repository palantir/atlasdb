/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.exception.NotInitializedException;

public final class DynamicDecoratingProxy<T> extends AbstractInvocationHandler {
    private final T decoratedService;
    private final T defaultService;
    private final Supplier<Boolean> shouldDecorate;

    private static final Logger log = LoggerFactory.getLogger(DynamicDecoratingProxy.class);

    private DynamicDecoratingProxy(T decoratedService, T defaultService, Supplier<Boolean> shouldDecorate) {
        this.decoratedService = decoratedService;
        this.defaultService = defaultService;
        this.shouldDecorate = shouldDecorate;
    }

    public static <T> T newProxyInstance(
            T decoratedService, T defaultService, Supplier<Boolean> shouldDecorate, Class<T> clazz) {
        DynamicDecoratingProxy<T> service =
                new DynamicDecoratingProxy<>(decoratedService, defaultService, shouldDecorate);
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[] { clazz },
                service);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        Object target = shouldDecorate.get() ? decoratedService : defaultService;
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof NotInitializedException) {
                log.warn("Resource is not initialized yet!");
            }
            throw e.getTargetException();
        }
    }
}
