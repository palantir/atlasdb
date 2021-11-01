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
package com.palantir.common.proxy;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

@SuppressWarnings("ProxyNonConstantType")
public final class PredicateSwitchedProxy<T> extends AbstractInvocationHandler {
    private final T firstService;
    private final T secondService;
    private final Supplier<Boolean> shouldUseFirstService;

    private static final SafeLogger log = SafeLoggerFactory.get(PredicateSwitchedProxy.class);

    private PredicateSwitchedProxy(T firstService, T secondService, Supplier<Boolean> shouldUseFirstService) {
        this.firstService = firstService;
        this.secondService = secondService;
        this.shouldUseFirstService = shouldUseFirstService;
    }

    public static <T> T newProxyInstance(
            T firstService, T secondService, Supplier<Boolean> shouldUseFirstService, Class<T> clazz) {
        PredicateSwitchedProxy<T> service =
                new PredicateSwitchedProxy<>(firstService, secondService, shouldUseFirstService);
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz}, service);
    }

    @Override
    protected Object handleInvocation(Object _proxy, Method method, Object[] args) throws Throwable {
        Object target = shouldUseFirstService.get() ? firstService : secondService;
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof NotInitializedException) {
                log.warn("Resource is not initialized yet!", e.getCause());
            }
            throw e.getCause();
        }
    }
}
