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
package com.palantir.nexus.db.pool;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.AbstractInvocationHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Allows you to intercept and override methods in {@link Connection}.
 */
public final class InterceptorConnection extends AbstractInvocationHandler implements InvocationHandler {
    private final Connection delegate;

    private static final ImmutableMap<String, Class<? extends Statement>> INTERCEPT_METHODS =
            ImmutableMap.<String, Class<? extends Statement>>builder()
                    .put("createStatement", Statement.class)
                    .put("prepareCall", CallableStatement.class)
                    .put("prepareStatement", PreparedStatement.class)
                    .build();

    private InterceptorConnection(final Connection delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        Object ret;
        try {
            ret = method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }

        Class<? extends Statement> wrapperClazz = INTERCEPT_METHODS.get(method.getName());
        if (wrapperClazz != null) {
            ret = wrap(wrapperClazz, ret);
        }

        return ret;
    }

    private static <T extends Statement> Object wrap(Class<T> clazz, Object ret) {
        return InterceptorStatement.wrapInterceptor(clazz.cast(ret), clazz);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass()).add("delegate", delegate).toString();
    }

    public static Connection wrapInterceptor(Connection delegate) {
        InterceptorConnection instance = new InterceptorConnection(delegate);
        return ConnectionWrapperProxy.create(instance);
    }
}
