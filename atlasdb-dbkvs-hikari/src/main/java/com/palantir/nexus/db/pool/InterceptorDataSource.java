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
import com.google.common.reflect.AbstractInvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import javax.sql.DataSource;

/**
 * Allows you to intercept and override methods in {@link DataSource}.
 * <p/>
 * We only care about {@link DataSource#getConnection()}().
 * Its partner, getConnection(String username, String password), is never used since we pre-configure the data sources.
 * <p/>
 * We use this to run SQL against a connection to prepare it for use before placing it in a pool.
 * (i.e., create temporary tables on PostgreSQL, etc.)
 * <p/>
 * See {@link HikariCPConnectionManager#init} to see this in action.
 */
public abstract class InterceptorDataSource {
    private final AbstractInvocationHandler handler;

    protected InterceptorDataSource(final DataSource delegate) {
        this.handler = new AbstractInvocationHandler() {
            @Override
            protected Object handleInvocation(Object proxy, Method method, Object[] args)
                    throws Throwable {
                Object ret;
                try {
                    ret = method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
                if (method.getName().equals("getConnection")) {
                    Connection conn = (Connection) ret;
                    conn = InterceptorConnection.wrapInterceptor(conn);
                    onAcquire(conn);
                    ret = conn;
                }
                return ret;

            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass()).add("delegate", delegate).toString();
            }
        };
    }

    protected abstract void onAcquire(Connection conn);

    public static DataSource wrapInterceptor(InterceptorDataSource instance) {
        return (DataSource) Proxy.newProxyInstance(
                instance.getClass().getClassLoader(),
                new Class[]{DataSource.class},
                instance.handler);
    }
}
