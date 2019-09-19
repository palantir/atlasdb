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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows you to intercept and override methods in {@link Statement} and subinterfaces
 * methods.
 */
public final class InterceptorStatement<T extends Statement> extends AbstractInvocationHandler
        implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final T delegate;

    private InterceptorStatement(final T delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            if (method.getName().startsWith("execute")) {
                try {
                    handleExecException(e);
                } catch (Exception ex) {
                    // We want to preserve the original exception, so let's log and discard here.
                    log.debug("Unexpected exception handling unexpected exception from Statement execution.  "
                            + "This exception will be logged and discarded.", ex);
                }
            }
            throw e;
        }
    }

    /**
     * Off the rails... if we get an exception back that isn't a SQLException, we're going to assume
     * that it's not going to be handled properly in the rest of the application. In addition,
     * chances are it's from a bug in an underlying layer. To reduce the chance of us later
     * poisoning the pool with a bad connection, let's just close it.
     * <p/>
     * This will end poorly for whatever this just got kicked out from underneath it.
     */
    private void handleExecException(Exception execException) throws SQLException {
        // TODO (bullman): This is terrible. There has to be a better way.
        log.debug("Handling Exception from Statement method.", execException);
        Connection conn = delegate.getConnection().unwrap(Connection.class);

        if (conn.isClosed()) {
            // Closed connections are automatically returned to the pool.
            return;
        }

        try {
            // How a JDBC driver handles closing a connection with an active connection is
            // an implementation-specific detail. Don't assume that closing the driver will
            // roll back the transaction - some of them commit.
            log.debug("Issuing rollback on connection.");
            conn.rollback();
        } catch (SQLException sqe) {
            log.error("Transaction roll back threw exception.", sqe);
        } finally {
            conn.close();
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass()).add("delegate", delegate).toString();
    }

    @SuppressWarnings("unchecked")
    public static <T extends Statement> T wrapInterceptor(final T delegate,
                                                          final Class<? extends Statement> clazz) {
        InterceptorStatement<T> instance = new InterceptorStatement<T>(delegate);
        return (T) Proxy.newProxyInstance(
                instance.getClass().getClassLoader(),
                new Class[]{clazz},
                instance);
    }
}
