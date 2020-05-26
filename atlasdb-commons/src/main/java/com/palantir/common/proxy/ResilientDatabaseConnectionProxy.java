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

package com.palantir.common.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

public class ResilientDatabaseConnectionProxy extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ResilientDatabaseConnectionProxy.class);

    private final Supplier<Connection> connectionFactory;
    private volatile Connection currentConnection;

    public ResilientDatabaseConnectionProxy(Supplier<Connection> connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.currentConnection = connectionFactory.get();
    }

    public static Connection newProxyInstance(Supplier<Connection> connectionFactory) {
        ResilientDatabaseConnectionProxy service = new ResilientDatabaseConnectionProxy(connectionFactory);
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[] { Connection.class },
                service);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close") && args.length == 0) {
            currentConnection.close(); // Otherwise, consider the impact of closing the proxy two times
            return null;
        }

        return runOperationExpectingLiveConnection(connection -> {
            try {
                return method.invoke(connection, args);
            } catch (IllegalAccessException e) {
                log.error("Illegal access when making a database connection! This suggests an AtlasDb side"
                        + " class-loading problem. Please contact support", e);
                throw Throwables.rewrapAndThrowUncheckedException(e);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof SQLException) {
                    throw (SQLException) e.getTargetException();
                }
                throw Throwables.rewrapAndThrowUncheckedException(e.getTargetException());
            }
        });
    }

    private <T> T runOperationExpectingLiveConnection(DatabaseConnectionOperation<T> operation) throws SQLException {
        int maxAttempts = 10;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Connection connection = currentConnection;
            if (isCurrentConnectionLive(connection)) {
                // we can live with the race condition here
                return operation.apply(connection);
            }
            // the connection is known to be non-live
            maybeCreateNewConnection(connection);
        }
        throw new SafeRuntimeException("Attempted a database operation repeatedly and failed",
                SafeArg.of("numAttempts", maxAttempts));
    }

    private synchronized void maybeCreateNewConnection(Connection knownDeadConnection) {
        if (currentConnection == knownDeadConnection) {
            currentConnection = connectionFactory.get();
        }
    }

    private static boolean isCurrentConnectionLive(Connection connection) {
        try {
            return !connection.isClosed();
        } catch (SQLException e) {
            log.warn("Encountered an SQL exception", e);
            return false;
        }
    }

    private interface DatabaseConnectionOperation<T> {
        T apply(Connection connection) throws SQLException;
    }
}
