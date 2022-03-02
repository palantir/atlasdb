/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This is an implementation {@link ConnectionManager} that delegates to a shared underlying
 * {@link HikariCPConnectionManager}, but only allows a fixed number of connections to be given out at any given time.
 * Closing the borrowed connection allows for borrowing a new connection.
 * Since the underlying connection manager is shared, the {@link #close()} and {@link #closeUnchecked()} methods are
 * no-ops.
 */
public class HikariClientPoolConnectionManagerView extends BaseConnectionManager {
    private final HikariCPConnectionManager sharedManager;
    private final Semaphore semaphore;
    private final int timeout;

    public HikariClientPoolConnectionManagerView(HikariCPConnectionManager sharedManager, int poolSize, int timeout) {
        this.sharedManager = sharedManager;
        this.semaphore = new Semaphore(poolSize);
        this.timeout = timeout;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            if (semaphore.tryAcquire(timeout, TimeUnit.SECONDS)) {
                Connection connection;
                try {
                    connection = sharedManager.getConnection();
                } catch (SQLException | RuntimeException e) {
                    semaphore.release();
                    throw e;
                }
                return ConnectionWithCallback.wrap(connection, semaphore::release);
            } else {
                throw new SafeRuntimeException("timed out waiting to acquire connection");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SafeRuntimeException("interrupted waiting for a shared connection", e);
        }
    }

    @Override
    public void close() throws SQLException {
        // do not close shared manager
    }

    @Override
    public boolean isClosed() {
        return sharedManager.isClosed();
    }

    @Override
    public void init() throws SQLException {
        sharedManager.init();
    }

    @Override
    public DBType getDbType() {
        return sharedManager.getDbType();
    }

    @Override
    public void setPassword(String newPassword) {
        sharedManager.setPassword(newPassword);
    }
}
