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

public class HikariClientPoolConnectionManagerView implements ConnectionManager {
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
    public Connection getConnectionUnchecked() {
        try {
            if (semaphore.tryAcquire(timeout, TimeUnit.SECONDS)) {
                Connection connection;
                try {
                    connection = sharedManager.getConnectionUnchecked();
                } catch (RuntimeException e) {
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
        // ignore
    }

    @Override
    public void closeUnchecked() {
        // ignore
    }

    @Override
    public boolean isClosed() {
        return sharedManager.isClosed();
    }

    @Override
    public void init() throws SQLException {
        // no
    }

    @Override
    public void initUnchecked() {
        // no
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
