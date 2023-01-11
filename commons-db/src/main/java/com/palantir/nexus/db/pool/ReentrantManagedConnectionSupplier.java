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

import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.ReentrantConnectionSupplier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class ReentrantManagedConnectionSupplier implements ReentrantConnectionSupplier {

    private final ConnectionManager delegate;
    private final ThreadLocal<ResourceSharer<Connection, SQLException>> threadLocal;
    private final BooleanSupplier isCloseTrackingEnabled;

    private ReentrantManagedConnectionSupplier(
            ConnectionManager delegate,
            ThreadLocal<ResourceSharer<Connection, SQLException>> threadLocal,
            BooleanSupplier isCloseTrackingEnabled) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
        this.threadLocal = Preconditions.checkNotNull(threadLocal, "threadLocal");
        this.isCloseTrackingEnabled = Preconditions.checkNotNull(isCloseTrackingEnabled, "isCloseTrackingEnabled");
    }

    public static ReentrantManagedConnectionSupplier createForTesting(ConnectionManager connectionManager) {
        return new ReentrantManagedConnectionSupplier(
                connectionManager, createThreadLocal(connectionManager), () -> true);
    }

    public static ReentrantManagedConnectionSupplier create(
            ConnectionManager delegate, BooleanSupplier isCloseTrackingEnabled) {
        return new ReentrantManagedConnectionSupplier(delegate, createThreadLocal(delegate), isCloseTrackingEnabled);
    }

    private static ThreadLocal<ResourceSharer<Connection, SQLException>> createThreadLocal(ConnectionManager delegate) {
        //noinspection Convert2Diamond - <> causes compiler failure https://bugs.openjdk.org/browse/JDK-8246111
        return ThreadLocal.withInitial(() -> new ResourceSharer<Connection, SQLException>(ResourceTypes.CONNECTION) {
            @Override
            public Connection open() {
                return delegate.getConnectionUnchecked();
            }
        });
    }

    @Override
    public Connection get() throws PalantirSqlException {
        Connection connection = threadLocal.get().get();
        if (isCloseTrackingEnabled.getAsBoolean()) {
            return CloseTracking.wrap(connection);
        }
        return connection;
    }

    @Override
    public Connection getUnsharedConnection() throws PalantirSqlException {
        Connection connection = delegate.getConnectionUnchecked();
        if (isCloseTrackingEnabled.getAsBoolean()) {
            return CloseTracking.wrap(connection);
        }
        return connection;
    }

    @Override
    public void close() throws PalantirSqlException {
        delegate.closeUnchecked();
    }
}
