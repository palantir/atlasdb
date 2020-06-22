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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.nexus.db.sql.SqlConnection;
import java.sql.SQLException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionSupplier implements SqlConnectionSupplier {
    private static final Logger log = LoggerFactory.getLogger(ConnectionSupplier.class);
    private volatile SqlConnection sharedConnection;
    private final Supplier<SqlConnection> delegate;

    public ConnectionSupplier(Supplier<SqlConnection> delegate) {
        this.delegate = delegate;
    }

    @Override
    public SqlConnection get() {
        if (sharedConnection == null) {
            sharedConnection = getFresh();
        }
        return sharedConnection;
    }

    /**
     * Returns fresh PalantirSqlConnection. It is the responsibility of the consumer of this method
     * to close the returned connection when done.
     */
    public SqlConnection getFresh() {
        close();
        return delegate.get();
    }

    @Override
    public synchronized void close() {
        if (sharedConnection != null) {
            try {
                sharedConnection.getUnderlyingConnection().close();
            } catch (SQLException e) {
                log.error("Error occurred closing the underlying connection", e);
            } finally {
                sharedConnection = null;
            }
        }
    }
}
