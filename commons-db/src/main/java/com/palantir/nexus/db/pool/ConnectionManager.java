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

import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A SQL connection pool that can be flushed.
 *
 * @author jweel
 */
public interface ConnectionManager {
    /**
     * Initializes the connection pool if necessary, then obtains a SQL connection.
     *
     * @return a {@link Connection}, possibly fresh, or possibly recycled
     */
    Connection getConnection() throws SQLException;

    Connection getConnectionUnchecked();

    /**
     * Shuts down the underlying connection pool.
     */
    void close() throws SQLException;

    void closeUnchecked();

    /**
     * Initializes the connection pool if necessary, and verifies that it does indeed work. Since
     * initialization is implicit in getConnection(), this is mostly useful to force an exception in
     * case the pool cannot be initialized.
     */
    void init() throws SQLException;

    void initUnchecked();

    DBType getDbType();
}
