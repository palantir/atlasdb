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
package com.palantir.nexus.db;

import com.palantir.exception.PalantirSqlException;
import java.io.Closeable;
import java.sql.Connection;
import java.util.function.Supplier;

public interface ConnectionSupplier extends Supplier<Connection>, Closeable {

    /**
     * Retrieves a {@link Connection}. Callers must {@link Connection#close() close()} the returned
     * connection when they are finished with this connection.
     *
     * @return a Connection
     * @throws PalantirSqlException if an error occurs getting the connection
     */
    @Override
    Connection get() throws PalantirSqlException;

    /**
     * Closes this connection supplier and releases any resources it may hold.
     *
     * @see Closeable#close()
     * @throws PalantirSqlException if an error occurs while closing
     */
    @Override
    public void close() throws PalantirSqlException;

}
