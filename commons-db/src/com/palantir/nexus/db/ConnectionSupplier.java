package com.palantir.nexus.db;

import java.io.Closeable;
import java.sql.Connection;

import com.google.common.base.Supplier;
import com.palantir.exception.PalantirSqlException;

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
     * @see java.io.Closeable#close()
     * @throws PalantirSqlException if an error occurs while closing
     */
    @Override
    public void close() throws PalantirSqlException;

}
