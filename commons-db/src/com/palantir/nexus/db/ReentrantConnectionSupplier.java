package com.palantir.nexus.db;

import java.io.Closeable;
import java.sql.Connection;

import com.palantir.exception.PalantirSqlException;

public interface ReentrantConnectionSupplier extends ConnectionSupplier, Closeable {

    /**
     * Returns a shared {@link Connection} that can be shared within this thread. Callers must
     * {@link Connection#close() close()} the returned connection when they are finished with this
     * connection.
     *
     * @return a Connection
     * @throws PalantirSqlException if an error occurs getting the connection
     */
    @Override
    Connection get() throws PalantirSqlException;

    /**
     * Returns an unshared {@link Connection}. Callers must {@link Connection#close() close()} the
     * returned connection when they are finished with this connection.
     *
     * @return a Connection
     * @throws PalantirSqlException if an error occurs getting the connection
     */
    Connection getUnsharedConnection() throws PalantirSqlException;

    /**
     * Closes this connection supplier and releases any resources it may hold.
     *
     * @see java.io.Closeable#close()
     * @throws PalantirSqlException if an error occurs while closing
     */
    @Override
    public void close() throws PalantirSqlException;
}
