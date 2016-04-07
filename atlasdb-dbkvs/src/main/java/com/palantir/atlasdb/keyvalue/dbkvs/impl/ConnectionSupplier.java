package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

public class ConnectionSupplier implements PalantirSqlConnectionSupplier {
    private static final Logger log = LoggerFactory.getLogger(ConnectionSupplier.class);
    private volatile PalantirSqlConnection sharedConnection;
    private final Supplier<PalantirSqlConnection> delegate;

    public ConnectionSupplier(Supplier<PalantirSqlConnection> delegate) {
        this.delegate = delegate;
    }

    @Override
    public PalantirSqlConnection get() {
        if (sharedConnection == null) {
            sharedConnection = getFresh();
        }
        return sharedConnection;
    }

    /**
     * Returns fresh PalantirSqlConnection. It is the responsibility of the consumer of this method
     * to close the returned connection when done.
     *
     * @return
     */
    public PalantirSqlConnection getFresh() {
        close();
        return delegate.get();
    }

    @Override
    public synchronized void close() {
        if (sharedConnection != null) {
            try {
                sharedConnection.getUnderlyingConnection().close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            } finally {
                sharedConnection = null;
            }
        }
    }
}
