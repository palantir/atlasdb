package com.palantir.nexus.db.pool;

import java.sql.Connection;
import java.sql.SQLException;

import com.palantir.nexus.db.DBType;

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