package com.palantir.nexus.db.sql;

import java.sql.Connection;

import com.palantir.exception.PalantirSqlException;

public interface ConnectionBackedSqlConnection extends PalantirSqlConnection {
    public void close() throws PalantirSqlException;

    @Override
    public Connection getUnderlyingConnection();
}
