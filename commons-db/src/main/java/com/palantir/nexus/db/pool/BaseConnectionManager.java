package com.palantir.nexus.db.pool;

import java.sql.Connection;
import java.sql.SQLException;

import com.palantir.exception.PalantirSqlException;

public abstract class BaseConnectionManager implements ConnectionManager {
    @Override
    public Connection getConnectionUnchecked() {
        try {
            return getConnection();
        } catch (SQLException e) {
            throw PalantirSqlException.create(e);
        }
    }

    @Override
    public void closeUnchecked() {
        try {
            close();
        } catch (SQLException e) {
            throw PalantirSqlException.create(e);
        }
    }

    @Override
    public void initUnchecked() {
        try {
            init();
        } catch (SQLException e) {
            throw PalantirSqlException.create(e);
        }
    }
}
