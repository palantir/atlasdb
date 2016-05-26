package com.palantir.nexus.db.pool;

import com.palantir.exception.PalantirSqlException;

/**
 * Exception type thrown by the {@link DBMgr} when it is misconfigured.
 */
public class DBMgrConfigurationException extends PalantirSqlException {
    private static final long serialVersionUID = 1L;

    public DBMgrConfigurationException(String msg) {
        super(PalantirSqlException.DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES, msg);
    }

    public DBMgrConfigurationException(String msg, Throwable cause) {
        super(PalantirSqlException.DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES, msg);
        initCause(cause);
    }
}
