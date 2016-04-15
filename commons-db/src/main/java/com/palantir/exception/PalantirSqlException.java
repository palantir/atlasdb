package com.palantir.exception;

import java.sql.SQLException;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.palantir.common.exception.PalantirRuntimeException;

/**
 * SQLExceptions are checked. However, generally speaking, we just want to propagate them.
 * Having a whole bunch of 'throws' and 'catch throws' is ugly & unnecessary.
 *
 */
public class PalantirSqlException extends PalantirRuntimeException {
    private static final long serialVersionUID = 1L;
    public static enum DO_NOT_SET_INITIAL_SQL_EXCEPTION { YES};
    public static enum SET_INITIAL_SQL_EXCEPTION {YES};

    /**
     * @deprecated Do not use! This should only be used by Throwables.rewrap which
     * constructs new exceptions via reflection and relies on constructors with
     * particular signatures being present.
     */
    @Deprecated
    public PalantirSqlException(String message, Throwable t) {
        super(message, t);

        debugLostSqlExceptionData();
    }

    protected PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION i) {
        super();

        debugLostSqlExceptionData();
    }

    protected PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION i, String msg) {
        super(msg);

        debugLostSqlExceptionData();
    }

    /**
     * This is not safe to use with Throwables.chain()
     */
    protected PalantirSqlException(SET_INITIAL_SQL_EXCEPTION i) {
        super(new SQLException());

        debugLostSqlExceptionData();
    }

    protected PalantirSqlException(SET_INITIAL_SQL_EXCEPTION i, String msg) {
        super(msg, new SQLException(msg));

        debugLostSqlExceptionData();
    }

    protected PalantirSqlException(String msg, SQLException n) {
        super(msg, n);

        debugLostSqlExceptionData();
    }

    public static PalantirSqlException create() {
        return new PalantirSqlException(SET_INITIAL_SQL_EXCEPTION.YES);
    }

    public static PalantirSqlException create(String msg) {
        return new PalantirSqlException(SET_INITIAL_SQL_EXCEPTION.YES, msg);
    }

    public static PalantirSqlException create(SQLException e) {
        // the detail message string of a throwable can be null.
        return new PalantirSqlException(ExceptionUtils.getMessage(e), e);
    }

    public static PalantirSqlException createForChaining() {
        return new PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES);
    }

    public static PalantirSqlException createForChaining(String msg) {
        return new PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES, msg);
    }

    private void debugLostSqlExceptionData() {
        if(System.getProperty("user.name").contains("bamboo")) {
            printStackTrace(); // (authorized)
        }
    }
}

