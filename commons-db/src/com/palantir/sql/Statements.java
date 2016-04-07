package com.palantir.sql;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;

public class Statements {
    private static final Logger sqlExceptionlog = Logger.getLogger("sqlException." + Statements.class.getName());
    public static boolean execute(Statement s, String sql) throws PalantirSqlException {
        try {
            return s.execute(sql);
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }


    public static void close(Statement s) throws PalantirSqlException {
        try {
            s.close();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static int getUpdateCount(Statement s) throws PalantirSqlException {
        try {
            return s.getUpdateCount();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
