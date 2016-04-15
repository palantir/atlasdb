package com.palantir.sql;

import java.sql.Blob;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;

import oracle.sql.BLOB;

public class Blobs {
    private final static Logger sqlExceptionlog = Logger.getLogger("sqlException." + Blobs.class.getName());

    public static long length(Blob b) throws PalantirSqlException {
        try {
            return b.length();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static byte[] getBytes(Blob b, long pos, int length) throws PalantirSqlException {
        try {
            return b.getBytes(pos, length);
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static boolean isTemporary(BLOB blob)  throws PalantirSqlException {
        try {
            return blob.isTemporary();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
