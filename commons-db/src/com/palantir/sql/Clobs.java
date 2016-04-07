package com.palantir.sql;

import java.sql.Clob;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;

public class Clobs {
    private static final Logger sqlExceptionlog = Logger.getLogger("sqlException." + Clobs.class.getName());

    public static String getString(Clob clob, long clobLen) throws PalantirSqlException {
        try {
        if (clob == null || clobLen == 0L) {
            return ""; //$NON-NLS-1$
        }

        StringBuilder rv = new StringBuilder();
        long pos=1;
        if (clobLen < 0) {
            // then we have to ask Oracle
            clobLen = clob.length();
        }

        while (pos <= clobLen) {
            int readLen = 32000;
            if ((pos+readLen-1) > clobLen) {
                readLen = (int)(clobLen - pos + 1);
            }

            rv.append(clob.getSubString(pos, readLen));

            pos += readLen;
        }

        return rv.toString();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static String getString(Clob clob, int clobLength) throws PalantirSqlException {
        return getString(clob, (long) clobLength);
    }
}
