package com.palantir.nexus.db.sql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.ResourceCreationLocation;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.nexus.streaming.PTInputStream;
import com.palantir.sql.PreparedStatements;
import com.palantir.sql.ResultSets;
import com.palantir.util.streams.PTStreams;

import oracle.jdbc.OracleConnection;
import oracle.sql.BLOB;

/**
 * Routines for issuing SQL statements to the database.
 * <p>
 * All the methods deprecated in this class have alternatives in {@link PalantirSqlConnection}
 */
public abstract class SQL extends BasicSQL {
    private static final Logger sqlExceptionlog = LoggerFactory.getLogger("sqlException." + SQL.class.getName()); //$NON-NLS-1$

    /** key for the sql query to list tables with a given column. */
    static final String LIST_TABLES_WITH_COLUMN = "SQL_LIST_TABLES_WITH_COLUMN"; //$NON-NLS-1$
    static {
        SQLString.registerQuery(LIST_TABLES_WITH_COLUMN,
                " /* ORACLE_COLUMN_QUERY */ " + //$NON-NLS-1$
                " SELECT DISTINCT table_name FROM user_tab_columns " + //$NON-NLS-1$
                "   WHERE (lower(table_name) LIKE lower('PT_%'))" + //$NON-NLS-1$
                "   AND lower(column_name) = ?", //$NON-NLS-1$
                DBType.ORACLE);
        SQLString.registerQuery(LIST_TABLES_WITH_COLUMN,
                " /* POSTGRES_COLUMN_QUERY */" + //$NON-NLS-1$
                " SELECT tablename AS table_name FROM pg_tables " + //$NON-NLS-1$
                "  WHERE tablename !~* 'pg_*' AND tablename LIKE 'pt\\_%' " +  //$NON-NLS-1$
                " AND lower(?) IN " + //$NON-NLS-1$
                " (SELECT lower(a.attname) AS field " + //$NON-NLS-1$
                " FROM pg_class c, pg_attribute a, pg_type t " + //$NON-NLS-1$
                " WHERE c.relname = tablename " + //$NON-NLS-1$
                " AND a.attnum > 0 " + //$NON-NLS-1$
                " AND a.attrelid = c.oid " + //$NON-NLS-1$
                " AND a.atttypid = t.oid)" + //$NON-NLS-1$
                "ORDER BY table_name", //$NON-NLS-1$
                DBType.POSTGRESQL);
    }

    public static final int POSTGRES_BLOB_WRITE_LIMIT = 1000 * 1000 * 1000; // Postgres can store 1G

    public static final int POSTGRES_BLOB_READ_LIMIT = 100 * 1000 * 1000; // Postgres doesn't like to read more than ~2^28 bytes at once, so limit to something smaller

    @Override
    protected BlobCleanup setObject(Connection c, PreparedStatement ps, int i, Object obj) {
        if (obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            PTInputStream is = new PTInputStream(new ByteArrayInputStream(bytes), bytes.length);
            return handlePtInputStream(c, ps, i, is);
        } else if (obj instanceof PTInputStream) {
            return handlePtInputStream(c, ps, i, (PTInputStream) obj);
        } else if (obj instanceof OracleStructArray) {
            setOracleStructArray(c, ps, i, (OracleStructArray)obj);
            return null;
        } else {
            return super.setObject(c, ps, i, obj);
        }
    }

    // QA-5818: there is a lower-limit(!) on stream sizes for the Oracle
    // JDBC driver.  We check for the limit and, if it's below, we read
    // the whole stream into a byte[] and set the object directly.
    private static final int ORACLE_BYTE_LOWER_LIMIT = 2000; // QA-70384
    private BlobCleanup handlePtInputStream(Connection c, PreparedStatement ps, int i, PTInputStream is) {
        if (is.getLength() <= ORACLE_BYTE_LOWER_LIMIT) {
            try {
                byte[] bytes = IOUtils.toByteArray(is, is.getLength());
                Preconditions.checkArgument(bytes.length == is.getLength(),
                        "incorrect length - bytes: %s, input stream: %s", //$NON-NLS-1$
                        bytes.length, is.getLength());
                PreparedStatements.setBytes(ps, i, bytes);
            } catch (IOException e) {
                throw Throwables.chain(PalantirSqlException.createForChaining(), Throwables.chain(new SQLException("Internal IOException"), e)); //$NON-NLS-1$
            } finally {
                IOUtils.closeQuietly(is);
            }
            return null;
        } else if (is.getLength() <= Integer.MAX_VALUE) {
            if (DBType.getTypeFromConnection(c) == DBType.POSTGRESQL
                    && is.getLength() > SQL.POSTGRES_BLOB_WRITE_LIMIT) {
                Validate.isTrue(false, "Postgres only supports blobs up to 1G"); //$NON-NLS-1$
            }
            PreparedStatements.setBinaryStream(ps, i, is, (int)is.getLength());
            return null;
        } else {
            DBType dbType = DBType.getTypeFromConnection(c);
            Validate.isTrue(dbType == DBType.ORACLE, "We only support blobs over 2GB on oracle (postgres only supports blobs up to 1G)"); //$NON-NLS-1$
            BLOB blob;
            try {
                blob = BLOB.createTemporary(getNativeOracleConnection(c),
                        false, BLOB.DURATION_SESSION);
            } catch (SQLException e){
                sqlExceptionlog.info("Caught SQLException", e); //$NON-NLS-1$
                throw PalantirSqlException.create(e);
            }
            BlobCleanup ret = new BlobCleanup(blob);
            OutputStream os = null;
            try {
                os = blob.setBinaryStream(0);
                PTStreams.copy(is, os);
                os.close();
                ps.setBlob(i, blob);
            } catch (Exception e) {
                ret.dispose(); // dispose early if we aren't returning correctly
                throw Throwables.chain(PalantirSqlException.createForChaining(), Throwables.chain(new SQLException("failed to transfer blob over 2GB to the DB"), e)); //$NON-NLS-1$
            } finally {
                IOUtils.closeQuietly(os);
            }
            return ret;
        }
    }

    private void setOracleStructArray(Connection c,
                                      PreparedStatement ps,
                                      int paramIndex,
                                      OracleStructArray array) {
        Preconditions.checkArgument(DBType.getTypeFromConnection(c) == DBType.ORACLE);
        try {
            c = getNativeOracleConnection(c);
            PreparedStatements.setObject(ps, paramIndex, array.toOracleArray(c));
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static OracleConnection getNativeOracleConnection(Connection c) throws PalantirSqlException {
        OracleConnection rawconn = null;

        try {
            if (c instanceof OracleConnection) {
                rawconn = (OracleConnection)c;
            } else if (c.isWrapperFor(OracleConnection.class)) {
                rawconn = c.unwrap(OracleConnection.class);
            }
        } catch (SQLException e){
            sqlExceptionlog.info("Caught SQLException", e); //$NON-NLS-1$
            throw PalantirSqlException.create(e);
        }

        return rawconn;
    }

    public AgnosticLightResultSet fromResultSet(PreparedStatement preparedStatement,
                                                ResultSet resultSet,
                                                DBType dbType,
                                                String queryString) {
        FinalSQLString sqlString = SQLString.getUnregisteredQuery(queryString);
        String timingModule = "visitResultSet";
        ResultSetMetaData metaData = ResultSets.getMetaData(resultSet);
        SqlTimer sqlTimer = getSqlTimer();
        ResourceCreationLocation creationException = new ResourceCreationLocation("This is where the ResultsSet was created"); //$NON-NLS-1$
        return new AgnosticLightResultSetImpl(
                resultSet,
                dbType,
                metaData,
                preparedStatement,
                timingModule,
                sqlString,
                sqlTimer,
                creationException);
    }
}
