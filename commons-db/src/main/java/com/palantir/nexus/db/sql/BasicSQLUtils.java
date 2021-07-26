/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.sql;

import com.palantir.common.concurrent.ThreadNamingCallable;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.SQLConstants;
import com.palantir.nexus.db.ThreadConfinedProxy;
import com.palantir.nexus.db.sql.monitoring.logger.SqlLoggers;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;

public class BasicSQLUtils {

    /**
     * Returns count of fields in a field list.  Used for optimizing and
     * normalizing CRUD code.
     *
     * @return count of comma-delimited fields.
     */
    public static int countFields(String fieldList) {
        return fieldList.split("\\s*,\\s*").length; // $NON-NLS-1$
    }

    public static String nArguments(int n) {
        return StringUtils.repeat("?", ",", n); // $NON-NLS-1$ //$NON-NLS-2$
    }

    // convert ints to longs, to ensure that we do not overflow our bind variables.
    // private, so that clients can only pass in values that we know will not overflow
    public static String internalLimitQuery(
            String query, long maxRows, long offset, List<Object> varbinds, DBType dbType) {
        // our strategy is to create a subselect around the user-provided
        // query that limits on rows returned and offsets.  this isn't the
        // most efficient operation in most dbs, but it works...
        if (dbType == DBType.ORACLE) {
            if (offset == 0) { // limit only
                final String limitQuery = String.format(SQLConstants.SQL_ORACLE_LIMIT_QUERY, query);
                varbinds.add(maxRows + 1);
                return limitQuery;
            } else { // limit & offset (paging)
                final String limitOffsetQuery = String.format(SQLConstants.SQL_ORACLE_LIMIT_OFFSET_QUERY, query);
                varbinds.add(maxRows + offset + 1);
                varbinds.add(offset + 1);
                return limitOffsetQuery;
            }
        } else if (dbType == DBType.POSTGRESQL) {
            if (maxRows > 0) {
                final String limitOffsetQuery = String.format(SQL_POSTGRES_LIMIT_OFFSET_QUERY, query);
                varbinds.add(maxRows);
                varbinds.add(offset);
                return limitOffsetQuery;
            } else {
                SqlLoggers.LOGGER.warn("Passed a negative limit to SQL.limitQuery - ignoring limit"); // $NON-NLS-1$
                return query;
            }
        }
        Preconditions.checkState(
                false,
                "limitQuery() only supports POSTGRES, H2, HSQL, and ORACLE db type.",
                UnsafeArg.of("db", dbType));
        return query;
    }

    /**
     * POSTGRES limit and offset query syntax.
     */
    private static final String SQL_POSTGRES_LIMIT_OFFSET_QUERY = " %s  LIMIT ? OFFSET ?"; // $NON-NLS-1$

    /**
     * Alters the varbinds to work with limitQuery.  To use this, you must have created your
     * query with a limitQueryNoOffset call.  This decoupling of the query creating and the varbinds
     * allows for the registration of limit queries.
     *
     * @see SQL#formatLimitQuery(String)
     */
    public static void addVarbindsForLimitQuery(int maxRows, long offset, List<Object> varbinds, DBType dbType) {
        internalLimitQuery("", maxRows, offset, varbinds, dbType); // $NON-NLS-1$
    }

    /**
     * Alters the varbinds to work with limitQuery.  To use this, you must have created your
     * query with a limitQueryNoOffset call.  This decoupling of the query creating and the varbinds
     * allows for the registration of limit queries.
     *
     * @see SQL#formatLimitQuery(String)
     */
    public static void addVarbindsForLimitQuery(int maxRows, List<Object> varbinds, DBType dbType) {
        addVarbindsForLimitQuery(maxRows, 0, varbinds, dbType);
    }

    /**
     * Limits the given query to the given number of maximum rows at
     * the given offset.  This is effective when trying to page through
     * a set of results.  This method does not alter the query directly,
     * it simply wraps it in sub-selects.
     * <p>
     * Note: you cannot alter the query or varbinds after calling this method.
     *
     * @param query    - query to limit
     * @param maxRows  - maximum number of results to provide
     * @param offset   - 0-based offset to return results from
     * @param varbinds - the variable bindings for the given query
     */
    public static String limitQuery(String query, int maxRows, int offset, List<Object> varbinds, DBType dbType) {
        return internalLimitQuery(query, maxRows, offset, varbinds, dbType);
    }

    /**
     * Limits the given query to the given number of maximum rows.  This method
     * does not alter the query directly, it simply wraps it in sub-selects.
     * <p>
     * Note: you cannot alter the query or varbinds after calling this method.
     *
     * @param query    - query to limit
     * @param maxRows  - maximum number of results to provide
     * @param varbinds - the variable bindings for the given query
     */
    public static String limitQuery(String query, int maxRows, List<Object> varbinds, DBType dbType) {
        return limitQuery(query, maxRows, 0, varbinds, dbType);
    }

    /**
     * Creates a query string for a limit query, but does not set the varbinds.
     * To use this, you will have to pass your varbinds through a limitQuery call.
     * query with a limitQuery call.  This decoupling of the query creating and the varbinds
     * allows for the registration of limit queries.
     * <p>
     * This method has NoOffset in the name because it creates a limit query that
     * does not have an offset value.  The presence of an offset could change the
     * resulting String.
     *
     * @see addVarbindsForLimitQuery
     */
    public static String formatLimitQuery(String query, BasicSQL.OffsetInclusion offset, DBType dbType) {
        int fakeOffset;
        if (offset.equals(BasicSQL.OffsetInclusion.INCLUDE_OFFSET)) {
            // TODO (dcohen): comment why this fake offset causes the proper query to be created
            fakeOffset = 1;
        } else {
            fakeOffset = 0;
        }
        return internalLimitQuery(query, 1, fakeOffset, new ArrayList<Object>(), dbType);
    }

    /**
     * Gives you a single sequence increment text as appropriate for the
     * db you're using (e.g. "sequence.nextval as label" or "next value for sequence as label"
     * It will not have spaces around it!
     *
     * @param sequenceName Name of a valid sequence in your database
     * @param label        Name assigned to the "column" in the result set
     */
    public static String getSequenceColumnString(String sequenceName, String label, DBType type) {
        String sql;
        if (type == DBType.POSTGRESQL) {
            sql = String.format(POSTGRESQL_SINGLE_ID_COLUMN, sequenceName);
        } else {
            sql = String.format(ORACLE_SINGLE_ID_COLUMN, new Object[] {sequenceName});
        }
        if (label != null) {
            sql += " AS " + label; // $NON-NLS-1$
        }
        return sql;
    }

    private static final String ORACLE_SINGLE_ID_COLUMN = "%s.nextval"; // $NON-NLS-1$
    private static final String POSTGRESQL_SINGLE_ID_COLUMN = "nextval('%s')"; // $NON-NLS-1$

    public static String qualifyFields(String fieldList, String tableName) {
        String fields[] = fieldList.split("\\s*,\\s*"); // $NON-NLS-1$
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            out.append(tableName).append(".").append(fields[i]); // $NON-NLS-1$
            if (i < fields.length - 1) {
                out.append(","); // $NON-NLS-1$
            }
        }
        return out.toString();
    }

    @Nullable
    public static Boolean getNullableBoolean(AgnosticResultRow row, String colName) throws PalantirSqlException {
        Long ret = row.getLongObject(colName);
        if (ret == null) {
            return null;
        }
        if (ret == 0) {
            return false;
        }
        return true;
    }

    public static boolean getBoolean(AgnosticResultRow row, String colName) throws PalantirSqlException {
        long ret = row.getLong(colName);
        if (ret == 0) {
            return false;
        }
        return true;
    }

    public static final String COMMON_SYSTEM_FIELDS = "deleted, created_by, time_created, last_modified"; // $NON-NLS-1$

    /**
     * Builds update compatible field list from comma delimited field names
     *
     * @param fieldList - in form of "field1, field2,..."
     */
    public static String generateUpdateString(String fieldList) {
        String fields[] = fieldList.split("\\s*,\\s*"); // $NON-NLS-1$
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            out.append(fields[i]).append(" = ?"); // $NON-NLS-1$
            if (i < fields.length - 1) {
                out.append(","); // $NON-NLS-1$
            }
        }
        return out.toString();
    }

    private static final SafeLogger cancelLogger = SafeLoggerFactory.get("SQLUtils.cancel"); // $NON-NLS-1$

    /**
     * Helper method for wrapping quick calls that don't appreciate being interrupted.
     * Passes all exceptions and errors back to the client.
     * Runs in another thread - do not acquire connections from within the callable (it will fail).
     * N.B. (DCohen) Despite the claim in the previous line, there is code that acquires connections from within the callable successfully.
     * Unclear why this is bad.
     * <p>
     * Should only use low-level type stuff.
     * <p>
     * This method takes a connection, which the callable intends to use,
     * essentially declaring that the child thread now owns
     * this connection.
     */
    public static <T> T runUninterruptably(
            final Callable<T> callable, String threadString, final @Nullable Connection connection)
            throws PalantirSqlException {
        return runUninterruptably(BasicSQL.DEFAULT_EXECUTE_EXECUTOR.get(), callable, threadString, connection);
    }

    public static <T> T runUninterruptably(
            ExecutorService executorService,
            final Callable<T> callable,
            String threadString,
            final @Nullable Connection connection)
            throws PalantirSqlException {
        Future<T> future = executorService.submit(ThreadNamingCallable.wrapWithThreadName(
                ThreadConfinedProxy.threadLendingCallable(connection, () -> {
                    if (Thread.currentThread().isInterrupted()) {
                        cancelLogger.error("Threadpool thread has interrupt flag set!"); // $NON-NLS-1$
                        // we want to clear the interrupted status here -
                        // we cancel via the prepared statement, not interrupts
                        Thread.interrupted();
                    }
                    return callable.call();
                }),
                threadString,
                ThreadNamingCallable.Type.APPEND));

        boolean interrupted = false;
        T result = null;
        final String oldName = Thread.currentThread().getName();
        final String currentTimestamp = DateTimeFormat.forPattern("HH:mm:ss").print(System.currentTimeMillis());
        Thread.currentThread().setName(oldName + " blocking on " + threadString + " started at " + currentTimestamp);
        try {
            long startTime = System.currentTimeMillis();
            while (true) {
                try {
                    result = future.get();
                    break; // success
                } catch (InterruptedException e) {
                    // ignore
                    interrupted = true;
                } catch (ExecutionException ee) {
                    throw BasicSQL.handleInterruptions(startTime, ee);
                }
            }
        } finally {
            Thread.currentThread().setName(oldName);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return result;
    }

    public static void toStringSqlArgs(final StringBuilder sb, Object[] args) {
        if (args instanceof Object[][]) {
            // then we're doing a batch query
            for (Object[] rowOfArgs : (Object[][]) args) {
                toStringSqlArgs(sb, rowOfArgs);
            }
        } else {
            sb.append("("); // $NON-NLS-1$
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                if (arg == null) {
                    sb.append("NULL: 'null'"); // $NON-NLS-1$
                } else {
                    sb.append(arg.getClass().getName());
                    sb.append(": '"); // $NON-NLS-1$
                    if (arg instanceof ByteArrayInputStream) {
                        @SuppressWarnings("resource") // Caller is responsible for closing
                        ByteArrayInputStream bais = (ByteArrayInputStream) arg;
                        byte[] bytes = new byte[bais.available()];
                        bais.read(bytes, 0, bais.available());
                        prettyPrintByteArray(sb, bytes);
                    } else if (arg instanceof byte[]) {
                        prettyPrintByteArray(sb, (byte[]) arg);
                    } else {
                        sb.append(arg);
                    }
                    sb.append("'"); // $NON-NLS-1$
                }
                if (i != args.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")\n"); // $NON-NLS-1$
        }
    }

    private static final int MAX_ARRAY_PRINT_BYTES = 64;

    // Oracle use HEXTORAW('0f1f') to create a 2 byte RAW literal
    // Postgres use '\\x0f1f' to create a 2 byte bytea literal
    private static void prettyPrintByteArray(final StringBuilder sb, byte[] arg) {
        int printLength = Math.min(arg.length, MAX_ARRAY_PRINT_BYTES);
        for (int i = 0; i < printLength; i++) {
            sb.append(String.format("%02x", arg[i] & 0xff));
        }
        if (arg.length > MAX_ARRAY_PRINT_BYTES) {
            sb.append("...");
        }
    }

    public static void toStringSqlArgs(final StringBuilder sb, Iterable<Object[]> args) {
        // then we're doing a batch query
        for (Object[] rowOfArgs : args) {
            toStringSqlArgs(sb, rowOfArgs);
        }
    }

    public static void throwUncheckedIfSQLException(Throwable t) throws PalantirSqlException {
        if ((t != null) && SQLException.class.isAssignableFrom(t.getClass())) {
            SQLException kt = (SQLException) t;
            throw PalantirSqlException.create(kt);
        } else if ((t != null) && PalantirSqlException.class.isAssignableFrom(t.getClass())) {
            throw (PalantirSqlException) t;
        }
    }
}
