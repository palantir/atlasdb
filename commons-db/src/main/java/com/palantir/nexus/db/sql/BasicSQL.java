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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.concurrent.ThreadNamingCallable;
import com.palantir.db.oracle.JdbcHandler;
import com.palantir.db.oracle.JdbcHandler.BlobHandler;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.nexus.db.sql.monitoring.logger.SqlLoggers;
import com.palantir.sql.Connections;
import com.palantir.sql.PreparedStatements;
import com.palantir.sql.ResultSets;
import com.palantir.util.sql.VerboseSQLException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.Validate;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public abstract class BasicSQL {

    public interface SqlConfig {
        boolean isSqlCancellationDisabled();

        SqlTimer getSqlTimer();
    }

    protected abstract SqlConfig getSqlConfig();

    private static final String ORACLE_CANCEL_ERROR =
            "ORA-01013: user requested cancel of current operation"; //$NON-NLS-1$
    private static final String POSTGRES_CANCEL_ERROR = "ERROR: canceling statement due to user request"; // $NON-NLS-1$

    private static PalantirSqlException wrapSQLExceptionWithVerboseLogging(
            final SQLException sqlEx, String sql, Object[] args) {
        try {
            StringBuilder why = new StringBuilder();
            why.append("While attempting to execute the SQL '" + sql
                    + "' we caught a SQLException.\n" //$NON-NLS-1$ //$NON-NLS-2$
                    + "The SQL was executed with the following bind args:\n"); //$NON-NLS-1$
            BasicSQLUtils.toStringSqlArgs(why, args);
            why.append("End of verbose SQLException error message"); // $NON-NLS-1$
            return PalantirSqlException.create(new VerboseSQLException(sqlEx, why.toString()));
        } catch (Throwable e) {
            // make sure we don't interfere with the real error
            SqlLoggers.LOGGER.error(
                    "Trapped an exception while printing out information about an exception.  " //$NON-NLS-1$
                            + "No client requests were harmed.",
                    e); //$NON-NLS-1$
            SqlLoggers.LOGGER.error("Printing SQLException now so it doesn't get lost", sqlEx); // $NON-NLS-1$
        }
        return PalantirSqlException.create(sqlEx);
    }

    private static PalantirSqlException wrapSQLExceptionWithVerboseLogging(
            final PalantirSqlException sqlEx, String sql, Object[] args) {
        try {
            if (sqlEx.getCause() == null || !(sqlEx.getCause() instanceof SQLException)) {
                return sqlEx;
            }
            StringBuilder why = new StringBuilder();
            why.append("While attempting to execute the SQL '" + sql
                    + "' we caught a SQLException.\n" //$NON-NLS-1$ //$NON-NLS-2$
                    + "The SQL was executed with the following bind args:\n"); //$NON-NLS-1$
            BasicSQLUtils.toStringSqlArgs(why, args);
            why.append("End of verbose SQLException error message"); // $NON-NLS-1$
            return PalantirSqlException.create(
                    new VerboseSQLException((SQLException) sqlEx.getCause(), why.toString()));
        } catch (Throwable e) {
            // make sure we don't interfere with the real error
            SqlLoggers.LOGGER.error(
                    "Trapped an exception while printing out information about an exception.  " //$NON-NLS-1$
                            + "No client requests were harmed.",
                    e); //$NON-NLS-1$
            SqlLoggers.LOGGER.error("Printing SQLException now so it doesn't get lost", sqlEx); // $NON-NLS-1$
        }
        return sqlEx;
    }

    static String fillInQuery(String query, Object[] argArr) {
        for (Object x : argArr) {
            query = query.replaceFirst("\\Q?\\E", x.toString()); // $NON-NLS-1$
        }
        return query;
    }

    protected static void closeSilently(PreparedStatement ps) {
        closeSilently(ps, AutoClose.TRUE);
    }

    private static void closeSilently(ResultSet rs) {
        if (rs == null) {
            return;
        }

        try {
            rs.close();
        } catch (SQLException e) {
            SqlLoggers.LOGGER.error("ignored sql exception on resultSet close", e); // $NON-NLS-1$
        }
    }

    private static void closeSilently(PreparedStatement ps, AutoClose autoClose) {
        if (ps != null && autoClose == AutoClose.TRUE) {
            try {
                ps.close();
            } catch (SQLException ignored) {
                SqlLoggers.SQL_EXCEPTION_LOG.info("Caught ignored SQLException", ignored);
                SqlLoggers.LOGGER.error("ignored sql exception on preparedStatement close", ignored); // $NON-NLS-1$
            }
        }
    }

    // postgres acceptable timestamp range
    private static final long POSTGRES_BEGIN_RANGE = new DateTime(-4712, 1, 1, 0, 0, 0, 0).getMillis();
    private static final long POSTGRES_END_RANGE = new DateTime(294276, 1, 1, 0, 0, 0, 0).getMillis();

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    protected BlobHandler setObject(Connection c, PreparedStatement ps, int i, Object obj) throws PalantirSqlException {
        if (obj instanceof ByteArrayInputStream) {
            ByteArrayInputStream bais = (ByteArrayInputStream) obj;
            // Using #available is only okay for ByteArrayInputStream,
            // not for a generic InputStream
            PreparedStatements.setBinaryStream(ps, i, bais, bais.available());
        } else if (obj instanceof DateTime) {
            setDateTime(c, ps, i, (DateTime) obj);
        } else if (obj instanceof Boolean) {
            // TODO: gross hack because our code abuses the distinction between
            // boolean true and false and non-zeroness on a numeric field and
            // postgres complains because it is strict on column types.
            Boolean b = (Boolean) obj;
            Long converted = b.booleanValue() ? 1L : 0L;
            PreparedStatements.setObject(ps, i, converted);
        } else if (obj instanceof Number) {
            setNumber(ps, i, obj);
        } else {
            assert !(obj instanceof InputStream)
                    : "InputStreams must be passed as PTInputStreams so we know the length"; //$NON-NLS-1$
            PreparedStatements.setObject(ps, i, obj);
        }
        return null;
    }

    /**
     * Sets the specified joda {@link DateTime} on the prepared statement.
     */
    private static void setDateTime(Connection c, PreparedStatement ps, int i, DateTime dt)
            throws PalantirSqlException {
        // This method no longer attempts to store timezone information. Any
        // attempts to do so _must_ deal specifically with daylight savings time
        // (which caused issues for us before--see QA-14416 and QA-14625, and
        // the corresponding SVN revisions).
        long millis = dt.getMillis();
        if (DBType.getTypeFromConnection(c)
                == DBType.POSTGRESQL) { // postgres has a smaller range of allowable timestamps
            if (millis < POSTGRES_BEGIN_RANGE) {
                millis = POSTGRES_BEGIN_RANGE;
            }
            if (millis > POSTGRES_END_RANGE) {
                millis = POSTGRES_END_RANGE;
            }
        }
        PreparedStatements.setTimestamp(ps, i, new Timestamp(millis));
    }

    private static void setNumber(PreparedStatement ps, int i, Object obj) throws PalantirSqlException {
        if (obj instanceof Integer) {
            PreparedStatements.setInt(ps, i, (Integer) obj);
        } else if (obj instanceof Long) {
            PreparedStatements.setLong(ps, i, (Long) obj);
        } else if (obj instanceof Double) {
            PreparedStatements.setDouble(ps, i, (Double) obj);
        } else if (obj instanceof Float) {
            PreparedStatements.setFloat(ps, i, (Float) obj);
        } else if (obj instanceof AtomicInteger) {
            PreparedStatements.setInt(ps, i, ((AtomicInteger) obj).intValue());
        } else if (obj instanceof AtomicLong) {
            PreparedStatements.setLong(ps, i, ((AtomicLong) obj).longValue());
        } else if (obj instanceof Short) {
            PreparedStatements.setShort(ps, i, (Short) obj);
        } else if (obj instanceof Byte) {
            PreparedStatements.setByte(ps, i, (Byte) obj);
        } else {
            PreparedStatements.setObject(ps, i, obj);
        }
    }

    /**
     * Takes an object, serializes it, and returns a byte array suitable
     * for insertion into a BLOB.
     */
    static byte[] getBlobArray(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();

        return baos.toByteArray();
    }

    /**
     * Takes a byte array, deserializes it and returns an Object
     */
    @SuppressWarnings("BanSerializableRead")
    static Object getBlobObject(InputStream stream) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(stream);
        Object object = ois.readObject();
        ois.close();

        return object;
    }

    public enum OffsetInclusion {
        INCLUDE_OFFSET,
        EXCLUDE_OFFSET;
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    static String getInClause(Collection<Long> allowed) {
        Iterator<Long> it = allowed.iterator();
        if (!it.hasNext()) {
            assert false;
            return "( )"; //$NON-NLS-1$
        }
        StringBuilder sb = new StringBuilder();
        sb.append("( "); // $NON-NLS-1$
        sb.append(it.next());

        while (it.hasNext()) {
            sb.append(", "); // $NON-NLS-1$
            sb.append(it.next());
        }

        sb.append(" )"); // $NON-NLS-1$
        return sb.toString();
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    static long getLongFromNumber(Object obj) {
        assert (obj != null);
        return (obj != null) ? ((BigDecimal) obj).longValue() : 0L;
    }

    static long getLongFromNumber(Object obj, long returnIfNull) {
        return (obj != null) ? ((BigDecimal) obj).longValue() : returnIfNull;
    }

    /**
     * Encapsulates the logic for creating a prepared statement with the arguments set
     */
    private PreparedStatement createPreparedStatement(Connection c, String sql, Object[] vs)
            throws PalantirSqlException {
        PreparedStatement ps;
        ps = Connections.prepareStatement(c, sql);
        List<BlobHandler> toClean = new ArrayList<>();
        if (vs != null) {
            try {
                for (int i = 0; i < vs.length; i++) {
                    BlobHandler cleanup = setObject(c, ps, i + 1, vs[i]);
                    if (cleanup != null) {
                        toClean.add(cleanup);
                    }
                }
            } catch (Exception e) {
                // if we throw, we need to clean up any blobs we have already made
                for (BlobHandler cleanupBlob : toClean) {
                    try {
                        cleanupBlob.freeTemporary();
                    } catch (Exception e1) {
                        SqlLoggers.LOGGER.error("failed to free temp blob", e1); // $NON-NLS-1$
                    }
                }
                BasicSQLUtils.throwUncheckedIfSQLException(e);
                throw Throwables.throwUncheckedException(e);
            }
        }
        return BlobCleanupPreparedStatement.create(ps, toClean);
    }

    private static final class BlobCleanupPreparedStatement implements InvocationHandler {
        final PreparedStatement ps;
        final Collection<BlobHandler> toCleanup;

        static PreparedStatement create(PreparedStatement ps, Collection<BlobHandler> toCleanup) {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    new BlobCleanupPreparedStatement(ps, toCleanup));
        }

        private BlobCleanupPreparedStatement(PreparedStatement ps, Collection<BlobHandler> toCleanup) {
            Preconditions.checkNotNull(ps);
            Validate.noNullElements(toCleanup);
            this.ps = ps;
            this.toCleanup = ImmutableList.copyOf(toCleanup);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) { // $NON-NLS-1$
                for (BlobHandler cleanup : toCleanup) {
                    try {
                        cleanup.freeTemporary();
                    } catch (Exception e) {
                        SqlLoggers.LOGGER.error("failed to free temp blob", e); // $NON-NLS-1$
                    }
                }
            }
            try {
                return method.invoke(ps, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    public static boolean assertNotOnSqlThread() {
        assert !Thread.currentThread().getName().contains(SELECT_THREAD_NAME);
        assert !Thread.currentThread().getName().contains(EXECUTE_THREAD_NAME);
        return true;
    }

    private static final String SELECT_THREAD_NAME = "SQL select statement"; // $NON-NLS-1$
    private static final String EXECUTE_THREAD_NAME = "SQL execute statement"; // $NON-NLS-1$

    // TODO (jkong): Should these be lazily initialized?
    private static final Supplier<ExecutorService> DEFAULT_SELECT_EXECUTOR =
            Suppliers.memoize(() -> PTExecutors.newCachedThreadPool(SELECT_THREAD_NAME));
    static final Supplier<ExecutorService> DEFAULT_EXECUTE_EXECUTOR =
            Suppliers.memoize(() -> PTExecutors.newCachedThreadPool(EXECUTE_THREAD_NAME));

    private ExecutorService selectStatementExecutor;
    private ExecutorService executeStatementExecutor;

    public BasicSQL() {
        this(DEFAULT_SELECT_EXECUTOR.get(), DEFAULT_EXECUTE_EXECUTOR.get());
    }

    /**
     * Management of the life-cycle of these executors is left to the user.
     */
    public BasicSQL(ExecutorService selectStatementExecutor, ExecutorService executeStatementExecutor) {
        this.selectStatementExecutor = selectStatementExecutor;
        this.executeStatementExecutor = executeStatementExecutor;
    }

    protected enum AutoClose {
        TRUE,
        FALSE,
    }

    /**
     * Execute the PreparedStatement asynchronously, cancel it if we're interrupted, and return the result set if we're not.
     * Throws a RuntimeException (PalantirInterruptedException) in case of interrupts.
     */
    private <T> T runCancellably(
            PreparedStatement ps, ResultSetVisitor<T> visitor, FinalSQLString sql, @Nullable Integer fetchSize)
            throws PalantirInterruptedException, PalantirSqlException {
        return runCancellably(ps, visitor, sql, AutoClose.TRUE, fetchSize);
    }

    private <T> T runCancellably(
            final PreparedStatement ps,
            ResultSetVisitor<T> visitor,
            final FinalSQLString sql,
            AutoClose autoClose,
            @Nullable Integer fetchSize)
            throws PalantirInterruptedException, PalantirSqlException {
        if (isSqlCancellationDisabled()) {
            return runUninterruptablyInternal(ps, visitor, sql, autoClose, fetchSize);
        } else {
            return runCancellablyInternal(ps, visitor, sql, autoClose, fetchSize);
        }
    }

    protected boolean isSqlCancellationDisabled() {
        return getSqlConfig().isSqlCancellationDisabled();
    }

    private <T> T runUninterruptablyInternal(
            final PreparedStatement ps,
            final ResultSetVisitor<T> visitor,
            final FinalSQLString sql,
            final AutoClose autoClose,
            @Nullable Integer fetchSize)
            throws PalantirInterruptedException, PalantirSqlException {
        if (Thread.currentThread().isInterrupted()) {
            SqlLoggers.CANCEL_LOGGER.debug("interrupted prior to executing uninterruptable SQL call");
            throw new PalantirInterruptedException("interrupted prior to executing uninterruptable SQL call");
        }
        return BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                () -> {
                    if (fetchSize != null) {
                        ps.setFetchSize(fetchSize);
                    }
                    ResultSet rs = null;
                    try {
                        rs = ps.executeQuery();
                        return visitor.visit(rs);
                    } finally {
                        if (rs != null && autoClose == AutoClose.TRUE) {
                            rs.close();
                        }
                    }
                },
                sql.toString(),
                /* We no longer have a connection to worry about */ null);
    }

    private <T> T runCancellablyInternal(
            final PreparedStatement ps,
            ResultSetVisitor<T> visitor,
            final FinalSQLString sql,
            AutoClose autoClose,
            @Nullable Integer fetchSize)
            throws PalantirInterruptedException, PalantirSqlException {
        final String threadString = sql.toString();
        Future<ResultSet> result = selectStatementExecutor.submit(ThreadNamingCallable.wrapWithThreadName(
                () -> {
                    if (Thread.currentThread().isInterrupted()) {
                        SqlLoggers.CANCEL_LOGGER.error("Threadpool thread has interrupt flag set!"); // $NON-NLS-1$
                        // we want to clear the interrupted status here -
                        // we cancel via the prepared statement, not interrupts
                        Thread.interrupted();
                    }
                    if (fetchSize != null) {
                        ps.setFetchSize(fetchSize);
                    }
                    return ps.executeQuery();
                },
                threadString,
                ThreadNamingCallable.Type.APPEND));

        ResultSet rs = null;
        long startTime = System.currentTimeMillis();
        final String oldName = Thread.currentThread().getName();
        final String currentTimestamp = DateTimeFormat.forPattern("HH:mm:ss").print(System.currentTimeMillis());
        Thread.currentThread().setName(oldName + " blocking on " + threadString + " started at " + currentTimestamp);
        try {
            rs = result.get();
            return visitor.visit(rs);
        } catch (InterruptedException e) {
            try {
                SqlLoggers.CANCEL_LOGGER.debug("about to cancel a SQL call", e); // $NON-NLS-1$
                PreparedStatements.cancel(ps);
                throw new PalantirInterruptedException("SQL call interrupted", e); // $NON-NLS-1$
            } finally {
                // we delay this as long as possible, but under no
                // circumstances lose it
                Thread.currentThread().interrupt();
            }
        } catch (ExecutionException ee) {
            throw handleInterruptions(startTime, ee);
        } finally {
            Thread.currentThread().setName(oldName);
            if (rs != null && autoClose == AutoClose.TRUE) {
                ResultSets.close(rs);
            }
        }
    }

    interface PreparedStatementVisitor<T> {
        T visit(PreparedStatement ps) throws PalantirSqlException;
    }

    interface ResultSetVisitor<T> {
        T visit(ResultSet rs) throws PalantirSqlException;
    }

    protected <T> T wrapPreparedStatement(
            Connection c, FinalSQLString query, Object[] vs, PreparedStatementVisitor<T> visitor, String description)
            throws PalantirSqlException, PalantirInterruptedException {
        return wrapPreparedStatement(c, query, vs, visitor, description, AutoClose.TRUE);
    }

    private <T> T wrapPreparedStatement(
            final Connection c,
            final FinalSQLString query,
            final Object[] vs,
            PreparedStatementVisitor<T> visitor,
            String description,
            AutoClose autoClose)
            throws PalantirSqlException, PalantirInterruptedException {
        SqlTimer.Handle timerKey = getSqlTimer().start(description, query.getKey(), query.getQuery());
        PreparedStatement ps = null;

        try {
            ps = BasicSQLUtils.runUninterruptably(
                    executeStatementExecutor,
                    () -> createPreparedStatement(c, query.getQuery(), vs),
                    "SQL createPreparedStatement",
                    c);
            return visitor.visit(ps);
        } catch (PalantirSqlException sqle) {
            throw wrapSQLExceptionWithVerboseLogging(sqle, query.getQuery(), vs);
        } finally {
            closeSilently(ps, autoClose);
            timerKey.stop();
        }
    }

    static PalantirSqlException handleInterruptions(long startTime, ExecutionException ee) throws PalantirSqlException {
        SQLException e = getSQLException(ee.getCause());
        return handleInterruptions(startTime, e);
    }

    private static SQLException getSQLException(Throwable cause) {
        if (cause instanceof SQLException) {
            return (SQLException) cause;
        }
        if (cause.getCause() == null || !(cause instanceof PalantirSqlException)) {
            throw Throwables.rewrapAndThrowUncheckedException(cause);
        }
        return getSQLException(cause.getCause());
    }

    public static PalantirSqlException handleInterruptions(long startTime, SQLException cause)
            throws PalantirSqlException {
        SqlLoggers.SQL_EXCEPTION_LOG.debug("Caught SQLException", cause);

        String message = cause.getMessage().trim();
        // check for oracle and postgres
        if (!message.contains(ORACLE_CANCEL_ERROR) && !message.contains(POSTGRES_CANCEL_ERROR)) {
            throw PalantirSqlException.create(cause);
        }
        String elapsedTime = "N/A";
        if (startTime > 0) {
            elapsedTime = String.valueOf(System.currentTimeMillis() - startTime); // $NON-NLS-1$
        }
        SqlLoggers.CANCEL_LOGGER.info(
                "We got an execution exception that was an interrupt, most likely from someone " //$NON-NLS-1$
                        + "incorrectly ignoring an interrupt. "
                        + "Elapsed time: {}\n"
                        + "Error message: {} "
                        + "Error code: {} "
                        + "Error state: {} "
                        + "Error cause: {}",
                SafeArg.of("elapsedTime", elapsedTime),
                SafeArg.of("message", cause.getMessage()), // $NON-NLS-1$
                SafeArg.of("errorCode", cause.getErrorCode()), // $NON-NLS-1$
                SafeArg.of("SQLState", cause.getSQLState()), // $NON-NLS-1$
                SafeArg.of("nextException", cause.getNextException()), // $NON-NLS-1$
                new Exception("Just for a stack trace"));
        Thread.currentThread().interrupt();
        throw new PalantirInterruptedException("SQL call interrupted", cause); // $NON-NLS-1$
    }

    public SqlTimer getSqlTimer() {
        return getSqlConfig().getSqlTimer();
    }

    protected PreparedStatement execute(
            final Connection c, final FinalSQLString sql, final Object vs[], final AutoClose autoClose)
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL execution query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                () -> {
                    return wrapPreparedStatement(
                            c,
                            sql,
                            vs,
                            ps -> {
                                PreparedStatements.execute(ps);
                                return ps;
                            },
                            "execute",
                            autoClose); //$NON-NLS-1$
                },
                sql.toString(),
                c);
    }

    protected boolean selectExistsInternal(final Connection c, final FinalSQLString sql, Object... vs)
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectExistsInternal query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return wrapPreparedStatement(
                c,
                sql,
                vs,
                ps -> {
                    PreparedStatements.setMaxRows(ps, 1);

                    return runCancellably(ps, ResultSets::next, sql, null);
                },
                "selectExists"); //$NON-NLS-1$
    }

    @SuppressWarnings("BadAssert") // only fail check with asserts enabled
    protected int selectIntegerInternal(final Connection c, final FinalSQLString sql, Object... vs)
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectIntegerInternal query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return wrapPreparedStatement(
                c,
                sql,
                vs,
                ps -> runCancellably(
                        ps,
                        rs -> {
                            if (ResultSets.next(rs)) {
                                int ret = ResultSets.getInt(rs, 1);
                                if (ResultSets.next(rs)) {
                                    SqlLoggers.LOGGER.error(
                                            "In selectIntegerInternal more than one row was returned for query: {}",
                                            SafeArg.of("sql", sql)); // $NON-NLS-1$
                                    assert false : "Found more than one row in SQL#selectInteger"; // $NON-NLS-1$
                                }
                                return ret;
                            }

                            // indicate failure
                            throw PalantirSqlException.create("No rows returned."); // $NON-NLS-1$
                        },
                        sql,
                        null),
                "selectInteger"); //$NON-NLS-1$
    }

    @SuppressWarnings("BadAssert") // only fail check with asserts enabled
    protected Long selectLongInternal(
            final Connection c,
            final FinalSQLString sql,
            Object vs[],
            final Long defaultVal,
            final boolean useBrokenBehaviorWithNullAndZero)
            throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectLongInternal query", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return wrapPreparedStatement(
                c,
                sql,
                vs,
                ps -> runCancellably(
                        ps,
                        rs -> {
                            if (ResultSets.next(rs)) {
                                Long ret = null;
                                if (ResultSets.getObject(rs, 1) != null) {
                                    ret = ResultSets.getLong(rs, 1);
                                }

                                if (ResultSets.next(rs)) {
                                    SqlLoggers.LOGGER.error(
                                            "In selectLongInternal more than one row was returned for query: {}",
                                            SafeArg.of("sql", sql)); // $NON-NLS-1$
                                    assert false : "Found more than one row in SQL#selectLong"; // $NON-NLS-1$
                                }

                                if (ret != null) {
                                    return ret;
                                } else if (useBrokenBehaviorWithNullAndZero) {
                                    SqlLoggers.LOGGER.error(
                                            "In selectLongInternal null was returned in the one row for this query: {}",
                                            SafeArg.of("sql", sql)); // $NON-NLS-1$
                                    assert false
                                            : "If this case is hit, it is a programming error. "
                                                    + "You should use a default value."; //$NON-NLS-1$
                                    return 0L;
                                }
                            }

                            if (useBrokenBehaviorWithNullAndZero && defaultVal == null) {
                                throw PalantirSqlException.create("No rows returned."); // $NON-NLS-1$
                            } else {
                                return defaultVal;
                            }
                        },
                        sql,
                        null),
                "selectLong"); //$NON-NLS-1$
    }

    protected AgnosticLightResultSet selectLightResultSetSpecifyingDBType(
            final Connection c, final FinalSQLString sql, Object vs[], final DBType dbType, @Nullable Integer fetchSize)
            throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL light result set selection query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }

        PreparedStatementVisitor<AgnosticLightResultSet> preparedStatementVisitor = ps -> {
            final ResultSetVisitor<AgnosticLightResultSet> resultSetVisitor = rs -> {
                try {
                    return new AgnosticLightResultSetImpl(
                            rs,
                            dbType,
                            rs.getMetaData(),
                            ps,
                            "selectList", //$NON-NLS-1$
                            sql,
                            getSqlTimer());
                } catch (Exception e) {
                    closeSilently(rs);
                    BasicSQLUtils.throwUncheckedIfSQLException(e);
                    throw Throwables.throwUncheckedException(e);
                }
            };

            try {
                return runCancellably(ps, resultSetVisitor, sql, AutoClose.FALSE, fetchSize);
            } catch (Exception e) {
                closeSilently(ps);
                BasicSQLUtils.throwUncheckedIfSQLException(e);
                throw Throwables.throwUncheckedException(e);
            }
        };

        return wrapPreparedStatement(
                c,
                sql,
                vs,
                preparedStatementVisitor,
                "selectList", //$NON-NLS-1$
                AutoClose.FALSE); // don't close the ps
    }

    protected AgnosticResultSet selectResultSetSpecifyingDBType(
            final Connection c, final FinalSQLString sql, Object[] vs, final DBType dbType)
            throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL result set query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return wrapPreparedStatement(
                c,
                sql,
                vs,
                ps -> runCancellably(
                        ps,
                        (ResultSetVisitor<AgnosticResultSet>) rs -> {
                            List<List<Object>> rvs = new ArrayList<List<Object>>();
                            ResultSetMetaData meta = ResultSets.getMetaData(rs);
                            int columnCount = ResultSets.getColumnCount(meta);
                            Map<String, Integer> columnMap = ResultSets.buildInMemoryColumnMap(meta, dbType);
                            while (ResultSets.next(rs)) {

                                List<Object> row = new ArrayList<>(columnCount);

                                for (int i = 0; i < columnCount; i++) {
                                    row.add(ResultSets.getObject(rs, i + 1));
                                }

                                rvs.add(row);
                            }

                            return new AgnosticResultSetImpl(rvs, dbType, columnMap);
                        },
                        sql,
                        null),
                "selectList"); //$NON-NLS-1$
    }

    PreparedStatement updateInternal(
            final Connection c, final FinalSQLString sql, final Object vs[], final AutoClose autoClose)
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL update interval query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                () -> {
                    return wrapPreparedStatement(
                            c,
                            sql,
                            vs,
                            ps -> {
                                PreparedStatements.execute(ps);
                                return ps;
                            },
                            "update",
                            autoClose); //$NON-NLS-1$
                },
                sql.toString(),
                c);
    }

    protected void updateMany(final Connection c, final FinalSQLString sql, final Object vs[][])
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL update many query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                (Callable<Void>) () -> {
                    List<BlobHandler> cleanups = new ArrayList<>();
                    PreparedStatement ps = null;
                    SqlTimer.Handle timerKey = getSqlTimer()
                            .start(
                                    "updateMany(" + vs.length + ")",
                                    sql.getKey(),
                                    sql.getQuery()); // $NON-NLS-1$ //$NON-NLS-2$
                    try {
                        ps = c.prepareStatement(sql.getQuery());
                        for (int i = 0; i < vs.length; i++) {
                            for (int j = 0; j < vs[i].length; j++) {
                                Object obj = vs[i][j];
                                BlobHandler cleanup = setObject(c, ps, j + 1, obj);
                                if (cleanup != null) {
                                    cleanups.add(cleanup);
                                }
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (SQLException sqle) {
                        SqlLoggers.SQL_EXCEPTION_LOG.debug("Caught SQLException", sqle);
                        throw wrapSQLExceptionWithVerboseLogging(sqle, sql.getQuery(), vs);
                    } finally {
                        closeSilently(ps);
                        timerKey.stop();
                        for (BlobHandler cleanup : cleanups) {
                            try {
                                cleanup.freeTemporary();
                            } catch (Exception e) {
                                SqlLoggers.LOGGER.error("failed to free temp blob", e); // $NON-NLS-1$
                            }
                        }
                    }
                    return null;
                },
                sql.toString(),
                c);
    }

    protected int insertOneCountRowsInternal(final Connection c, final FinalSQLString sql, final Object... vs)
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace(
                    "SQL insert one count rows internal query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                () -> {
                    return wrapPreparedStatement(
                            c,
                            sql,
                            vs,
                            ps -> {
                                PreparedStatements.execute(ps);
                                return PreparedStatements.getUpdateCount(ps);
                            },
                            "insertOne"); //$NON-NLS-1$
                },
                sql.toString(),
                c);
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    public boolean insertMany(final Connection c, final FinalSQLString sql, final Object vs[][])
            throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL insert many query: {}", SafeArg.of("sqlQuery", sql.getQuery()));
        }
        return BasicSQLUtils.runUninterruptably(
                executeStatementExecutor,
                () -> {
                    int[] inserted = null;
                    PreparedStatement ps = null;

                    SqlTimer.Handle timerKey = getSqlTimer()
                            .start(
                                    "insertMany(" + vs.length + ")",
                                    sql.getKey(),
                                    sql.getQuery()); // $NON-NLS-1$ //$NON-NLS-2$
                    List<BlobHandler> cleanups = new ArrayList<>();
                    try {
                        ps = c.prepareStatement(sql.getQuery());
                        for (int i = 0; i < vs.length; i++) {
                            for (int j = 0; j < vs[i].length; j++) {
                                Object obj = vs[i][j];
                                BlobHandler cleanup = setObject(c, ps, j + 1, obj);
                                if (cleanup != null) {
                                    cleanups.add(cleanup);
                                }
                            }
                            ps.addBatch();
                        }

                        inserted = ps.executeBatch();
                    } catch (SQLException sqle) {
                        SqlLoggers.SQL_EXCEPTION_LOG.debug("Caught SQLException", sqle);
                        throw wrapSQLExceptionWithVerboseLogging(sqle, sql.getQuery(), vs);
                    } finally {
                        closeSilently(ps);
                        timerKey.stop();
                        for (BlobHandler cleanup : cleanups) {
                            try {
                                cleanup.freeTemporary();
                            } catch (Exception e) {
                                SqlLoggers.LOGGER.error("failed to free temp blob", e); // $NON-NLS-1$
                            }
                        }
                    }
                    if (inserted == null || inserted.length != vs.length) {
                        assert false;
                        return false;
                    }
                    for (int numInsertedForRow : inserted) {
                        if (numInsertedForRow == Statement.EXECUTE_FAILED) {
                            assert DBType.getTypeFromConnection(c) != DBType.ORACLE
                                    : "numInsertedForRow: " + numInsertedForRow; // $NON-NLS-1$
                            return false;
                        }
                    }
                    return true;
                },
                sql.toString(),
                c);
    }

    protected int updateCountRowsInternal(Connection c, FinalSQLString sql, Object... vs) throws PalantirSqlException {
        PreparedStatement ps = null;
        try {
            ps = updateInternal(c, sql, vs, AutoClose.FALSE);
            return PreparedStatements.getUpdateCount(ps);
        } finally {
            closeSilently(ps);
        }
    }

    protected JdbcHandler getJdbcHandler() {
        return new JdbcHandler() {
            @Override
            public ArrayHandler createStructArray(String structType, String arrayType, List<Object[]> elements) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BlobHandler createBlob(Connection c) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
