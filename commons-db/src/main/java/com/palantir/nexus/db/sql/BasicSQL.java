/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.sql;

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

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.concurrent.ThreadNamingCallable;
import com.palantir.db.oracle.JdbcHandler;
import com.palantir.db.oracle.JdbcHandler.BlobHandler;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.ResourceCreationLocation;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.nexus.db.sql.monitoring.logger.SqlLoggers;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.sql.Connections;
import com.palantir.sql.PreparedStatements;
import com.palantir.sql.ResultSets;
import com.palantir.util.sql.VerboseSQLException;

public abstract class BasicSQL {

    public interface SqlConfig {
        boolean isSqlCancellationDisabled();

        SqlTimer getSqlTimer();
    }

    protected abstract SqlConfig getSqlConfig();

    private static final String ORACLE_CANCEL_ERROR = "ORA-01013: user requested cancel of current operation"; //$NON-NLS-1$
    private static final String POSTGRES_CANCEL_ERROR = "ERROR: canceling statement due to user request"; //$NON-NLS-1$

    private static PalantirSqlException wrapSQLExceptionWithVerboseLogging(final SQLException sqlEx, String sql, Object[] args) {
        try {
            StringBuilder why = new StringBuilder();
            why.append("While attempting to execute the SQL '" + sql + "' we caught a SQLException.\n" + //$NON-NLS-1$ //$NON-NLS-2$
                    "The SQL was executed with the following bind args:\n"); //$NON-NLS-1$
            BasicSQLUtils.toStringSqlArgs(why, args);
            why.append("End of verbose SQLException error message"); //$NON-NLS-1$
            return PalantirSqlException.create(new VerboseSQLException(sqlEx, why.toString()));
        } catch (Throwable e) {
            // make sure we don't interfere with the real error
            SqlLoggers.LOGGER.error("Trapped an exception while printing out information about an exception.  " + //$NON-NLS-1$
                    "No client requests were harmed.", e); //$NON-NLS-1$
            SqlLoggers.LOGGER.error("Printing SQLException now so it doesn't get lost", sqlEx); //$NON-NLS-1$
        }
        return PalantirSqlException.create(sqlEx);
    }

    private static PalantirSqlException wrapSQLExceptionWithVerboseLogging(final PalantirSqlException sqlEx, String sql, Object[] args) {
        try {
            if (sqlEx.getCause() == null || !(sqlEx.getCause() instanceof SQLException)) {
                return sqlEx;
            }
            StringBuilder why = new StringBuilder();
            why.append("While attempting to execute the SQL '" + sql + "' we caught a SQLException.\n" + //$NON-NLS-1$ //$NON-NLS-2$
                    "The SQL was executed with the following bind args:\n"); //$NON-NLS-1$
            BasicSQLUtils.toStringSqlArgs(why, args);
            why.append("End of verbose SQLException error message"); //$NON-NLS-1$
            return PalantirSqlException.create(new VerboseSQLException((SQLException) sqlEx.getCause(), why.toString()));
        } catch (Throwable e) {
            // make sure we don't interfere with the real error
            SqlLoggers.LOGGER.error("Trapped an exception while printing out information about an exception.  " + //$NON-NLS-1$
                    "No client requests were harmed.", e); //$NON-NLS-1$
            SqlLoggers.LOGGER.error("Printing SQLException now so it doesn't get lost", sqlEx); //$NON-NLS-1$
        }
        return sqlEx;
    }

    static String fillInQuery(String query, Object[] argArr) {
        for (Object x : argArr) {
            query = query.replaceFirst("\\Q?\\E", x.toString()); //$NON-NLS-1$
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
        } catch(SQLException e) {
            SqlLoggers.LOGGER.error("ignored sql exception on resultSet close",e); //$NON-NLS-1$
        }
    }

    private static void closeSilently(PreparedStatement ps, AutoClose autoClose) {
        if (ps != null && autoClose == AutoClose.TRUE) {
            try {
                ps.close();
            } catch (SQLException ignored ) {
                SqlLoggers.SQL_EXCEPTION_LOG.info("Caught ignored SQLException", ignored);
                SqlLoggers.LOGGER.error("ignored sql exception on preparedStatement close", ignored); //$NON-NLS-1$
            }
        }
    }


    //postgres acceptable timestamp range
    private static final long POSTGRES_BEGIN_RANGE = new DateTime(-4712, 1, 1, 0, 0, 0, 0).getMillis();
    private static final long POSTGRES_END_RANGE = new DateTime(294276, 1, 1, 0, 0, 0, 0).getMillis();


    protected BlobHandler setObject(Connection c, PreparedStatement ps, int i, Object obj) throws PalantirSqlException {
        if (obj instanceof ByteArrayInputStream) {
            ByteArrayInputStream bais = (ByteArrayInputStream)obj;
            // Using #available is only okay for ByteArrayInputStream,
            // not for a generic InputStream
            PreparedStatements.setBinaryStream(ps, i, bais, bais.available());
        } else if (obj instanceof org.joda.time.DateTime) {
            setDateTime(c, ps, i, (DateTime)obj);
        } else if (obj instanceof Boolean) {
            // TODO: gross hack because our code abuses the distinction between
            // boolean true and false and non-zeroness on a numeric field and
            // postgres complains because it is strict on column types.
            Boolean b = (Boolean)obj;
            Long converted = b.booleanValue() ? 1L : 0L;
            PreparedStatements.setObject(ps, i, converted);
        } else if (obj instanceof Number) {
            setNumber(ps, i, obj);
        } else {
            assert !(obj instanceof InputStream) : "InputStreams must be passed as PTInputStreams so we know the length"; //$NON-NLS-1$
            PreparedStatements.setObject(ps, i, obj);
        }
        return null;
    }

    /**
     * Sets the specified joda {@link DateTime} on the prepared statement.
     */
    private static void setDateTime(Connection c, PreparedStatement ps, int i, DateTime dt) throws PalantirSqlException {
        // This method no longer attempts to store timezone information. Any
        // attempts to do so _must_ deal specifically with daylight savings time
        // (which caused issues for us before--see QA-14416 and QA-14625, and
        // the corresponding SVN revisions).
        long millis = dt.getMillis();
        if (DBType.getTypeFromConnection(c) == DBType.POSTGRESQL) { //postgres has a smaller range of allowable timestamps
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

    /** Takes a byte array, deserializes it and returns an Object
     */
    static Object getBlobObject(InputStream stream) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(stream);
        Object object = ois.readObject();
        ois.close();

        return object;
    }

    public enum OffsetInclusion {
        INCLUDE_OFFSET, EXCLUDE_OFFSET;
    }

    static String getInClause(Collection<Long> allowed) {
        Iterator<Long> it = allowed.iterator();
        if (!it.hasNext()) {
            assert false;
            return "( )"; //$NON-NLS-1$
        }
        StringBuilder sb = new StringBuilder();
        sb.append("( "); //$NON-NLS-1$
        sb.append(it.next());

        while (it.hasNext()) {
            sb.append(", "); //$NON-NLS-1$
            sb.append(it.next());
        }

        sb.append(" )"); //$NON-NLS-1$
        return sb.toString();
    }

    static long getLongFromNumber(Object obj) {
        assert(obj != null);
        return (obj != null) ? ((BigDecimal)obj).longValue() : 0L;
    }

    static long getLongFromNumber(Object obj, long returnIfNull) {
        return (obj != null) ? ((BigDecimal)obj).longValue() : returnIfNull;
    }



    /** Encapsulates the logic for creating a prepared statement with the arguments set*/
    private PreparedStatement createPreparedStatement(Connection c,
            String sql, Object[] vs) throws PalantirSqlException {
        PreparedStatement ps;
        ps = Connections.prepareStatement(c, sql);
        List<BlobHandler> toClean = Lists.newArrayList();
        if (vs != null) {
            try {
                for (int i=0; i < vs.length; i++) {
                    BlobHandler cleanup = setObject(c, ps, i+1, vs[i]);
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
                        SqlLoggers.LOGGER.error("failed to free temp blob", e1); //$NON-NLS-1$
                    }
                }
                BasicSQLUtils.throwUncheckedIfSQLException(e);
                throw Throwables.throwUncheckedException(e);
            }
        }
        return BlobCleanupPreparedStatement.create(ps, toClean);
    }

    private static class BlobCleanupPreparedStatement implements InvocationHandler {
        final PreparedStatement ps;
        final Collection<BlobHandler> toCleanup;

        static PreparedStatement create(PreparedStatement ps, Collection<BlobHandler> toCleanup) {
            return (PreparedStatement)Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, new BlobCleanupPreparedStatement(ps, toCleanup));
        }

        private BlobCleanupPreparedStatement(PreparedStatement ps, Collection<BlobHandler> toCleanup) {
            Validate.notNull(ps);
            Validate.noNullElements(toCleanup);
            this.ps = ps;
            this.toCleanup = ImmutableList.copyOf(toCleanup);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) { //$NON-NLS-1$
                for (BlobHandler cleanup : toCleanup) {
                    try {
                        cleanup.freeTemporary();
                    } catch (Exception e) {
                        SqlLoggers.LOGGER.error("failed to free temp blob", e); //$NON-NLS-1$
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

    public static boolean assertNotOnSqlThread() {
        assert !Thread.currentThread().getName().contains(selectThreadName);
        assert !Thread.currentThread().getName().contains(executeThreadName);
        return true;
    }

    private static final String selectThreadName = "SQL select statement"; //$NON-NLS-1$
    private static final String executeThreadName = "SQL execute statement"; //$NON-NLS-1$
    private static final int KEEP_SQL_THREAD_ALIVE_TIMEOUT = 3000; //3 seconds
    private static ExecutorService service = Tracers.wrap(PTExecutors.newCachedThreadPool(
            new NamedThreadFactory(selectThreadName, true), KEEP_SQL_THREAD_ALIVE_TIMEOUT));
    static ExecutorService executeService = Tracers.wrap(PTExecutors.newCachedThreadPool(
            new NamedThreadFactory(executeThreadName, true), KEEP_SQL_THREAD_ALIVE_TIMEOUT));

    protected static enum AutoClose {
        TRUE, FALSE;
    }

    /**Execute the PreparedStatement asynchronously, cancel it if we're interrupted, and return the result set if we're not.
    Throws a RuntimeException (PalantirInterruptedException) in case of interrupts. */
    private <T> T runCancellably(PreparedStatement ps, ResultSetVisitor<T> visitor, FinalSQLString sql,
            @Nullable Integer fetchSize) throws PalantirInterruptedException, PalantirSqlException {
        return runCancellably(ps, visitor, sql, AutoClose.TRUE, fetchSize);
    }

    private <T> T runCancellably(final PreparedStatement ps, ResultSetVisitor<T> visitor, final FinalSQLString sql,
            AutoClose autoClose, @Nullable Integer fetchSize) throws PalantirInterruptedException, PalantirSqlException {
        if (isSqlCancellationDisabled()) {
            return runUninterruptablyInternal(ps, visitor, sql, autoClose, fetchSize);
        } else {
            return runCancellablyInternal(ps, visitor, sql, autoClose, fetchSize);
        }
    }

    protected boolean isSqlCancellationDisabled() {
        return getSqlConfig().isSqlCancellationDisabled();
    }

    private static <T> T runUninterruptablyInternal(final PreparedStatement ps, final ResultSetVisitor<T> visitor, final FinalSQLString sql,
            final AutoClose autoClose, @Nullable Integer fetchSize) throws PalantirInterruptedException, PalantirSqlException {
        if (Thread.currentThread().isInterrupted()) {
            SqlLoggers.CANCEL_LOGGER.debug("interrupted prior to executing uninterruptable SQL call");
            throw new PalantirInterruptedException("interrupted prior to executing uninterruptable SQL call");
        }
        return BasicSQLUtils.runUninterruptably(new Callable<T>() {
            @Override
            public T call() throws SQLException {
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
            }
        },
                sql.toString(),
                /* We no longer have a connection to worry about */ null);
    }

    private static <T> T runCancellablyInternal(final PreparedStatement ps, ResultSetVisitor<T> visitor, final FinalSQLString sql,
                                        AutoClose autoClose, @Nullable Integer fetchSize) throws PalantirInterruptedException, PalantirSqlException {
        final String threadString = sql.toString();
        Future<ResultSet> result = service.submit(ThreadNamingCallable.wrapWithThreadName(new Callable<ResultSet>() {
            @Override
            public ResultSet call() throws Exception {
                if(Thread.currentThread().isInterrupted()) {
                    SqlLoggers.CANCEL_LOGGER.error("Threadpool thread has interrupt flag set!"); //$NON-NLS-1$
                    //we want to clear the interrupted status here -
                    //we cancel via the prepared statement, not interrupts
                    Thread.interrupted();
                }
                if (fetchSize != null) {
                    ps.setFetchSize(fetchSize);
                }
                return ps.executeQuery();
            }
        }, threadString, ThreadNamingCallable.Type.APPEND));

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
                SqlLoggers.CANCEL_LOGGER.debug("about to cancel a SQL call", e); //$NON-NLS-1$
                PreparedStatements.cancel(ps);
                throw new PalantirInterruptedException("SQL call interrupted", e); //$NON-NLS-1$
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

    static interface PreparedStatementVisitor<T> {
        public T visit(PreparedStatement ps) throws PalantirSqlException;
    }
    static interface ResultSetVisitor<T> {
        public T visit(ResultSet rs) throws PalantirSqlException;
    }

    protected <T> T wrapPreparedStatement(Connection c,
            FinalSQLString query, Object[] vs, PreparedStatementVisitor<T> visitor, String description) throws PalantirSqlException, PalantirInterruptedException {
        return wrapPreparedStatement(c, query, vs, visitor, description, AutoClose.TRUE);
    }

    private <T> T wrapPreparedStatement(final Connection c,
            final FinalSQLString query, final Object[] vs, PreparedStatementVisitor<T> visitor, String description,
            AutoClose autoClose) throws PalantirSqlException, PalantirInterruptedException {
        SqlTimer.Handle timerKey = getSqlTimer().start(description, query.getKey(), query.getQuery());
        PreparedStatement ps = null;

        try {
            ps = BasicSQLUtils.runUninterruptably(new Callable<PreparedStatement>() {
                @Override
                public PreparedStatement call() throws Exception {
                    return createPreparedStatement(c, query.getQuery(), vs);
                }
            }, "SQL createPreparedStatement", c);
            return visitor.visit(ps);
        } catch (PalantirSqlException sqle) {
            throw wrapSQLExceptionWithVerboseLogging(sqle, query.getQuery(), vs);
        } finally {
            closeSilently(ps, autoClose);
            timerKey.stop();
        }
    }

    static PalantirSqlException handleInterruptions(long startTime,
            ExecutionException ee) throws PalantirSqlException {
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

    public static PalantirSqlException handleInterruptions(long startTime,
                                                           SQLException cause) throws PalantirSqlException {
        SqlLoggers.SQL_EXCEPTION_LOG.debug("Caught SQLException", cause);

        String message = cause.getMessage().trim();
        //check for oracle and postgres
        if (!message.contains(ORACLE_CANCEL_ERROR) && !message.contains(POSTGRES_CANCEL_ERROR)) {
            throw PalantirSqlException.create(cause);
        }
        String elapsedTime = "N/A";
        if (startTime > 0) {
           elapsedTime = String.valueOf(System.currentTimeMillis() - startTime); //$NON-NLS-1$
        }
        SqlLoggers.CANCEL_LOGGER.info(
                "We got an execution exception that was an interrupt, most likely from someone " //$NON-NLS-1$
                + "incorrectly ignoring an interrupt. "
                + "Elapsed time: {}\n"
                + "Error message: {} "
                + "Error code: {} "
                + "Error state: {} "
                + "Error cause: {}",
                elapsedTime,
                cause.getMessage(),  //$NON-NLS-1$
                cause.getErrorCode(),  //$NON-NLS-1$
                cause.getSQLState(),  //$NON-NLS-1$
                cause.getNextException(),  //$NON-NLS-1$
                new Exception("Just for a stack trace"));
        Thread.currentThread().interrupt();
        throw new PalantirInterruptedException("SQL call interrupted", cause); //$NON-NLS-1$
    }

    public SqlTimer getSqlTimer() {
        return getSqlConfig().getSqlTimer();
    }

    protected PreparedStatement execute(final Connection c, final FinalSQLString sql,
            final Object vs[], final AutoClose autoClose) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL execution query: {}", sql.getQuery());
        }
        return BasicSQLUtils.runUninterruptably (new Callable<PreparedStatement>() {
            @Override
            public PreparedStatement call() throws PalantirSqlException {
                return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<PreparedStatement>() {
                    @Override
                    public PreparedStatement visit(PreparedStatement ps) throws PalantirSqlException {
                        PreparedStatements.execute(ps);
                        return ps;
                    }
                }, "execute", autoClose); //$NON-NLS-1$
            }
        }, sql.toString(), c);
    }

    protected boolean selectExistsInternal(final Connection c,
            final FinalSQLString sql, Object... vs) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectExistsInternal query: {}", sql.getQuery());
        }
        return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<Boolean>() {
            @Override
            public Boolean visit(PreparedStatement ps) throws PalantirSqlException {
                PreparedStatements.setMaxRows(ps, 1);

                return runCancellably(ps, new ResultSetVisitor<Boolean>() {
                    @Override
                    public Boolean visit(ResultSet rs) throws PalantirSqlException {
                        return ResultSets.next(rs);
                    }
                }, sql, null);
            }
        }, "selectExists"); //$NON-NLS-1$
    }

    protected int selectIntegerInternal(final Connection c,
            final FinalSQLString sql, Object... vs) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectIntegerInternal query: {}", sql.getQuery());
        }
        return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<Integer>() {
            @Override
            public Integer visit(PreparedStatement ps) throws PalantirSqlException {
                return runCancellably(ps, new ResultSetVisitor<Integer>() {
                    @Override
                    public Integer visit(ResultSet rs) throws PalantirSqlException {
                        if (ResultSets.next(rs)) {
                            int ret = ResultSets.getInt(rs, 1);
                            if (ResultSets.next(rs)) {
                                SqlLoggers.LOGGER.error(
                                        "In selectIntegerInternal more than one row was returned for query: {}",
                                        sql); //$NON-NLS-1$
                                assert false : "Found more than one row in SQL#selectInteger"; //$NON-NLS-1$
                            }
                            return ret;
                        }

                        // indicate failure
                        throw PalantirSqlException.create("No rows returned."); //$NON-NLS-1$
                    }}, sql, null);
            }
        }, "selectInteger"); //$NON-NLS-1$
    }

    protected Long selectLongInternal(final Connection c, final FinalSQLString sql, Object vs[], final Long defaultVal,
            final boolean useBrokenBehaviorWithNullAndZero) throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL selectLongInternal query: {}", sql.getQuery());
        }
        return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<Long>() {
            @Override
            public Long visit(PreparedStatement ps) throws PalantirSqlException {
                return runCancellably(ps, new ResultSetVisitor<Long>() {
                    @Override
                    public Long visit(ResultSet rs) throws PalantirSqlException {
                        if (ResultSets.next(rs)) {
                            Long ret = null;
                            if (ResultSets.getObject(rs, 1) != null) {
                                ret = ResultSets.getLong(rs, 1);
                            }

                            if (ResultSets.next(rs)) {
                                SqlLoggers.LOGGER.error(
                                        "In selectLongInternal more than one row was returned for query: {}",
                                        sql); //$NON-NLS-1$
                                assert false : "Found more than one row in SQL#selectLong"; //$NON-NLS-1$
                            }

                            if (ret != null) {
                                return ret;
                            } else if (useBrokenBehaviorWithNullAndZero) {
                                SqlLoggers.LOGGER.error(
                                        "In selectLongInternal null was returned in the one row for this query: {}",
                                        sql); //$NON-NLS-1$
                                assert false : "If this case is hit, it is a programming error. "
                                        + "You should use a default value."; //$NON-NLS-1$
                                return 0L;
                            }
                        }

                        if (useBrokenBehaviorWithNullAndZero && defaultVal == null) {
                            throw PalantirSqlException.create("No rows returned."); //$NON-NLS-1$
                        } else {
                            return defaultVal;
                        }
                    }
                }, sql, null);
            }
        }, "selectLong"); //$NON-NLS-1$
    }

    protected AgnosticLightResultSet selectLightResultSetSpecifyingDBType(final Connection c,
                                                                          final FinalSQLString sql,
                                                                          Object vs[],
                                                                          final DBType dbType,
                                                                          @Nullable Integer fetchSize)
            throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL light result set selection query: {}", sql.getQuery());
        }

        final ResourceCreationLocation alrsCreationException = new ResourceCreationLocation("This is where the AgnosticLightResultSet wrapper was created"); //$NON-NLS-1$
        PreparedStatementVisitor<AgnosticLightResultSet> preparedStatementVisitor = new PreparedStatementVisitor<AgnosticLightResultSet>() {
            @Override
            public AgnosticLightResultSet visit(final PreparedStatement ps)
                    throws PalantirSqlException {
                final ResourceCreationLocation creationException = new ResourceCreationLocation("This is where the ResultsSet was created", alrsCreationException); //$NON-NLS-1$
                final ResultSetVisitor<AgnosticLightResultSet> resultSetVisitor = new ResultSetVisitor<AgnosticLightResultSet>() {
                    @Override
                    public AgnosticLightResultSet visit(ResultSet rs) throws PalantirSqlException {
                        try {
                            return new AgnosticLightResultSetImpl(
                                    rs,
                                    dbType,
                                    rs.getMetaData(),
                                    ps,
                                    "selectList", //$NON-NLS-1$
                                    sql,
                                    getSqlTimer(),
                                    creationException);
                        } catch (Exception e) {
                            closeSilently(rs);
                            BasicSQLUtils.throwUncheckedIfSQLException(e);
                            throw Throwables.throwUncheckedException(e);
                        }
                    }
                };

                try {
                    return runCancellably(ps, resultSetVisitor, sql, AutoClose.FALSE, fetchSize);
                } catch (Exception e) {
                    closeSilently(ps);
                    BasicSQLUtils.throwUncheckedIfSQLException(e);
                    throw Throwables.throwUncheckedException(e);
                }
            }
        };

        return wrapPreparedStatement(
                c,
                sql,
                vs,
                preparedStatementVisitor,
                "selectList", //$NON-NLS-1$
                AutoClose.FALSE); //don't close the ps
    }

    protected AgnosticResultSet selectResultSetSpecifyingDBType(final Connection c,
            final FinalSQLString sql, Object[] vs, final DBType dbType)
    throws PalantirSqlException, PalantirInterruptedException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL result set query: {}", sql.getQuery());
        }
        return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<AgnosticResultSet>() {
            @Override
            public AgnosticResultSet visit(PreparedStatement ps) throws PalantirSqlException {
                return runCancellably(ps, new ResultSetVisitor<AgnosticResultSet>() {
                    @Override
                    public AgnosticResultSet visit(ResultSet rs) throws PalantirSqlException {
                        List<List<Object>> rvs = new ArrayList<List<Object>>();
                        ResultSetMetaData meta = ResultSets.getMetaData(rs);
                        int columnCount = ResultSets.getColumnCount(meta);
                        Map<String, Integer> columnMap = ResultSets.buildInMemoryColumnMap(meta, dbType);
                        while (ResultSets.next(rs)) {

                            List<Object> row = Lists.newArrayListWithCapacity(columnCount);

                            for (int i=0; i < columnCount; i++) {
                                row.add(ResultSets.getObject(rs, i+1));
                            }

                            rvs.add(row);
                        }

                        return new AgnosticResultSetImpl(rvs, dbType, columnMap);
                    }
                }, sql, null);
            }
        }, "selectList"); //$NON-NLS-1$
    }

    PreparedStatement updateInternal(final Connection c, final FinalSQLString sql, final Object vs[],
            final AutoClose autoClose) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL update interval query: {}", sql.getQuery());
        }
        return BasicSQLUtils.runUninterruptably (new Callable<PreparedStatement>() {
            @Override
                   public PreparedStatement call() throws PalantirSqlException {
         return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<PreparedStatement>() {
                    @Override
                    public PreparedStatement visit(PreparedStatement ps) throws PalantirSqlException {
                        PreparedStatements.execute(ps);
                        return ps;
                    }
                }, "update", autoClose); //$NON-NLS-1$
            }
        }, sql.toString(), c);
    }

    protected void updateMany(final Connection c, final FinalSQLString sql, final Object vs[][])
    throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL update many query: {}", sql.getQuery());
        }
        BasicSQLUtils.runUninterruptably (new Callable<Void>() {
            @Override
            public Void call() throws PalantirSqlException {
                List<BlobHandler> cleanups = Lists.newArrayList();
                PreparedStatement ps = null;
                SqlTimer.Handle timerKey = getSqlTimer().start("updateMany(" + vs.length + ")", sql.getKey(), sql.getQuery()); //$NON-NLS-1$ //$NON-NLS-2$
                try {
                    ps = c.prepareStatement(sql.getQuery());
                    for (int i=0; i < vs.length; i++) {
                        for (int j=0; j < vs[i].length; j++) {
                            Object obj = vs[i][j];
                            BlobHandler cleanup = setObject(c, ps, j+1, obj);
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
                            SqlLoggers.LOGGER.error("failed to free temp blob", e); //$NON-NLS-1$
                        }
                    }
                }
                return null;
            }
        }, sql.toString(), c);
    }

    protected int insertOneCountRowsInternal(final Connection c,
            final FinalSQLString sql, final Object... vs) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL insert one count rows internal query: {}", sql.getQuery());
        }
        return BasicSQLUtils.runUninterruptably (new Callable<Integer>() {
            @Override
            public Integer call() throws PalantirSqlException {
                return wrapPreparedStatement(c, sql, vs, new PreparedStatementVisitor<Integer>() {
                    @Override
                    public Integer visit(PreparedStatement ps) throws PalantirSqlException {
                        PreparedStatements.execute(ps);
                        return PreparedStatements.getUpdateCount(ps);
                    }
                }, "insertOne"); //$NON-NLS-1$
            }
        }, sql.toString(), c);
    }

    public boolean insertMany(final Connection c, final FinalSQLString sql, final Object vs[][]) throws PalantirSqlException {
        if (SqlLoggers.LOGGER.isTraceEnabled()) {
            SqlLoggers.LOGGER.trace("SQL insert many query: {}", sql.getQuery());
        }
        return BasicSQLUtils.runUninterruptably (new Callable<Boolean>() {
            @Override
            public Boolean call() throws PalantirSqlException {
                int[] inserted = null;
                PreparedStatement ps = null;

                SqlTimer.Handle timerKey = getSqlTimer().start("insertMany(" + vs.length + ")", sql.getKey(), sql.getQuery()); //$NON-NLS-1$ //$NON-NLS-2$
                List<BlobHandler> cleanups = Lists.newArrayList();
                try {
                    ps = c.prepareStatement(sql.getQuery());
                    for (int i=0; i < vs.length; i++) {
                        for (int j=0; j < vs[i].length; j++) {
                            Object obj = vs[i][j];
                            BlobHandler cleanup = setObject(c, ps, j+1, obj);
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
                            SqlLoggers.LOGGER.error("failed to free temp blob", e); //$NON-NLS-1$
                        }
                    }
                }
                if (inserted == null || inserted.length != vs.length) {
                    assert false;
                    return false;
                }
                for (int numInsertedForRow : inserted) {
                    if (numInsertedForRow == Statement.EXECUTE_FAILED) {
                        assert DBType.getTypeFromConnection(c) != DBType.ORACLE : "numInsertedForRow: " + numInsertedForRow; //$NON-NLS-1$
                        return false;
                    }
                }
                return true;
            }
        }, sql.toString(), c);
    }

    protected int updateCountRowsInternal(Connection c, FinalSQLString sql,
            Object... vs) throws PalantirSqlException {
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
            public ArrayHandler createStructArray(String structType,
                                                  String arrayType,
                                                  List<Object[]> elements) {
               throw new UnsupportedOperationException();
            }

            @Override
            public BlobHandler createBlob(Connection c) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
