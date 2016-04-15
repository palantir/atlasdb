package com.palantir.nexus.db.sql;

import java.sql.SQLException;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;

import com.palantir.nexus.db.chunked.ChunkedRowVisitor;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.util.Visitor;

public interface PalantirSqlConnectionRunner {
    public static abstract class SequenceEnabledSqlRunnerCheckedException<T, K extends Exception> {
        public abstract T call(PalantirSequenceEnabledSqlConnection conn) throws K;

        private Class<K> clazz;

        protected SequenceEnabledSqlRunnerCheckedException(@Nonnull Class<K> clazz) {
            Validate.notNull(clazz);
            Validate.isTrue(!SQLException.class.isAssignableFrom(clazz));
            this.clazz = clazz;
        }

        public Class<K> getExceptionClass() {
            return clazz;
        }
    }


    public static abstract class ImmutableArgumentsSqlRunnerCheckedException<T, K extends Exception> extends SequenceEnabledSqlRunnerCheckedException<T, K> {
        protected ImmutableArgumentsSqlRunnerCheckedException(Class<K> clazz) {
            super(clazz);
        }
    }

    public static abstract class SqlRunnerCheckedException<T, K extends Exception>
            extends SequenceEnabledSqlRunnerCheckedException<T, K> {
        public SqlRunnerCheckedException(@Nonnull Class<K> clazz) {
            super(clazz);
        }

        @Override
        public final T call(PalantirSequenceEnabledSqlConnection conn) throws K {
            return call((PalantirSqlConnection)conn);
        }

        public abstract T call(PalantirSqlConnection conn) throws K;
    }

    public static abstract class SequenceEnabledSqlRunner<T>
            extends SequenceEnabledSqlRunnerCheckedException<T, RuntimeException> {
        public SequenceEnabledSqlRunner() {
            super(RuntimeException.class);
        }

        @Override
        public abstract T call(PalantirSequenceEnabledSqlConnection conn);
    }

    public static abstract class SqlRunner<T> extends SqlRunnerCheckedException<T, RuntimeException>  {
        public SqlRunner() {
            super(RuntimeException.class);
        }

        @Override
        public abstract T call(PalantirSqlConnection conn);
    }

    public static abstract class ImmutableArgumentsSqlRunner<T> extends ImmutableArgumentsSqlRunnerCheckedException<T, RuntimeException> {
        public ImmutableArgumentsSqlRunner() {
            super(RuntimeException.class);
        }

        @Override
        public final T call(PalantirSequenceEnabledSqlConnection conn) {
            return call((PalantirSqlConnection)conn);
        }

        public abstract T call(PalantirSqlConnection conn) ;
    }

    /**
     * Obtains a database connection and internally takes care of connecting and disconnecting.
     * It is reentrant, so that if you call it inside of another invocation, it will re-use the same connection.
     *
     * Runs the callable, passes any SQL or Palantir Exceptions, wraps all other exceptions in PalantirExceptions.
     * If the exception was an InterruptedException, or a SQLException caused by an InterruptedException,
     * this method also interrupts the current thread.
     *
     * @param c the Callable to run with the connection
     * @return the returned Object from the input Callable
     */
    public <T, K extends Exception> T runWithSqlConnection(SequenceEnabledSqlRunnerCheckedException<T, K> call) throws K;
    public <T, K extends Exception> T runWithSqlConnectionInTransaction(SequenceEnabledSqlRunnerCheckedException<T, K> call) throws K;
    public <T, K extends Exception> T runWithSqlConnectionInTransactionWithRetryOnConflict(SequenceEnabledSqlRunnerCheckedException<T, K> call) throws K;

    /**
     * Obtains a database connection and internally takes care of connecting and disconnecting.
     * It is reentrant, so that if you call it inside of another invocation, it will re-use the same connection.
     *
     * Runs the callable, passes any SQL or Palantir Exceptions, wraps all other exceptions in PalantirExceptions.
     * If the exception was an InterruptedException, or a SQLException caused by an InterruptedException,
     * this method also interrupts the current thread.
     *
     * This method will attempt to retry the callable on deadlock.
     *
     * Note that we could avoid deadlocks by doing explicit locking. However,
     * because we expect deadlocks to occur _very_ rarely in practice, the
     * optimistic approach should be more performant than the pessimistic
     * approach to concurrency control.
     *
     * @param c the Callable to run with the connection
     * @return the returned Object from the input Callable
     */
    public <T, K extends Exception> T runWithSqlConnectionRetryOnDeadlock(SequenceEnabledSqlRunnerCheckedException<T, K> call) throws K;

    /**
     * Wraps <code>runWithSqlConnection</code> so that all queries will be
     * run within the same temp table transaction.
     *
     * This method will attempt to retry the callable on deadlock.
     *
     * Note that we could avoid deadlocks by doing explicit locking. However,
     * because we expect deadlocks to occur _very_ rarely in practice, the
     * optimistic approach should be more performant than the pessimistic
     * approach to concurrency control.
     *
     * @param <T> return type of the Callable
     * @param runnable the Callable the run within the temp table transaction
     * @return object returned by the Callable
     */
    public <T, K extends Exception> T runWithSqlConnectionInTransactionRetryOnDeadlock(SequenceEnabledSqlRunnerCheckedException<T, K> call) throws K;

    public void executeAndProcessQueryWithEightTempsIds(Iterable<Object[]> tempIds, RegisteredSQLString sql, Object[] sqlArgs, Visitor<AgnosticLightResultRow> rowVisitor);
    public void executeQueryWithTempsIds(final Iterable<Long> tempIds, final String key, final Object... sqlArgs);
    public void executeQueryWithTempsIds(Iterable<Long> tempIds, RegisteredSQLString sql, Object ... sqlArgs);
    public void executeAndProcessQueryConvertSqlException(RegisteredSQLString sql, Object[] sqlArgs, Visitor<AgnosticLightResultRow> rowVisitor);
    public <T> void executeAndProcessQueryInChunks(RegisteredSQLString sql, Object[] sqlArgs, int chunkSize, ChunkedRowVisitor<T> visitor);

    /**
     * Encapsulates much of the boilerplate code required to execute a SQL query
     * which requires ids to first be loaded into multiple temporary tables.
     *
     * @param tempIds the temp ids to load.  Maps from tempTableName to tempIds iterable.
     * @param key The registred SQL to execute by name
     * @param sqlArgs the SQL bind variables
     * @param rowVisitor the AgnosticLightResultRow.Visitor which processes the results of the query
     */
    public void executeAndProcessQueryWithTempsIds(Map<String, Iterable<Long>> tempIds, String key, Object[] sqlArgs, Visitor<AgnosticLightResultRow> rowVisitor);
    public void executeAndProcessQueryWithTempsIds(Iterable<Long> tempIds, RegisteredSQLString sql, Object[] sqlArgs, Visitor<AgnosticLightResultRow> rowVisitor);

    public <T> void executeAndProcessQueryInChunks(final String sqlKey,
            final Object[] sqlArgs, final int chunkSize,
            final ChunkedRowVisitor<T> visitor);

    public void executeAndProcessQueryWithTempExternalIds(
            Iterable<Object[]> externalIds,
            RegisteredSQLString key,
            Object[] sqlArgs,
            Visitor<AgnosticLightResultRow> rowVisitor);

    public void executeAndProcessQueryWithThreeTempIds(Iterable<Object[]> tempIds,
                                                       RegisteredSQLString key,
                                                       Object[] sqlArgs,
                                                       Visitor<AgnosticLightResultRow> rowVisitor);

    /**
     * Encapsulates much of the boilerplate code required to execute a SQL query
     * which requires ids to first be loaded into a temporary table
     *
     * @param tempIds the temp ids to load
     * @param key The registred SQL to execute by name
     * @param sqlArgs the SQL bind variables
     * @param rowVisitor the AgnosticLightResultRow.Visitor which processes the results of the query
     */
    public void executeAndProcessQueryWithTempsIds(final Iterable<Long> tempIds,
            final String key, final Object[] sqlArgs,
            final Visitor<AgnosticLightResultRow> rowVisitor);
}
