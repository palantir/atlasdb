package com.palantir.atlasdb.rdbms.api.service;

import java.sql.SQLException;

import javax.annotation.CheckForNull;

import com.palantir.util.Nullable;

/**
 * Represents a connection to an RDBMS.  The raw connection
 * is hidden by the interface.  If an error occurs during
 * execution of any statements using this connection and
 * auto-commit is turned off (meaning this connection is
 * running a transaction), then the transaction will
 * automatically be aborted by the AtlasRdbms, and the
 * error will be available on this connection's getError()
 * method.
 * @author mharris
 *
 */
public interface AtlasRdbmsConnection {

    /**
     * Executes the specified sql query, optionally with parameters
     * specified, and returns a result based on the provided
     * result set handler.
     *
     * The sql string should contain a number of '?'
     * characters equal to the number of extra params provided to
     * this method.  This method will prepare a query by filling
     * in each question mark in the order it appears in the sql
     * string with the provided params in the order they are
     * provided.
     *
     * The sql must be a SELECT statement.  If it is not, a
     * SQLException will be thrown.
     *
     * @param sql The sql to execute
     * @param rsh The result set handler to use to convert the result
     * set returned by the RDBMS to an object
     * @param params Parameters to the sql query, which will fill in
     * '?' characters in the query string.
     * @return The value returned by the specified result set handler
     * @throws SQLException if a DB error occurs
     */
    @CheckForNull
    <T> T query(String sql, AtlasRdbmsResultSetHandler<T> rsh, Object... params) throws SQLException;


    /**
     * Executes the specified sql query, optionally with parameters
     * specified, and returns the number of rows affected by the query
     *
     * The sql string should contain a number of '?'
     * characters equal to the number of extra params provided to
     * this method.  This method will prepare a query by filling
     * in each question mark in the order it appears in the sql
     * string with the provided params in the order they are
     * provided.
     *
     * The sql must be an INSERT, UPDATE, or DELETE statement.
     * If it is not, a SQLException will be thrown.
     *
     * @param sql The sql to execute
     * @param params Parameters to the sql query, which will fill in
     * '?' characters in the query string.
     * @return The number of rows affected by the query
     * @throws SQLException if a DB error occurs
     */
    int update(String sql, Object... params) throws SQLException;

    /**
     * Executes the specified sql query once per entry in the
     * params array.  This has the same effect as the following
     * code:
     *
     * int[] rowsAffected = new int[params.length];
     * for (int i = 0; i < params.length; i++) {
     *     rowsAffected[i] = update(sql, params[i]);
     * }
     * return rowsAffected
     *
     * except that the queries are sent to the underlying RDBMS
     * in batch, so it avoids params.length - 1 round-trips to
     * the RDBMS.  It is strongly recommended to use this method
     * rather than update() when you want to execute the same
     * query multiple times with different parameters.
     *
     * The sql string should contain a number of '?'
     * characters equal to the size of the 2nd dimension of the
     * params array.  This method will prepare a query by filling
     * in each question mark in the order it appears in the sql
     * string with the provided params in the order they are
     * provided for each entry in the params array.
     *
     * The sql must be an INSERT, UPDATE, or DELETE statement.
     * If it is not, a SQLException will be thrown.
     *
     * @param sql The sql to execute
     * @param params Parameters to the sql query, which will fill in
     * '?' characters in the query string.
     * @return An array of ints, each representing the number
     * of rows affected by the respective query.
     * @throws SQLException if a DB error occurs
     */
    int[] batch(String sql, Object[][] params) throws SQLException;

    /**
     * If a DB error occurs during any operations performed on
     * this connection, and this connection has auto-commit
     * turned off, meaning it is running in a transaction, then
     * the transaction will automatically be aborted when this
     * connection is closed.  This method will return the
     * last DB error to occur on this connection.
     * @return The last DB error to occur on this connection,
     * or Nullable.absent() if no such error has occurred.
     */
    Nullable<SQLException> getError();

    /**
     * Whether this connection has auto-commit turned on.
     * This connection is running in a transaction if and
     * only if auto-commit is turned off
     * @return True if this transaction has auto-commit
     * turned on, false otherwise.
     * @throws SQLException If an error occurs while checking
     * auto-commit status
     */
    boolean isAutoCommit() throws SQLException;
}
