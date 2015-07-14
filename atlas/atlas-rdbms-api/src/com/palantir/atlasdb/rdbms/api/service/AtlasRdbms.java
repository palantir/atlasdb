package com.palantir.atlasdb.rdbms.api.service;

import javax.annotation.CheckForNull;

/**
 * Represents a relational database management service
 * @author mharris
 *
 */
public interface AtlasRdbms {

    /**
     * Executes the provided callable, which is given a connection
     * to the underlying RDBMS that has auto-commit turned on,
     * meaning it is not running in a transaction.  The connection
     * is safely closed before this method exits.
     *
     * @param dbCallable A function to execute with a connection to the underlying RDBMS
     *
     * @return The value returned by the provided callable
     *
     * @throws AtlasRdbmsExecutionException If an exception is thrown
     * by the callable, then this exception will be wrapped in an
     * AtlasRdbmsExecutionException.  It is recommended to throw whatever
     * exception is sensible from the callable, then have the caller of
     * this method unwrap that exception from the AtlasRdbmsExecutionException
     */
    @CheckForNull
    <T> T runWithDbConnection(final AtlasRdbmsConnectionCallable<T> callable) throws AtlasRdbmsExecutionException;

    /**
     * Executes the provided callable, which is given a connection
     * to the underlying RDBMS that has auto-commit turned on,
     * meaning it is not running in a transaction.  The connection
     * is safely closed before this method exits.
     *
     * @param dbCallable A function to execute with a connection to the underlying RDBMS
     *
     * @return The value returned by the provided callable
     *
     * @throws AtlasRdbmsExecutionException If an exception is thrown
     * by the callable, then this exception will be wrapped in an
     * AtlasRdbmsExecutionException.  It is recommended to throw whatever
     * exception is sensible from the callable, then have the caller of
     * this method unwrap that exception from the AtlasRdbmsExecutionException
     *
     * @throws K If a K is thrown by the callable, this method will throw
     * that K instead of wrapping it in an AtlasRdbmsExecutionException
     */
    @CheckForNull
    <T, K extends Exception> T runWithDbConnection(final AtlasRdbmsConnectionCheckedCallable<T, K> callable)
            throws AtlasRdbmsExecutionException, K;

    /**
     * Executes the provided callable, which is given a connection
     * to the underlying RDBMS that has auto-commit turned on,
     * meaning it is not running in a transaction.  The connection
     * is safely closed before this method exits.
     *
     * @param dbCallable A function to execute with a connection to the underlying RDBMS
     *
     * @return The value returned by the provided callable
     *
     * @throws AtlasRdbmsExecutionException If an exception is thrown
     * by the callable, then this exception will be wrapped in an
     * AtlasRdbmsExecutionException.  It is recommended to throw whatever
     * exception is sensible from the callable, then have the caller of
     * this method unwrap that exception from the AtlasRdbmsExecutionException
     */
    @CheckForNull
    <T> T runWithDbConnectionInTransaction(final AtlasRdbmsConnectionCallable<T> callable) throws AtlasRdbmsExecutionException;

    /**
     * Executes the provided callable, which is given a connection
     * to the underlying RDBMS that has auto-commit turned on,
     * meaning it is not running in a transaction.  The connection
     * is safely closed before this method exits.
     *
     * @param dbCallable A function to execute with a connection to the underlying RDBMS
     *
     * @return The value returned by the provided callable
     *
     * @throws AtlasRdbmsExecutionException If an exception is thrown
     * by the callable, then this exception will be wrapped in an
     * AtlasRdbmsExecutionException.  It is recommended to throw whatever
     * exception is sensible from the callable, then have the caller of
     * this method unwrap that exception from the AtlasRdbmsExecutionException
     *
     * @throws K If a K is thrown by the callable, this method will throw
     * that K instead of wrapping it in an AtlasRdbmsExecutionException
     */
    @CheckForNull
    <T, K extends Exception> T runWithDbConnectionInTransaction(final AtlasRdbmsConnectionCheckedCallable<T, K> callable)
            throws AtlasRdbmsExecutionException, K;
}
