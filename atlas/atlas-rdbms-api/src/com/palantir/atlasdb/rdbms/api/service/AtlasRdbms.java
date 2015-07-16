package com.palantir.atlasdb.rdbms.api.service;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.CheckForNull;

import com.palantir.util.Nullable;

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

    /**
     * Gets the current version of the schema of this RDBMS
     * @return The current version of the schema
     * @throws SQLException If a DB error occurs
     */
    AtlasRdbmsSchemaVersion getDbSchemaVersion() throws SQLException;

    /**
     * Gets a system property from this RDBMS
     * @param property The name of a system property to retrieve
     * @return The value of the specified system property, wrapped
     * in a Nullable.  If this property does not exist, then
     * this Nullable will return false from <code>isPresent</code>
     * @throws SQLException If a DB error occurs
     */
    Nullable<String> getDbSystemProperty(String property) throws SQLException;

    /**
     * Sets the value of a system property
     * @param property The name of the system property to set
     * @param value The value of the system property to set
     * @return The previous value of the specified system property,
     * wrapped in a Nullable.  If this property did not exist, then
     * this Nullable will return false from <code>isPresent</code>
     * @throws SQLException If a DB error occurs
     */
    Nullable<String> setDbSystemProperty(String property, String value) throws SQLException;

    /**
     * Performs a schema migration on this RDBMS.  The current
     * schema version of this RDBMS must match the provided
     * expectedCurrentVersion.  Otherwise, an
     * <code>AtlasRdbmsSchemaVersionException</code> will be thrown
     * by this method.  The versionAfterMigration must also be
     * larger than the expectedCurrentVersion.  This can be tested
     * with the <code>compareTo</code> method of <code>AtlasRdbmsSchemaVersion</code>
     *
     * After this method is finished executing, either no change
     * will be made and an exception will be thrown, or all of
     * the provided migrationSqlStatements will be executed in
     * order on this RDBMS and the schema version will be set
     * to versionAfterMigration.
     *
     * @param expectedCurrentVersion The schema version the client thinks
     * this AtlasRdbms has
     * @param versionAfterMigration The version to set the schema to
     * after this migration is complete
     * @param migrationSqlStatements The sql statements to run to
     * perform this migration
     * @throws AtlasRdbmsSchemaVersionMismatchException If the schema
     * version of this AtlasRdbms differs from what the client expects
     * @throws AtlasRdbmsIllegalMigrationException If the versionAfterMigration
     * is smaller than the current version of the schema
     * @throws SQLException If a DB error occurs
     */
    void performSchemaMigration(AtlasRdbmsSchemaVersion expectedCurrentVersion, AtlasRdbmsSchemaVersion versionAfterMigration,
                                List<String> migrationSqlStatements) throws AtlasRdbmsSchemaVersionMismatchException,
                                AtlasRdbmsIllegalMigrationException, SQLException;
}
