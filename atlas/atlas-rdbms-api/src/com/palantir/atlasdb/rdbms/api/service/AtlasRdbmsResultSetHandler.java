package com.palantir.atlasdb.rdbms.api.service;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.CheckForNull;

/**
 * Implementations of this interface convert ResultSets into other objects.
 * @author mharris
 */
public interface AtlasRdbmsResultSetHandler<T> {

    /**
     * Turn the <code>ResultSet</code> into an Object.
     *
     * @param rs The <code>ResultSet</code> to handle.  It has not been touched
     * before being passed to this method.
     *
     * @return An Object initialized with <code>ResultSet</code> data. It is
     * legal for implementations to return <code>null</code>
     *
     * @throws SQLException if a database access error occurs
     */
    @CheckForNull
    public T handle(ResultSet rs) throws SQLException;

}
