package com.palantir.atlasdb.rdbms.api.service;

import javax.annotation.CheckForNull;


/**
 * Represents a function that returns an object
 * by using a connection to an RDBMS
 * @author mharris
 */
public interface AtlasRdbmsConnectionCallable<T> {

    /**
     * A function to execute using a connection
     * @param c A connection to the underlying RDBMS
     * @return Any object
     * @throws Exception This method is allowed to throw
     * any exception
     */
    @CheckForNull
    T call(AtlasRdbmsConnection c) throws Exception;
}
