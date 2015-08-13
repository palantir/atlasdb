package com.palantir.atlasdb.rdbms.api.service;

/**
 * An Exception that is meant to wrap any exception that
 * occurs during execution of DB operations.  This is
 * similar to <code>java.util.concurrent.ExecutionException</code>
 * @author mharris
 *
 */
public final class AtlasRdbmsExecutionException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an AtlasRdbmsExecutionException with the specified detail
     * message and cause.
     *
     * @param  message the detail message
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method)
     */
    public AtlasRdbmsExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an tlasRdbmsExecutionException with the specified cause.
     * The detail message will be set to cause.toString() if cause is non-null
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method)
     */
    public AtlasRdbmsExecutionException(Throwable cause) {
        super(cause);
    }

}
