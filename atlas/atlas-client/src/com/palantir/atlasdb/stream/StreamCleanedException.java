package com.palantir.atlasdb.stream;


/**
 * This may be thrown if trying to mark a stream that no longer exists because it was cleaned up due to having no references.
 *
 * @author carrino
 */
public class StreamCleanedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StreamCleanedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamCleanedException(String message) {
        super(message);
    }

    public StreamCleanedException(Throwable cause) {
        super(cause);
    }

}
