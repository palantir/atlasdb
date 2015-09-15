package com.palantir.atlasdb.keyvalue.partition.exception;

public class EndpointVersionTooOldException extends RuntimeException {

    private static final long serialVersionUID = 6421197986192185450L;

    public EndpointVersionTooOldException() {
        super();
    }

    public EndpointVersionTooOldException(String message) {
        super(message);
    }

    public EndpointVersionTooOldException(String message, Throwable cause) {
        super(message, cause);
    }

}
