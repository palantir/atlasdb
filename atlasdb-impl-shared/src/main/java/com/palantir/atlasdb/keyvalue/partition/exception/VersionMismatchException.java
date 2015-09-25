package com.palantir.atlasdb.keyvalue.partition.exception;

import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;

public abstract class VersionMismatchException extends TransactionFailedRetriableException {

    private static final long serialVersionUID = 5540601852684261371L;

    public VersionMismatchException(String message) {
        super(message);
    }

    public VersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

}
