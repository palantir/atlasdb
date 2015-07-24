package com.palantir.atlasdb.transaction.service;

import java.util.Iterator;

/**
 * Interface for transaction service storing logs.
 * It can be in states - open, when only writing is allowed and closed, when only reading is allowed.
 */
public interface WriteAheadLog extends Iterable<TransactionLogEntry> {
    /**
     * Appends to the log. The log need to be open.
     */
    void append(long startTs, long commitTs);

    /**
     * Changes the state from open to closed.
     */
    void close();

    boolean isClosed();

    long getId();

    /**
     * Returns an iterator that goes through all entries. The log need to be closed.
     */
    @Override
    Iterator<TransactionLogEntry> iterator();
}
