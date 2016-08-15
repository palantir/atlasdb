package com.palantir.atlasdb.sql.jdbc.results.parsed;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LocalParsedRowResultsIterator implements ParsedRowResultsIterator {
    private final Iterator<ParsedRowResult> iter;
    private ParsedRowResult curResult;
    private boolean closed;

    public LocalParsedRowResultsIterator(Iterator<ParsedRowResult> iter) {
        this.iter = iter;
        this.curResult = null;
        this.closed = false;
    }

    @Override
    public ParsedRowResult next() {
        if (isClosed()) {
            throw new NoSuchElementException("Results iterator is closed.");
        }
        if (hasNext()) {
            curResult = iter.next();
            return curResult;
        }
        throw new NoSuchElementException("Out of results.");
    }

    @Override
    public ParsedRowResult current() {
        if (isClosed()) {
            return null;
        }
        return curResult;
    }

    @Override
    public boolean hasNext() {
        if (iter == null || isClosed()) {
            return false;
        }
        return iter.hasNext();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

}
