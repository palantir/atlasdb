package com.palantir.atlasdb.sql.jdbc.results.parsed;

import java.io.Closeable;
import java.util.Iterator;

public interface ParsedRowResultsIterator extends Iterator<ParsedRowResult>, Closeable {
    ParsedRowResult current();
    boolean isClosed();
}
