package com.palantir.nexus.db.sql;

import java.io.Closeable;
import java.util.Iterator;

import com.palantir.util.Visitor;

/**
 * This result set only loads one row at a time, and thus provides a
 * low-overhead solution for large queries.  Read the comments about the iterator before
 * using it, because there are non-obvious pitfalls.
 * @author dcohen
 *
 */
public interface AgnosticLightResultSet extends Iterable<AgnosticLightResultRow>, Closeable {
    @Override
    public void close();
    public void visitAndClose(Visitor<? super AgnosticLightResultRow> visitor);
    @Override
    public Iterator<AgnosticLightResultRow> iterator();

    public void setFetchSize(int fetchSize);
}
