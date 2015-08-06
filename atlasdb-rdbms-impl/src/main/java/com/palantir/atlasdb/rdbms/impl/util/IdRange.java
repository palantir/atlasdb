package com.palantir.atlasdb.rdbms.impl.util;

import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

//NOT IMMUTABLE
@NotThreadSafe
public class IdRange implements Iterable<Long> {

    private long start;
    private final long endExclusive;

    public IdRange(long start, long endExclusive) {
        Preconditions.checkArgument(endExclusive >= start, "Range end must be at or after range start");
        this.start = start;
        this.endExclusive = endExclusive;
    }

    public long size() {
        return endExclusive - start;
    }

    public IdRange strip(long numIds) {
        Preconditions.checkArgument(numIds >= 0, "Cannot strip a negative number of IDs");
        Preconditions.checkArgument(numIds <= size(), "Cannot strip more IDs than are in this range");
        IdRange toReturn = new IdRange(start, start + numIds);
        start += numIds;
        return toReturn;
    }

    @Override
    public Iterator<Long> iterator() {
        return new IdRangeIterator();
    }

    private class IdRangeIterator implements Iterator<Long> {

        private long next;

        public IdRangeIterator() {
            next = start;
        }

        @Override
        public boolean hasNext() {
            return next < endExclusive;
        }

        @Override
        public Long next() {
            if (!hasNext()) {
                throw new IllegalStateException("No ids remaining");
            }
            return next++;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
