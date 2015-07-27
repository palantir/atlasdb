package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.base.ClosableIterator;

public abstract class PartitionedRangedIterator<T> implements ClosableIterator<RowResult<T>> {

    final Multimap<RangeRequest, ClosablePeekingIterator<RowResult<T>>> rangeIterators;
    Iterator<RangeRequest> currentRange;
    protected PeekingIterator<RowResult<T>> rowIterator = Iterators.peekingIterator(Collections.<RowResult<T>> emptyIterator());

    public PartitionedRangedIterator(Multimap<RangeRequest, ClosablePeekingIterator<RowResult<T>>> rangeIterators) {
        this.rangeIterators = rangeIterators;
        this.currentRange = rangeIterators.keySet().iterator();
    }

    protected void prepareNextRange() {
        Preconditions.checkArgument(currentRange.hasNext());
        Preconditions.checkArgument(!rowIterator.hasNext());
        RangeRequest newRange = currentRange.next();
        Collection<ClosablePeekingIterator<RowResult<T>>> newRangeIterators = rangeIterators.get(newRange);
        rowIterator = Iterators.<RowResult<T>>peekingIterator(
                Iterators.mergeSorted(newRangeIterators, RowResultComparator.instance()));
    }

    @Override
    public boolean hasNext() {
        if (!rowIterator.hasNext() && currentRange.hasNext()) {
            prepareNextRange();
        }
        return rowIterator.hasNext();
    }

    @Override
    public abstract RowResult<T> next();

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        for (ClosableIterator<?> it : rangeIterators.values()) {
           it.close();
        }
    }


}
