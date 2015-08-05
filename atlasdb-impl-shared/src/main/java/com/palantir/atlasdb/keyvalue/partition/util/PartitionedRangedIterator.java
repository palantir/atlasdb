package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;
import com.palantir.common.base.ClosableIterator;

public abstract class PartitionedRangedIterator<T> implements ClosableIterator<RowResult<T>> {

    final SortedSet<ConsistentRingRangeRequest> ranges;
    Set<ClosablePeekingIterator<RowResult<T>>> currentRangeIterators;
    Iterator<ConsistentRingRangeRequest> currentRange;
    private PeekingIterator<RowResult<T>> rowIterator = Iterators.peekingIterator(Collections.<RowResult<T>> emptyIterator());

    public PartitionedRangedIterator(Collection<ConsistentRingRangeRequest> ranges) {
        this.ranges = Sets.newTreeSet(ConsistentRingRangeComparator.instance());
        this.ranges.addAll(ranges);
        this.currentRange = ranges.iterator();
        this.currentRangeIterators = ImmutableSet.of();
    }

    private void prepareNextRange() {
        Preconditions.checkArgument(currentRange.hasNext());
        Preconditions.checkArgument(!getRowIterator().hasNext());
        ConsistentRingRangeRequest newRange = currentRange.next();
        closeCurrentRangeIterators();
        currentRangeIterators = computeNextRange(newRange);
        rowIterator = Iterators.<RowResult<T>> peekingIterator(Iterators.mergeSorted(
                currentRangeIterators,
                RowResultComparator.instance()));
    }

    protected abstract Set<ClosablePeekingIterator<RowResult<T>>> computeNextRange(ConsistentRingRangeRequest range);

    @Override
    public final boolean hasNext() {
        while (!getRowIterator().hasNext() && currentRange.hasNext()) {
            prepareNextRange();
        }
        return getRowIterator().hasNext();
    }

    RowResult<T> cachedResult;
    @Override
    public final RowResult<T> next() {
        RowResult<T> newResult = computeNext();
        if (cachedResult != null) {
            Preconditions.checkState(
                    UnsignedBytes.lexicographicalComparator().compare(
                            cachedResult.getRowName(),
                            newResult.getRowName()
                    ) >= 0,
                    "Row must be non-descending.");
        }
        cachedResult = newResult;
        return cachedResult;
    }

    protected abstract RowResult<T> computeNext();

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        closeCurrentRangeIterators();
    }

    private void closeCurrentRangeIterators() {
        for (ClosableIterator<?> it : currentRangeIterators) {
            it.close();
        }
    }

    protected PeekingIterator<RowResult<T>> getRowIterator() {
        return rowIterator;
    }

}
