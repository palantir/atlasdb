/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.palantir.common.base.ClosableIterator;

public abstract class PartitionedRangedIterator<T> implements ClosableIterator<RowResult<T>> {
    private final SortedSet<ConsistentRingRangeRequest> ranges;

    private Set<ClosablePeekingIterator<RowResult<T>>> currentRangeIterators;
    private Iterator<ConsistentRingRangeRequest> currentRange;
    private PeekingIterator<RowResult<T>> rowIterator = Iterators.peekingIterator(Collections.emptyIterator());
    private RowResult<T> cachedResult; // Used to validate row ordering

    public PartitionedRangedIterator(Collection<ConsistentRingRangeRequest> ranges) {
        this.ranges = Sets.newTreeSet(ConsistentRingRangeRequests.getCompareByStartRow());
        this.ranges.addAll(ranges);
        this.currentRange = ranges.iterator();
        this.currentRangeIterators = ImmutableSet.of();
    }

    private void prepareNextRange() {
        Preconditions.checkState(currentRange.hasNext());
        Preconditions.checkState(!getRowIterator().hasNext());
        ConsistentRingRangeRequest newRange = currentRange.next();
        closeCurrentRangeIterators();
        currentRangeIterators = computeNextRange(newRange);
        Preconditions.checkState(!currentRangeIterators.isEmpty());
        rowIterator = Iterators.peekingIterator(Iterators.mergeSorted(
                currentRangeIterators,
                RowResult.getOrderingByRowName()));
    }

    protected abstract Set<ClosablePeekingIterator<RowResult<T>>> computeNextRange(ConsistentRingRangeRequest range);

    private void prepareNextElement() {
        while (!getRowIterator().hasNext() && currentRange.hasNext()) {
            prepareNextRange();
        }
    }

    @Override
    public final boolean hasNext() {
        prepareNextElement();
        return getRowIterator().hasNext();
    }

    @Override
    public final RowResult<T> next() {
        prepareNextElement();
        Preconditions.checkState(hasNext());

        RowResult<T> newResult = computeNext();

        // Validate the row ordering
        if (cachedResult != null) {
            Preconditions.checkState(
                    UnsignedBytes.lexicographicalComparator().compare(
                            cachedResult.getRowName(),
                            newResult.getRowName()
                    ) <= 0,
                    "Row must be non-descending.");
        }

        cachedResult = newResult;
        return newResult;
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
