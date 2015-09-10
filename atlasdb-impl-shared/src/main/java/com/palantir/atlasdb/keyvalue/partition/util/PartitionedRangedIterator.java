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

    final SortedSet<ConsistentRingRangeRequest> ranges;
    Set<ClosablePeekingIterator<RowResult<T>>> currentRangeIterators;
    Iterator<ConsistentRingRangeRequest> currentRange;
    private PeekingIterator<RowResult<T>> rowIterator = Iterators.peekingIterator(Collections.<RowResult<T>> emptyIterator());

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
        // TODO: if (currentRangeIterators.isEmpty()) { throw whatever; }
        rowIterator = Iterators.<RowResult<T>> peekingIterator(Iterators.mergeSorted(
                currentRangeIterators,
                RowResult.<T>getOrderingByRowName()));
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
