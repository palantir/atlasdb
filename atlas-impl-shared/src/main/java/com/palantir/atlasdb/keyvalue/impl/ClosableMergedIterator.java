// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.Validate;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.ForwardingClosableIterator;
import com.palantir.common.collect.IteratorUtils;
import com.palantir.util.Pair;

/* package */ class ClosableMergedIterator<T> extends AbstractIterator<RowResult<T>> implements ClosableIterator<RowResult<T>> {
    private final RangeRequest request;
    private final ClosableIterator<RowResult<T>> primaryIter;
    private final Function<RangeRequest, ClosableIterator<RowResult<T>>> secondaryResultProducer;
    private ClosableIterator<RowResult<T>> mergedIter = ClosableIterators.wrap(Iterators.<RowResult<T>>emptyIterator());
    private byte[] nextSecondaryStart;
    private boolean primaryCompleted = false;

    public ClosableMergedIterator(RangeRequest request,
                                  ClosableIterator<RowResult<T>> primaryIter,
                                  Function<RangeRequest, ClosableIterator<RowResult<T>>> secondaryResultProducer) {
        this.request = request;
        this.primaryIter = primaryIter;
        this.nextSecondaryStart = request.getStartInclusive();
        this.secondaryResultProducer = secondaryResultProducer;
    }

    @Override
    protected RowResult<T> computeNext() {
        if (mergedIter.hasNext()) {
            return mergedIter.next();
        } else if (primaryCompleted) {
            return endOfData();
        }
        int batchSize = BatchingVisitables.DEFAULT_BATCH_SIZE - 1;
        if (request.getBatchHint() != null) {
            batchSize = Math.max(request.getBatchHint() - 1, 1);
        }
        List<RowResult<T>> batch = Lists.newArrayList(Iterators.limit(primaryIter, batchSize));
        byte[] secondaryEndRow;
        if (batch.size() == batchSize) {
            secondaryEndRow = batch.get(batchSize-1).getRowName();
            if (RangeRequests.isTerminalRow(request.isReverse(), secondaryEndRow)) {
                secondaryEndRow = PtBytes.EMPTY_BYTE_ARRAY;
            } else {
                secondaryEndRow = RangeRequests.getNextStartRow(request.isReverse(), secondaryEndRow);
            }
        } else {
            Validate.isTrue(!primaryIter.hasNext());
            secondaryEndRow = request.getEndExclusive();
            primaryCompleted = true;
        }

        RangeRequest.Builder requestBuilder = request.getBuilder().startRowInclusive(nextSecondaryStart).endRowExclusive(secondaryEndRow);
        nextSecondaryStart = secondaryEndRow;
        ClosableIterator<RowResult<T>> secondaryIter = secondaryResultProducer.apply(requestBuilder.build());
        mergedIter = new ClosableMergedIterator.ClosableMergedBatchIterator<T>(batch.iterator(), secondaryIter, request.isReverse());
        if (!mergedIter.hasNext()) {
            return endOfData();
        }
        return mergedIter.next();
    }

    @Override
    public void close() {
        primaryIter.close();
        mergedIter.close();
    }

    private static class ClosableMergedBatchIterator<T> extends ForwardingClosableIterator<RowResult<T>> {
        private final ClosableIterator<RowResult<T>> secondaryIter;
        private final Iterator<RowResult<T>> mergedIter;

        public ClosableMergedBatchIterator(Iterator<RowResult<T>> primaryIter,
                                           ClosableIterator<RowResult<T>> secondaryIter,
                                           boolean isReversed) {
            this.secondaryIter = secondaryIter;
            this.mergedIter = mergeIterators(primaryIter, secondaryIter, isReversed);
        }

        @Override
        protected ClosableIterator<RowResult<T>> delegate() {
            return ClosableIterators.wrap(mergedIter);
        }

        @Override
        public void close() {
            secondaryIter.close();
        }
    }

    static <T> Iterator<RowResult<T>> mergeIterators(Iterator<RowResult<T>> writeIter,
            Iterator<RowResult<T>> readIter,
            boolean isReversed) {
        Ordering<RowResult<T>> comparator = RowResult.getOrderingByRowName();
        if (isReversed) {
            comparator = comparator.reverse();
        }
        return IteratorUtils.mergeIterators(
                writeIter,
                readIter,
                comparator,
                new Function<Pair<RowResult<T>, RowResult<T>>, RowResult<T>>() {
                    @Override
                    public RowResult<T> apply(Pair<RowResult<T>, RowResult<T>> rows) {
                        RowResult<T> writeResult = rows.getLhSide();
                        RowResult<T> readResult = rows.getRhSide();
                        for (Entry<byte[], T> entry : writeResult.getColumns().entrySet()) {
                            T readValue = readResult.getColumns().get(entry.getKey());
                            if (readValue instanceof Value) {
                                long writeTimestamp = ((Value)entry.getValue()).getTimestamp();
                                long readTimestamp = ((Value)readValue).getTimestamp();
                                if (writeTimestamp < readTimestamp) {
                                    throw new IllegalArgumentException("The read only kvs has a row with timestamp " + writeTimestamp +
                                            ", while the same row in the writing kvs has timestamp. " + readTimestamp);
                                }
                            }
                        }
                        // Order is important here, overwrite results from the
                        // secondary tier with results from the primary tier.
                        return RowResults.merge(readResult, writeResult);
                    }
                });
    }
}
