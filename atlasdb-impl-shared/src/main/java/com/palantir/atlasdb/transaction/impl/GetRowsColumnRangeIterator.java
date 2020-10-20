/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterators;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

final class GetRowsColumnRangeIterator extends AbstractIterator<Iterator<Map.Entry<Cell, byte[]>>> {

    private final BatchSizeIncreasingIterator<Map.Entry<Cell, Value>> batchIterator;
    private final Runnable batchValidationStep;
    private final PostFilterer postFilterer;

    private boolean firstBatchReturned = false;
    private boolean lastBatchReached = false;

    GetRowsColumnRangeIterator(
            BatchSizeIncreasingIterator<Map.Entry<Cell, Value>> batchIterator,
            Runnable batchValidationStep,
            PostFilterer postFilterer) {
        this.batchIterator = batchIterator;
        this.batchValidationStep = batchValidationStep;
        this.postFilterer = postFilterer;
    }

    /**
     * The iterator returned does not invoke the {@code batchValidationStep} for the first batch of calls as it is
     * assumed to be prefetched from its caller and the {@code batchValidationStep} can run where this is called.
     * <p>
     * If a batch returned by the internal iterator is empty or has size smaller than the batch size specified in
     * {@code columnRangeSelection}, that batch will be considered the last batch, and there will be no further
     * invocations of {@code batchValidationStep}.
     * <p>
     * This <em>may</em> request more elements than the specified batch hint inside {@code columnRangeSelection} if
     * there is detection of many deleted values.
     */
    public static Iterator<Map.Entry<Cell, byte[]>> iterator(
            BatchProvider<Map.Entry<Cell, Value>> batchProvider,
            RowColumnRangeIterator rawIterator,
            BatchColumnRangeSelection columnRangeSelection,
            Runnable batchValidationStep,
            PostFilterer postFilterer) {
        BatchSizeIncreasingIterator<Map.Entry<Cell, Value>> batchIterator = new BatchSizeIncreasingIterator<>(
                batchProvider, columnRangeSelection.getBatchHint(), ClosableIterators.wrap(rawIterator));
        GetRowsColumnRangeIterator postFilteredIterator =
                new GetRowsColumnRangeIterator(batchIterator, batchValidationStep, postFilterer);
        return Iterators.concat(postFilteredIterator);
    }

    @Override
    protected Iterator<Map.Entry<Cell, byte[]>> computeNext() {
        try {
            if (lastBatchReached) {
                return endOfData();
            }

            BatchSizeIncreasingIterator.BatchResult<Map.Entry<Cell, Value>> batchResult = batchIterator.getBatch();
            Map<Cell, Value> raw = ImmutableMap.copyOf(batchResult.batch());

            if (firstBatchReturned) {
                batchValidationStep.run();
            }

            if (batchResult.isLastBatch()) {
                lastBatchReached = true;
            }

            if (raw.isEmpty()) {
                return Collections.emptyIterator();
            }

            SortedMap<Cell, byte[]> postFiltered = ImmutableSortedMap.copyOf(postFilterer.postFilter(raw));
            batchIterator.markNumResultsNotDeleted(postFiltered.size());
            return postFiltered.entrySet().iterator();
        } finally {
            firstBatchReturned = true;
        }
    }

    @FunctionalInterface
    interface PostFilterer {
        Collection<Map.Entry<Cell, byte[]>> postFilter(Map<Cell, Value> rawResults);
    }
}
