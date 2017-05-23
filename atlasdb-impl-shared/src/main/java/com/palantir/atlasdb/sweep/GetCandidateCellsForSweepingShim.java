/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

// The plan is to make getCandidateCellsForSweeping() a method in KeyValueService.
// This class provides a temporary implementation of the new call using existing KVS calls,
// so that we can make this change in several smaller commits.
public class GetCandidateCellsForSweepingShim {
    private final KeyValueService keyValueService;

    public GetCandidateCellsForSweepingShim(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    /**
     * For a given range of rows, returns all candidate cells for sweeping (and their timestamps).
     * Here is the precise definition of a candidate cell:
     * <blockquote>
     *      Let {@code Ts} be {@code request.sweepTimestamp()}<br>
     *      Let {@code Tu} be {@code request.minUncommittedTimestamp()}<br>
     *      Let {@code V} be {@code request.shouldCheckIfLatestValueIsEmpty()}<br>
     *      Let {@code Ti} be set of timestamps in {@code request.timestampsToIgnore()}<br>
     *      <p>
     *      Consider a cell {@code C}. Let {@code Tc} be the set of all timestamps for {@code C} that are strictly
     *      less than {@code Ts}. Let {@code T} be {@code Tc \ Ti} (i.e. the cell timestamps minus the ignored
     *      timestamps).
     *      <p>
     *      Then {@code C} is a candidate for sweeping if and only if at least one of
     *      the following conditions is true:
     *      <ol>
     *          <li> The set {@code T} has more than one element
     *          <li> The set {@code T} contains an element that is greater than or equal to {@code Tu}
     *             (that is, there is a timestamp that can possibly come from an uncommitted or aborted transaction)
     *          <li> The set {@code T} contains {@link Value#INVALID_VALUE_TIMESTAMP}
     *             (that is, there is a sentinel we can possibly clean up)
     *          <li> {@code V} is true and the cell value corresponding to the maximum element of {@code T} is empty
     *             (that is, the latest sweepable value is a 'soft-delete' tombstone)
     *      </ol>
     *
     * </blockquote>
     * This method will scan the semi-open range of rows from the start row specified in the {@code request}
     * to the end of the table. If the given start row name is an empty byte array, the whole table will be
     * scanned.
     * <p>
     * The returned cells will be lexicographically ordered.
     * <p>
     * We return an iterator of lists instead of a "flat" iterator of results so that we preserve the information
     * about batching. The caller can always use Iterators.concat() or similar if this is undesired.
     */
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
                TableReference tableRef,
                CandidateCellForSweepingRequest request) {
        RangeRequest range = RangeRequest.builder()
                .startRowInclusive(request.startRowInclusive())
                .batchHint(request.batchSizeHint().orElse(AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT))
                .build();
        try (ReleasableCloseable<ClosableIterator<RowResult<Value>>> valueResults = new ReleasableCloseable<>(
                    getValues(tableRef, range, request.sweepTimestamp(), request.shouldCheckIfLatestValueIsEmpty()));
             ReleasableCloseable<ClosableIterator<RowResult<Set<Long>>>> tsResults = new ReleasableCloseable<>(
                     keyValueService.getRangeOfTimestamps(tableRef, range, request.sweepTimestamp()))) {
            PeekingIterator<RowResult<Value>> peekingValues = Iterators.peekingIterator(valueResults.get());
            MutableLong numExamined = new MutableLong(0);
            Set<Long> timestampsToIgnore = ImmutableSet.copyOf(Longs.asList(request.timestampsToIgnore()));
            Iterator<List<RowResult<Set<Long>>>> tsBatches = Iterators.partition(tsResults.get(), range.getBatchHint());
            Iterator<List<CandidateCellForSweeping>> candidates = Iterators.transform(tsBatches, tsBatch -> {
                List<CandidateCellForSweeping> candidateBatch = Lists.newArrayList();
                for (RowResult<Set<Long>> rr : tsBatch) {
                    for (Map.Entry<byte[], Set<Long>> e : rr.getColumns().entrySet()) {
                        byte[] colName = e.getKey();
                        Set<Long> timestamps = Sets.difference(e.getValue(), timestampsToIgnore);
                        long[] timestampArr = Longs.toArray(timestamps);
                        Arrays.sort(timestampArr);
                        Cell cell = Cell.create(rr.getRowName(), colName);
                        boolean latestValEmpty = isLatestValueEmpty(cell, peekingValues);
                        numExamined.add(timestampArr.length);
                        boolean candidate = isCandidate(timestampArr, latestValEmpty, request);
                        candidateBatch.add(ImmutableCandidateCellForSweeping.builder()
                                .cell(cell)
                                .sortedTimestamps(candidate ? timestampArr : EMPTY_LONG_ARRAY)
                                .isLatestValueEmpty(latestValEmpty)
                                .numCellsTsPairsExamined(numExamined.longValue())
                                .build());
                    }
                }
                return candidateBatch;
            });
            Closer closer = createCloserAndRelease(valueResults, tsResults);
            return ClosableIterators.wrap(candidates, closer);
        }
    }

    private static Closer createCloserAndRelease(ReleasableCloseable<?>... closeables) {
        Closer closer = Closer.create();
        for (ReleasableCloseable<?> c : closeables) {
            closer.register(c.get());
        }
        for (ReleasableCloseable<?> c : closeables) {
            c.release();
        }
        return closer;
    }

    private static boolean isCandidate(long[] timestamps,
                                       boolean lastValEmpty,
                                       CandidateCellForSweepingRequest request) {
        return timestamps.length > 1
            || (request.shouldCheckIfLatestValueIsEmpty() && lastValEmpty)
            || (timestamps.length == 1 && timestampIsPotentiallySweepable(timestamps[0], request));
    }

    private static boolean timestampIsPotentiallySweepable(long ts, CandidateCellForSweepingRequest request) {
        return ts == Value.INVALID_VALUE_TIMESTAMP || ts >= request.minUncommittedStartTimestamp();
    }

    private ClosableIterator<RowResult<Value>> getValues(TableReference tableRef,
                                                         RangeRequest range,
                                                         long sweepTs,
                                                         boolean checkIfLatestValueIsEmpty) {
        if (checkIfLatestValueIsEmpty) {
            return keyValueService.getRange(tableRef, range, sweepTs);
        } else {
            return ClosableIterators.emptyImmutableClosableIterator();
        }
    }

    private static boolean isLatestValueEmpty(Cell cell, PeekingIterator<RowResult<Value>> values) {
        while (values.hasNext()) {
            RowResult<Value> result = values.peek();
            int comparison = UnsignedBytes.lexicographicalComparator().compare(cell.getRowName(), result.getRowName());
            if (comparison == 0) {
                Value matchingValue = result.getColumns().get(cell.getColumnName());
                return matchingValue != null && matchingValue.getContents().length == 0;
            } else if (comparison < 0) {
                return false;
            } else {
                values.next();
            }
        }
        return false;
    }

    private static class ReleasableCloseable<T extends Closeable> implements AutoCloseable {
        private T closeable;

        ReleasableCloseable(T closeable) {
            this.closeable = closeable;
        }

        T get() {
            return closeable;
        }

        void release() {
            closeable = null;
        }

        @Override
        public void close() {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static final long[] EMPTY_LONG_ARRAY = new long[0];
}
