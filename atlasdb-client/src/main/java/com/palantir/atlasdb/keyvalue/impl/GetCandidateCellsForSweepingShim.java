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

package com.palantir.atlasdb.keyvalue.impl;

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

// This class provides a temporary implementation of the new call using existing KVS calls,
// so that we can make this change in several smaller commits.
public class GetCandidateCellsForSweepingShim {
    private final KeyValueService keyValueService;

    public GetCandidateCellsForSweepingShim(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

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
                        candidateBatch.add(ImmutableCandidateCellForSweeping.builder()
                                .cell(cell)
                                .sortedTimestamps(timestampArr)
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

}
