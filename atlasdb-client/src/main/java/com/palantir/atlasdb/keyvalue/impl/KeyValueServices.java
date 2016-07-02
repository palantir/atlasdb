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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class KeyValueServices {
    private static final Logger log = LoggerFactory.getLogger(KeyValueServices.class);

    private KeyValueServices() {/**/}

    public static TableMetadata getTableMetadataSafe(KeyValueService service, TableReference tableRef) {
        try {
            byte[] metadataForTable = service.getMetadataForTable(tableRef);
            if (metadataForTable == null || metadataForTable.length == 0) {
                return null;
            }
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataForTable);
        } catch (Exception e) {
            log.warn("failed to get metadata for table", e);
            return null;
        }
    }

    public static void getFirstBatchForRangeUsingGetRange(
            KeyValueService kv,
            TableReference tableRef,
            RangeRequest request,
            long timestamp,
            @Output Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret) {
        if (ret.containsKey(request)) {
            return;
        }
        RangeRequest requestWithHint = request;
        if (request.getBatchHint() == null) {
            requestWithHint = request.withBatchHint(100);
        }
        final ClosableIterator<RowResult<Value>> range = kv.getRange(tableRef, requestWithHint, timestamp);
        try {
            int batchSize = requestWithHint.getBatchHint();
            final Iterator<RowResult<Value>> withLimit = Iterators.limit(range, batchSize);
            ImmutableList<RowResult<Value>> results = ImmutableList.copyOf(withLimit);
            if (results.size() != batchSize) {
                ret.put(request, SimpleTokenBackedResultsPage.create(request.getEndExclusive(), results, false));
                return;
            }
            RowResult<Value> last = results.get(results.size()-1);
            byte[] lastRowName = last.getRowName();
            if (RangeRequests.isTerminalRow(request.isReverse(), lastRowName)) {
                ret.put(request, SimpleTokenBackedResultsPage.create(lastRowName, results, false));
                return;
            }
            byte[] nextStartRow = RangeRequests.getNextStartRow(request.isReverse(), lastRowName);
            if (Arrays.equals(request.getEndExclusive(), nextStartRow)) {
                ret.put(request, SimpleTokenBackedResultsPage.create(nextStartRow, results, false));
            } else {
                ret.put(request, SimpleTokenBackedResultsPage.create(nextStartRow, results, true));
            }
        } finally {
            range.close();
        }
    }

    public static Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRangesUsingGetRangeConcurrent(
            ExecutorService executor,
            final KeyValueService kv,
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            final long timestamp,
            int maxConcurrentRequests) {
        final Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = Maps.newConcurrentMap();
        BlockingWorkerPool pool = new BlockingWorkerPool(executor, maxConcurrentRequests);
        try {
            for (final RangeRequest request : rangeRequests) {
                pool.submitTask(new Runnable() {
                    @Override
                    public void run() {
                        getFirstBatchForRangeUsingGetRange(kv, tableRef, request, timestamp, ret);
                    }
                });
            }
            pool.waitForSubmittedTasks();
            return ret;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public static Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRangesUsingGetRange(
            KeyValueService kv,
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = Maps.newHashMap();
        for (final RangeRequest request : rangeRequests) {
            getFirstBatchForRangeUsingGetRange(kv, tableRef, request, timestamp, ret);
        }
        return ret;
    }

    public static Collection<Map.Entry<Cell, Value>> toConstantTimestampValues(final Collection<Map.Entry<Cell, byte[]>> cells, final long timestamp) {
        return Collections2.transform(cells, new Function<Map.Entry<Cell, byte[]>, Map.Entry<Cell, Value>>() {
            @Override
            public Map.Entry<Cell, Value> apply(Map.Entry<Cell, byte[]> entry) {
                return Maps.immutableEntry(entry.getKey(), Value.create(entry.getValue(), timestamp));
            }
        });
    }
}
