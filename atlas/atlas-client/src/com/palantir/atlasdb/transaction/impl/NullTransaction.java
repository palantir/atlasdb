// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.lang.Validate;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.collect.IterableView;
import com.palantir.common.collect.MapEntries;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * Transaction implementation that directly delegates to the underlying
 * key-value service (i.e., provides no transactionality or isolation
 * guarantees).
 * <p>
 * Intended to be used for benchmarking.
 */
public class NullTransaction extends AbstractTransaction {
    private static final int BATCH_SIZE_GET_FIRST_PAGE = 1000;

    private final KeyValueService service;
    private final long timeStamp;
    private final boolean isReadOnly;

    public NullTransaction(KeyValueService service, long timestamp, boolean isReadOnly) {
        this.service = service;
        this.timeStamp = timestamp;
        this.isReadOnly = isReadOnly;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(String tableName, Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        Map<Cell, byte[]> ret = unwrap(service.getRows(tableName, rows, columnSelection,
                                                       timeStamp+1));
        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(ret));
    }

    @Override
    public Map<Cell, byte[]> get(String tableName, Set<Cell> cells) {
        return unwrap(service.get(tableName, Cells.constantValueMap(cells, timeStamp+1)));
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        Validate.isTrue(!isReadOnly);
        service.put(tableName, values, timeStamp);
    }

    private Iterator<RowResult<byte[]>> transformResults(final Iterator<RowResult<Value>> it) {
        return new AbstractIterator<RowResult<byte[]>>() {
            @Override
            protected RowResult<byte[]> computeNext() {
                while (true) {
                    if (!it.hasNext()) {
                        return endOfData();
                    }
                    RowResult<Value> result = it.next();
                    Function<Entry<byte[], Value>, byte[]> compose = Functions.compose(Value.GET_VALUE, MapEntries.<byte[], Value>getValueFunction());
                    IterableView.of(result.getColumns().entrySet())
                    .transform(compose);

                    Map<byte[], byte[]> columns = Maps.filterValues(
                        Maps.transformValues(result.getColumns(), Value.GET_VALUE),
                        Predicates.not(Value.IS_EMPTY));

                    ImmutableSortedMap<byte[], byte[]> sortedColumns = ImmutableSortedMap.copyOf(columns, UnsignedBytes.lexicographicalComparator());
                    if (sortedColumns.isEmpty()) {
                        continue;
                    }
                    return RowResult.create(result.getRowName(), sortedColumns);
                }
            }
        };
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(final String tableName,
                                                                    Iterable<RangeRequest> rangeRequests) {
        return Iterables.concat(Iterables.transform(
                Iterables.partition(rangeRequests, BATCH_SIZE_GET_FIRST_PAGE),
                new Function<List<RangeRequest>, List<BatchingVisitable<RowResult<byte[]>>>>() {
                    @Override
                    public List<BatchingVisitable<RowResult<byte[]>>> apply(final List<RangeRequest> input) {
                        final Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstBatchForRanges =
                                service.getFirstBatchForRanges(
                                        tableName,
                                        input,
                                        timeStamp + 1);
                        List<BatchingVisitable<RowResult<byte[]>>> ret = Lists.newArrayListWithCapacity(input.size());
                        for (final RangeRequest range : input) {
                            ret.add(new AbstractBatchingVisitable<RowResult<byte[]>>() {
                                @Override
                                protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                                            ConsistentVisitor<RowResult<byte[]>, K> v)
                                        throws K {
                                    TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page = firstBatchForRanges.get(range);
                                    List<RowResult<Value>> firstPage = page.getResults();
                                    Iterator<RowResult<byte[]>> rowResults = transformResults(firstPage.iterator());
                                    while (rowResults.hasNext()) {
                                        if (!v.visitOne(rowResults.next())) {
                                            return;
                                        }
                                    }
                                    if (!page.moreResultsAvailable()) {
                                        return;
                                    }
                                    RangeRequest newRange = range.getBuilder().startRowInclusive(
                                            page.getTokenForNextPage()).build();
                                    getRange(tableName, newRange).batchAccept(batchSizeHint, v);
                                }
                            });
                        }
                        return ret;
                    }
                }));
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(final String tableName,
            final RangeRequest request) {
        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                    ConsistentVisitor<RowResult<byte[]>, K> v) throws K {
                if (request.getBatchHint() != null) {
                    batchSizeHint = request.getBatchHint();
                }
                ClosableIterator<RowResult<Value>> range = service.getRange(tableName, request.withBatchHint(batchSizeHint), timeStamp+1);
                try {
                    Iterator<RowResult<byte[]>> transform = transformResults(range);
                    BatchingVisitableFromIterable.create(transform).batchAccept(1, v);
                } finally {
                    range.close();
                }
            }
        };
    }

    @Override
    public void commit() {
        dropTempTables();
    }

    @Override
    public void abort() {
        dropTempTables();
    }

    @Override
    public long getTimestamp() {
        return timeStamp;
    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public boolean isUncommitted() {
        return false;
    }

    private Map<Cell, byte[]> unwrap(Map<Cell, Value> kvs) {
        Map<Cell, byte[]> r = Maps.newHashMap();
        for (Map.Entry<Cell, Value> e : kvs.entrySet()) {
            r.put(e.getKey(), e.getValue().getContents());
        }
        return r;
    }

    @Override
    public void useTable(String tableName, ConstraintCheckable table) {
        /**/
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return service;
    }
}
