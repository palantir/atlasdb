/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CassandraRangePagingIterable;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ThriftColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.SlicePredicate;

public class RangeLoader {
    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;
    private final MetricsManager metricsManager;
    private final ReadConsistencyProvider readConsistencyProvider;

    public RangeLoader(
            CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            MetricsManager metricsManager,
            ReadConsistencyProvider readConsistencyProvider) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.metricsManager = metricsManager;
        this.readConsistencyProvider = readConsistencyProvider;
    }

    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest, long ts) {
        return getRangeWithPageCreator(
                tableRef, rangeRequest, ts, () -> ValueExtractor.create(metricsManager));
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long startTs,
            Supplier<ResultsExtractor<T>> resultsExtractor) {
        SlicePredicate predicate;
        if (rangeRequest.getColumnNames().size() == 1) {
            byte[] colName = rangeRequest.getColumnNames().iterator().next();
            predicate = SlicePredicates.latestVersionForColumn(colName, startTs);
        } else {
            // TODO(nziebart): optimize fetching multiple columns by performing a parallel range request for
            // each column. note that if no columns are specified, it's a special case that means all columns
            predicate = SlicePredicates.create(SlicePredicates.Range.ALL, SlicePredicates.Limit.NO_LIMIT);
        }
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, readConsistencyProvider.getConsistency(tableRef),
                tableRef);
        ColumnGetter columnGetter = new ThriftColumnGetter();

        return getRangeWithPageCreator(rowGetter, predicate, columnGetter, rangeRequest, resultsExtractor, startTs);
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            RowGetter rowGetter,
            SlicePredicate slicePredicate,
            ColumnGetter columnGetter,
            RangeRequest rangeRequest,
            Supplier<ResultsExtractor<T>> resultsExtractor,
            long startTs) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<T>>of().iterator());
        }

        CassandraRangePagingIterable<T> rowResults = new CassandraRangePagingIterable<>(
                rowGetter, slicePredicate, columnGetter, rangeRequest, resultsExtractor, startTs);

        return ClosableIterators.wrap(rowResults.iterator());
    }
}
