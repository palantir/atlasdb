/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraQueryRunner;
import com.palantir.atlasdb.keyvalue.cassandra.ResultsExtractor;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.BasicResultsPage;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class PagingIterable<T, U> extends AbstractPagingIterable {
    private final ColumnGetter columnGetter;
    private final CassandraClientPool clientPool;
    private final CassandraQueryRunner queryRunner;
    private final RangeRequest rangeRequest;
    private final TableReference tableRef;
    private final SlicePredicate pred;
    private final ConsistencyLevel consistency;
    private final Supplier<ResultsExtractor<T, U>> resultsExtractor;
    private final long timestamp;

    private final int batchHint;
    private final ColumnSelection selection;

    public PagingIterable(
            ColumnGetter columnGetter,
            CassandraClientPool clientPool,
            CassandraQueryRunner queryRunner,
            RangeRequest rangeRequest,
            TableReference tableRef,
            SlicePredicate pred,
            ConsistencyLevel consistency,
            Supplier<ResultsExtractor<T, U>> resultsExtractor,
            long timestamp) {
        this.columnGetter = columnGetter;
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.rangeRequest = rangeRequest;
        this.tableRef = tableRef;
        this.pred = pred;
        this.consistency = consistency;
        this.resultsExtractor = resultsExtractor;
        this.timestamp = timestamp;

        batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        selection = rangeRequest.getColumnNames().isEmpty() ? ColumnSelection.all()
                : ColumnSelection.create(rangeRequest.getColumnNames());

    }

    @Override
    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getFirstPage() throws Exception {
        return getSinglePage(rangeRequest.getStartInclusive());
    }

    @Override
    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getNextPage(BasicResultsPage previous)
            throws Exception {
        TokenBackedBasicResultsPage<RowResult<U>, byte[]> castedPrevious =
                (TokenBackedBasicResultsPage<RowResult<U>, byte[]>) previous;
        return getSinglePage(castedPrevious.getTokenForNextPage());
    }

    private TokenBackedBasicResultsPage<RowResult<U>, byte[]> getSinglePage(final byte[] startKey) throws Exception {
        final byte[] endExclusive = rangeRequest.getEndExclusive();

        KeyRange keyRange = getKeyRange(startKey, endExclusive);
        List<KeySlice> firstPage = getRangeSlices(keyRange);

        Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = columnGetter.getColumnsByRow(firstPage);

        TokenBackedBasicResultsPage<RowResult<U>, byte[]> page =
                resultsExtractor.get().getPageFromRangeResults(colsByKey, timestamp, selection, endExclusive);

        if (page.moreResultsAvailable() && firstPage.size() < batchHint) {
            // If get_range_slices didn't return the full number of results, there's no
            // point to trying to get another page
            return SimpleTokenBackedResultsPage.create(endExclusive, page.getResults(), false);
        }

        return page;
    }

    private List<KeySlice> getRangeSlices(final KeyRange keyRange) throws Exception {
        final ColumnParent colFam = new ColumnParent(CassandraKeyValueService.internalTableName(tableRef));
        InetSocketAddress host = clientPool.getRandomHostForKey(keyRange.getStart_key());
        return clientPool.runWithRetryOnHost(
                host,
                new FunctionCheckedException<Cassandra.Client, List<KeySlice>, Exception>() {
                    @Override
                    public List<KeySlice> apply(Cassandra.Client client) throws Exception {
                        try {
                            return queryRunner.run(client, tableRef,
                                    () -> client.get_range_slices(colFam, pred, keyRange, consistency));
                        } catch (UnavailableException e) {
                            if (consistency.equals(ConsistencyLevel.ALL)) {
                                throw new InsufficientConsistencyException("This operation requires all Cassandra"
                                        + " nodes to be up and available.", e);
                            } else {
                                throw e;
                            }
                        }
                    }

                    @Override
                    public String toString() {
                        return "get_range_slices(" + colFam + ")";
                    }
                });
    }

    private KeyRange getKeyRange(byte[] startKey, byte[] endExclusive) {
        KeyRange keyRange = new KeyRange(batchHint);
        keyRange.setStart_key(startKey);
        if (endExclusive.length == 0) {
            keyRange.setEnd_key(endExclusive);
        } else {
            // We need the previous name because this is inclusive, not exclusive
            keyRange.setEnd_key(RangeRequests.previousLexicographicName(endExclusive));
        }
        return keyRange;
    }
}
