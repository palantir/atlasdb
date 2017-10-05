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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.util.Pair;

/** Iterates over raw Cassandra Columns of a single given row in batches. */
public class SingleRowColumnPager {
    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;

    public SingleRowColumnPager(CassandraClientPool clientPool, TracingQueryRunner queryRunner) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
    }

    public Iterator<List<ColumnOrSuperColumn>> createColumnIterator(TableReference tableRef,
                                                                    byte[] rowKey,
                                                                    int pageSize,
                                                                    @Nullable Column startColumnExclusive,
                                                                    ConsistencyLevel consistencyLevel) {
        ColumnParent columnParent = new ColumnParent(CassandraKeyValueServiceImpl.internalTableName(tableRef));
        return new PageIterator(
                tableRef, columnParent, ByteBuffer.wrap(rowKey), pageSize, consistencyLevel, startColumnExclusive);
    }

    private class PageIterator extends AbstractIterator<List<ColumnOrSuperColumn>> {
        private final TableReference tableRef;
        private final ColumnParent columnParent;
        private final ByteBuffer rowKey;
        private final int pageSize;
        private final ConsistencyLevel consistencyLevel;
        private Column lastSeenColumn;
        private boolean reachedEnd = false;

        PageIterator(TableReference tableRef, ColumnParent columnParent, ByteBuffer rowKey, int pageSize,
                ConsistencyLevel consistencyLevel, Column lastSeenColumn) {
            this.tableRef = tableRef;
            this.columnParent = columnParent;
            this.rowKey = rowKey;
            this.pageSize = pageSize;
            this.consistencyLevel = consistencyLevel;
            this.lastSeenColumn = lastSeenColumn;
        }

        @Override
        protected List<ColumnOrSuperColumn> computeNext() {
            if (reachedEnd) {
                return endOfData();
            } else {
                InetSocketAddress host = clientPool.getRandomHostForKey(rowKey.array());
                try {
                    List<ColumnOrSuperColumn> cells = clientPool.runWithRetryOnHost(host, this::getPage);
                    reachedEnd = cells.size() < pageSize;
                    if (cells.isEmpty()) {
                        return endOfData();
                    } else {
                        lastSeenColumn = Iterables.getLast(cells).column;
                        return cells;
                    }
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private List<ColumnOrSuperColumn> getPage(Cassandra.Client client) throws TException {
            Optional<ByteBuffer> startColumn = getStartColumn(lastSeenColumn);
            if (startColumn.isPresent()) {
                SliceRange sliceRange = new SliceRange(
                        startColumn.get(),
                        ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY),
                        false, // reversed
                        pageSize);
                SlicePredicate slicePred = new SlicePredicate();
                slicePred.setSlice_range(sliceRange);
                return queryRunner.run(client, tableRef,
                        () -> client.get_slice(rowKey, columnParent, slicePred, consistencyLevel));
            } else {
                return ImmutableList.of();
            }
        }
    }

    // Given the last seen column, compute the next start column
    @VisibleForTesting
    static Optional<ByteBuffer> getStartColumn(@Nullable Column lastSeenColumn) {
        if (lastSeenColumn == null) {
            // An empty byte array means "start from the beginning of the row"
            return Optional.of(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY));
        } else {
            // We need to "increment" the last seen (colName, ts) pair to the lexicographically next pair.
            // Note that timestamps are stored in descending order. For example, the next pair for ("a", 5)
            // will be ("a", 4).
            Pair<byte[], Long> colNameAndTs = CassandraKeyValueServices.decomposeName(lastSeenColumn);
            if (colNameAndTs.getRhSide() == Long.MIN_VALUE) {
                // This is an edge case that will never happen in reality, but technically we have
                // to handle it anyway. If the last seen timestamp equals minimum possible value of a 64-bit
                // signed integer, we can't decrement it, so we have to carry over to the colName.
                // E.g. ("a", Long.MIN_VALUE) -> ("b", Long.MAX_VALUE)
                byte[] nextColName = RangeRequests.getNextStartRowUnlessTerminal(false, colNameAndTs.getLhSide());
                if (nextColName == null) {
                    // An even "edgier" case: in addition to reaching the minimum timestamp, we also reached
                    // the last possible colName, so there is nowhere to increment.
                    return Optional.empty();
                } else {
                    return Optional.of(CassandraKeyValueServices.makeCompositeBuffer(nextColName, Long.MAX_VALUE));
                }
            } else {
                // Timestamps are in descending order, so just decrement the timestamp to find the next column.
                // E.g., ("a", 4) -> ("a", 3)
                return Optional.of(CassandraKeyValueServices.makeCompositeBuffer(
                        colNameAndTs.getLhSide(),
                        colNameAndTs.getRhSide() - 1));
            }
        }
    }
}
