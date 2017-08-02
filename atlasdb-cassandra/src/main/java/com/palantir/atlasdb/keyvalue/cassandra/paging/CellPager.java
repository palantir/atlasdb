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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellPagerBatchSizingStrategy.PageSizes;

/*
 * A class for iterating uniformly through raw cells, i.e. (rowName, columnName, timestamp) triplets, in a given table
 * in lexicographic order (with timestamps being ordered in reverse).
 *
 * By "uniformly", we mean that the size of each page we load at once is bounded in terms of the number of
 * raw cells rather than the number of rows, and page boundaries don't coincide with row (partition) boundaries.
 *
 * Cassandra Thrift API doesn't provide a way to do this directly. So we have to resort to a "clever" hybrid approach:
 * fetch blocks of row "heads" using the "get_range_slices" call with a column-per-row limit, and then page
 * through the remainder of rows that exceed the fetched "head" limit using the "get_slice" call.
 *
 * Calling "get_range_slices" can be visualized as getting a "rectangle" of data from Cassandra, whose width and height
 * are equal to SliceRange.count and KeyRange.count, respectively. Consider an example table:
 *
 *         |<-w=4->|
 *         ._______.         ___
 *   Row 1 |a b c  |          ^
 *   Row 2 |d e f g|h i j     |
 *   Row 3 |k      |         h=5
 *   Row 4 |l m n o|          |
 *   Row 5 |p______|         _v_
 *   Row 6  q r
 *   Row 7  s t u
 *   Row 8  v w x y z
 *
 * Here, we set KeyRange.count = 5 and SliceRange.count = 4.
 * Since 3 cells are returned for Row 1 while we requested 4 cells per row, we can conclude that Row 1 is fully fetched,
 * and thus we can return it to the user. However, Row 2 is not fully fetched, so we have to page through its remainder.
 *
 * Overall, paging through the entire table in the picture looks like this:
 *
 *  1) Get the first rectangle of data:
 *       get_range_slices(KeyRange.start_key = [], KeyRange.count = 5, SliceRange.count = 4)
 *          -> returns rows 1-5 with at most 4 columns in each
 *  2) Return Row 1 and the first four cells of Row 2 to the user
 *  3) Get the remainder of Row 2:
 *       get_slice(key = "Row 2", SliceRange.start = <lex. next column key after 'g'>, SliceRange.count = cellBatchHint)
 *          -> [ h i j ]
 *  4) Return [ h i j ] to the user
 *  5) Return Rows 3 and 4 to the user (they were already fetched in step 1)
 *  6) Since we got exactly 4 cells for row 4, we don't really know whether there are more, so we have to attempt
 *     to get its remainder:
 *       get_slice(key = "Row 4", SliceRange.start = <lex. next column key after 'o'>, SliceRange,count = cellBatchHint)
 *          -> []
 *  7) Return Row 5 to the user
 *  8) Now we finished the first rectangle, so it's time to get another one:
 *       get_range_slices(KeyRange.start_key = <lex. next key after "Row 5">, KeyRange.count = 5, SliceRange.count = 4)
 *          -> returns rows 6-8 with at most 4 columns in each
 *  9) Return Row 6, Row 7 and the first four columns of Row 8 to the user
 *  10) Get the remainder of Row 8:
 *       get_slice(key = "Row 8", SliceRange.start = <lex. next column key after 'y'>, SliceRange,count = cellBatchHint)
 *          -> [ z ]
 *  11) Return [ z ] to the user
 *
 * Note that getting the remainder of a row might require more than one call "get_slice" if the row is wide.
 *
 * We want the area of each rectangle to be roughly equal to "cellBatchHint" which is supplied by the user. In order to
 * choose reasonable width and height of the rectangle given the area, we estimate the ditribution of row widths
 * and try to make a somewhat intelligent decision based on that data (see CellPagerBatchSizingStrategy for details).
 */
public class CellPager {
    private final SingleRowColumnPager singleRowPager;
    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;
    private final CellPagerBatchSizingStrategy pageSizeStrategy;

    public CellPager(SingleRowColumnPager singleRowPager,
                     CassandraClientPool clientPool,
                     TracingQueryRunner queryRunner,
                     CellPagerBatchSizingStrategy pageSizeStrategy) {
        this.singleRowPager = singleRowPager;
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.pageSizeStrategy = pageSizeStrategy;
    }

    public Iterator<List<CassandraRawCellValue>> createCellIterator(TableReference tableRef,
                                                                    byte[] startRowInclusive,
                                                                    int cellBatchHint,
                                                                    ConsistencyLevel consistencyLevel) {
        Preconditions.checkNotNull(startRowInclusive,
                "Use an empty byte array rather than null to start from the beginning of the table");
        Preconditions.checkArgument(cellBatchHint > 0, "cellBatchHint must be strictly positive");
        RowRangeLoader rowRangeLoader = new RowRangeLoader(clientPool, queryRunner, consistencyLevel, tableRef);
        return new PageIterator(tableRef, rowRangeLoader, cellBatchHint, consistencyLevel, startRowInclusive);
    }

    private class PageIterator extends AbstractIterator<List<CassandraRawCellValue>> {
        private final TableReference tableRef;
        private final RowRangeLoader rowRangeLoader;
        private final int cellBatchHint;
        private final ConsistencyLevel consistencyLevel;

        private byte[] nextRow;

        private final Queue<Iterator<List<CassandraRawCellValue>>> queuedTasks = new ArrayDeque<>();
        private final StatsAccumulator stats = new StatsAccumulator();
        private PageSizes pageSizes = null;

        PageIterator(TableReference tableRef, RowRangeLoader rowRangeLoader, int cellBatchHint,
                ConsistencyLevel consistencyLevel, byte[] nextRow) {
            this.tableRef = tableRef;
            this.rowRangeLoader = rowRangeLoader;
            this.cellBatchHint = cellBatchHint;
            this.consistencyLevel = consistencyLevel;
            this.nextRow = nextRow;
        }

        @Override
        protected List<CassandraRawCellValue> computeNext() {
            while (true) {
                if (queuedTasks.isEmpty()) {
                    if (nextRow == null) {
                        return endOfData();
                    } else {
                        fetchNextRange();
                    }
                } else if (queuedTasks.peek().hasNext()) {
                    return queuedTasks.peek().next();
                } else {
                    queuedTasks.poll();
                }
            }
        }

        private void fetchNextRange() {
            pageSizes = pageSizeStrategy.computePageSizes(cellBatchHint, stats);
            KeyRange range = KeyRanges.createKeyRange(nextRow, PtBytes.EMPTY_BYTE_ARRAY, pageSizes.rowLimit);
            ColumnFetchMode fetchMode = ColumnFetchMode.fetchAtMost(pageSizes.columnPerRowLimit);
            List<KeySlice> slices = rowRangeLoader.getRows(range, fetchMode);
            if (slices.isEmpty()) {
                nextRow = null;
            } else {
                splitFetchedRowsIntoTasks(slices);
                computeNextStartRow(slices);
            }
        }

        private void splitFetchedRowsIntoTasks(List<KeySlice> slices) {
            // Split the returned slices into single partially fetched rows and contiguous runs of fully fetched rows
            List<KeySlice> loadedRows = new ArrayList<>();
            for (KeySlice slice : slices) {
                loadedRows.add(slice);
                if (isFullyFetched(slice)) {
                    stats.add(slice.getColumnsSize());
                } else {
                    queuedTasks.add(ImmutableList.of(keySlicesToCells(loadedRows)).iterator());
                    loadedRows.clear();
                    // If the row was only partially fetched, we enqueue an iterator to page through
                    // the remainder of that single row.
                    Column lastSeenColumn = Iterables.getLast(slice.getColumns()).column;
                    Iterator<List<ColumnOrSuperColumn>> rawColumnIter = singleRowPager.createColumnIterator(
                            tableRef, slice.getKey(), cellBatchHint, lastSeenColumn, consistencyLevel);
                    Iterator<List<ColumnOrSuperColumn>> statsUpdatingIter = new StatsUpdatingIterator(
                            rawColumnIter, stats, slice.getColumnsSize());
                    queuedTasks.add(Iterators.transform(
                            statsUpdatingIter, cols -> columnsToCells(cols, slice.getKey())));
                }
            }
            if (!loadedRows.isEmpty()) {
                queuedTasks.add(ImmutableList.of(keySlicesToCells(loadedRows)).iterator());
            }
        }

        private boolean isFullyFetched(KeySlice slice) {
            return slice.getColumnsSize() < pageSizes.columnPerRowLimit;
        }

        private void computeNextStartRow(List<KeySlice> slices) {
            if (slices.size() < pageSizes.rowLimit) {
                nextRow = null;
            } else {
                byte[] lastSeenRow = Iterables.getLast(slices).getKey();
                nextRow = RangeRequests.getNextStartRowUnlessTerminal(
                        false, RangeRequests.nextLexicographicName(lastSeenRow));
            }
        }
    }

    private static class StatsUpdatingIterator extends AbstractIterator<List<ColumnOrSuperColumn>> {
        private final Iterator<List<ColumnOrSuperColumn>> delegate;
        private final StatsAccumulator stats;
        private long columnCount = 0;

        StatsUpdatingIterator(Iterator<List<ColumnOrSuperColumn>> delegate, StatsAccumulator stats, long columnCount) {
            this.delegate = delegate;
            this.stats = stats;
            this.columnCount = columnCount;
        }

        @Override
        protected List<ColumnOrSuperColumn> computeNext() {
            if (delegate.hasNext()) {
                List<ColumnOrSuperColumn> cols = delegate.next();
                columnCount += cols.size();
                return cols;
            } else {
                stats.add(columnCount);
                return endOfData();
            }
        }
    }

    private static List<CassandraRawCellValue> keySlicesToCells(List<KeySlice> slices) {
        List<CassandraRawCellValue> ret = new ArrayList<>();
        for (KeySlice slice : slices) {
            for (ColumnOrSuperColumn col : slice.getColumns()) {
                ret.add(new CassandraRawCellValue(slice.getKey(), col.getColumn()));
            }
        }
        return ret;
    }

    private static List<CassandraRawCellValue> columnsToCells(List<ColumnOrSuperColumn> columns, byte[] rowKey) {
        return Lists.transform(columns, col -> new CassandraRawCellValue(rowKey, col.getColumn()));
    }

}
