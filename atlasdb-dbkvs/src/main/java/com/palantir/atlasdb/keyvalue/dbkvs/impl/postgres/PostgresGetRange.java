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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRange;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeHelpers;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/* 1) On Postgres, there seems to be no efficient way to page at atlas row boundaries.
 *    The approach with 'DENSE_RANK() <= x' that works exceptionally well on Oracle,
 *    performs quite terribly on Postgres. Another very hacky option would be to pack
 *    SQL rows corresponding to each atlas row in an array using 'ARRAY_AGG' and then
 *    LIMIT the results, but that also is not very fast.
 *
 *    Instead, we page at cell boundaries internally, but then we make sure that
 *    no single atlas row is spread over multiple RowResults. Unlike Oracle, Postgres
 *    supports the row value syntax (i.e. 'WHERE (row_name, col_name) >= (?, ?)'), so
 *    we can start the new page from the middle of an atlas row efficiently.
 *
 *    To convert the batch size hint from the number of rows to the number of cells,
 *    we estimate the number of cells per row using table metadata.
 *
 *
 * 2) Like on Oracle, there are several ways to load values for each cell. We stick to
 *    the regular 'GROUP BY wrapped in a self-join' approach because it seems to perform
 *    best. The other ideas I've tried:
 *
 *      - Window functions + DISTINCT ON
 *
 *           SELECT DISTINCT ON(row_name, col_name)
 *                  row_name,
 *                  col_name,
 *                  LAST_VALUE(ts) OVER row_and_col AS ts,
 *                  LAST_VALUE(val) OVER row_and_col AS val
 *           FROM my_table
 *           WHERE ...
 *           WINDOW row_and_col AS (
 *              PARTITION BY row_name, col_name
 *              ORDER BY ts
 *              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *           )
 *           ORDER BY row_name, col_name
 *
 *      - ARRAY_AGG hack to emulate Oracle's 'KEEP (DENSE_RANK LAST ORDER BY ts)' aggregate:
 *
 *           SELECT (ARRAY_AGG(
 *                      ROW(row_name, col_name, ts, val)::my_table
 *                      ORDER BY ts DESC
 *                    ))[1] AS cell
 *           FROM my_table
 *           WHERE ...
 *           GROUP BY row_name, col_name
 *           ORDER BY row_name ASC, col_name ASC
 *
 *      - Custom aggregate function:
 *
 *           CREATE TYPE cell_value AS (row_name BYTEA, col_name BYTEA, ts INT8, val BYTEA);
 *
 *           CREATE FUNCTION maxtsrow_sfunc(state cell_value, el cell_value) RETURNS cell_value
 *           IMMUTABLE LANGUAGE PLPGSQL AS $$
 *               begin
 *                   if el.ts < state.ts then
 *                       return state;
 *                   else
 *                       return el;
 *                   end if;
 *               end;
 *           $$;
 *
 *           CREATE AGGREGATE maxtsrow (cell_value) (sfunc = maxtsrow_sfunc, stype = cell_value);
 *
 *           SELECT maxtsrow(ROW(row_name, col_name, ts, val)::cell_value) AS cell
 *           FROM my_table
 *           WHERE ...
 *           GROUP BY row_name, col_name
 *           ORDER BY row_name ASC, col_name ASC
 *
 *    All of the above seem to perform wrose than the self-join approach.
 *
 */
public class PostgresGetRange implements DbKvsGetRange {
    private final PostgresPrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;
    private final TableMetadataCache tableMetadataCache;

    public PostgresGetRange(PostgresPrefixedTableNames prefixedTableNames,
                            SqlConnectionSupplier connectionPool,
                            TableMetadataCache tableMetadataCache) {
        this.prefixedTableNames = prefixedTableNames;
        this.connectionPool = connectionPool;
        this.tableMetadataCache = tableMetadataCache;
    }

    @Override
    public Iterator<RowResult<Value>> getRange(TableReference tableRef,
                                               RangeRequest rangeRequest,
                                               long timestamp) {
        int maxRowsPerPage = RangeHelpers.getMaxRowsPerPage(rangeRequest);
        int maxCellsPerPage = DbKvsGetRanges.getMaxCellsPerPage(
                tableRef, rangeRequest, maxRowsPerPage, connectionPool, tableMetadataCache);
        String tableName = DbKvs.internalTableName(tableRef);
        Iterator<Iterator<RowResult<Value>>> pageIterator = new PageIterator(
                rangeRequest.getStartInclusive(),
                rangeRequest.getEndExclusive(),
                rangeRequest.getColumnNames(),
                rangeRequest.isReverse(),
                timestamp,
                maxRowsPerPage,
                maxCellsPerPage,
                tableName,
                prefixedTableNames.get(tableRef));
        return Iterators.concat(pageIterator);
    }

    private class PageIterator extends AbstractIterator<Iterator<RowResult<Value>>> {
        private byte[] currentRowName;
        private ImmutableSortedMap.Builder<byte[], Value> currentRowCells = RangeHelpers.newColumnMap();
        private byte[] firstRowStartColumnInclusive = PtBytes.EMPTY_BYTE_ARRAY;
        private boolean endOfResults = false;

        private final byte[] endExclusive;
        private final Set<byte[]> columnSelection;
        private final boolean reverse;
        private final long ts;
        private final int maxRowsPerPage;
        private final int maxCellsPerPage;
        private final String tableName;
        private final String prefixedTableName;

        PageIterator(byte[] currentRowName, byte[] endExclusive, Set<byte[]> columnSelection, boolean reverse,
                     long ts, int maxRowsPerPage, int maxCellsPerPage, String tableName, String prefixedTableName) {
            this.currentRowName = currentRowName;
            this.endExclusive = endExclusive;
            this.columnSelection = columnSelection;
            this.reverse = reverse;
            this.ts = ts;
            this.maxRowsPerPage = maxRowsPerPage;
            this.maxCellsPerPage = maxCellsPerPage;
            this.tableName = tableName;
            this.prefixedTableName = prefixedTableName;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected Iterator<RowResult<Value>> computeNext() {
            if (endOfResults) {
                return endOfData();
            } else {
                try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                        ClosableIterator<AgnosticLightResultRow> iter = selectNextPage(conns)) {
                    List<RowResult<Value>> results = new ArrayList<>(maxRowsPerPage);
                    int numSqlRows = 0;
                    byte[] colName = null;
                    while (iter.hasNext()) {
                        numSqlRows += 1;
                        AgnosticLightResultRow sqlRow = iter.next();
                        byte[] rowName = sqlRow.getBytes("row_name");
                        colName = Preconditions.checkNotNull(sqlRow.getBytes("col_name"),
                                "received a null col_name from the database");
                        if (!Arrays.equals(currentRowName, rowName)) {
                            flushCurrentRow(results);
                            currentRowName = rowName;
                        }
                        Value value = Value.create(sqlRow.getBytes("val"), sqlRow.getLong("ts"));
                        currentRowCells.put(colName, value);
                    }
                    if (numSqlRows < maxCellsPerPage || colName == null) {
                        getCurrentRowResult().ifPresent(results::add);
                        endOfResults = true;
                    } else {
                        computeNextStartPosition(colName, results);
                    }
                    return results.iterator();
                }
            }
        }

        private void computeNextStartPosition(byte[] lastColName,
                                              @Output List<RowResult<Value>> results) {
            firstRowStartColumnInclusive = RangeRequests.getNextStartRowUnlessTerminal(reverse, lastColName);
            // We need to handle the edge case where the column was lexicographically last
            if (firstRowStartColumnInclusive == null) {
                flushCurrentRow(results);
                currentRowName = RangeRequests.getNextStartRowUnlessTerminal(reverse, currentRowName);
                firstRowStartColumnInclusive = PtBytes.EMPTY_BYTE_ARRAY;
                if (currentRowName == null) {
                    endOfResults = true;
                }
            }
        }

        private void flushCurrentRow(@Output List<RowResult<Value>> results) {
            getCurrentRowResult().ifPresent(results::add);
            currentRowCells = RangeHelpers.newColumnMap();
        }

        private Optional<RowResult<Value>> getCurrentRowResult() {
            ImmutableSortedMap<byte[], Value> cells = currentRowCells.build();
            if (cells.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(RowResult.create(currentRowName, cells));
            }
        }

        private ClosableIterator<AgnosticLightResultRow> selectNextPage(ConnectionSupplier conns) {
            FullQuery query = getRangeQuery();
            AgnosticLightResultSet rs = conns.get().selectLightResultSetUnregisteredQueryWithFetchSize(
                    query.getQuery(), maxCellsPerPage, query.getArgs());
            return ClosableIterators.wrap(rs.iterator(), rs);
        }

        private FullQuery getRangeQuery() {
            String direction = reverse ? "DESC" : "ASC";
            FullQuery.Builder queryBuilder = FullQuery.builder()
                    .append("/* GET_RANGE(").append(tableName).append(") */")
                    .append("SELECT wrap.row_name, wrap.col_name, wrap.ts, wrap.val")
                    .append("  FROM ").append(prefixedTableName).append(" wrap, (")
                    .append("    SELECT row_name, col_name, MAX(ts) AS ts FROM ").append(prefixedTableName)
                    .append("    WHERE ts < ? ", ts);
            RangePredicateHelper.create(reverse, DBType.POSTGRESQL, queryBuilder)
                    .startCellInclusive(currentRowName, firstRowStartColumnInclusive)
                    .endRowExclusive(endExclusive)
                    .columnSelection(columnSelection);
            queryBuilder
                    .append("    GROUP BY row_name, col_name")
                    .append("    ORDER BY row_name ").append(direction).append(", col_name ").append(direction)
                    .append("    LIMIT ").append(maxCellsPerPage)
                    .append("  ) i")
                    .append("  WHERE wrap.row_name = i.row_name")
                    .append("    AND wrap.col_name = i.col_name")
                    .append("    AND wrap.ts = i.ts")
                    .append("  ORDER BY row_name ").append(direction).append(", col_name ").append(direction);
            return queryBuilder.build();
        }
    }

}
