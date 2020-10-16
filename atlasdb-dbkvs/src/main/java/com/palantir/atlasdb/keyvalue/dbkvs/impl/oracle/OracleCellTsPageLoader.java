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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairInfo;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairToken;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// Unlike Postgres, Oracle doesn't properly support the "row value" syntax. If we want to start
// our scan from a given (row_name, col_name, ts) tuple, we can't say
//
//     WHERE (row_name, col_name, ts) >= (?, ?, ?)
//
// like we do with Postgres. Instead, we can try something like
//
//     WHERE (row_name = ? AND ((col_name = ? AND ts >= ?) OR col_name > ?)) OR row_name > ?      (*)
//     or
//     WHERE row_name >= ? AND (row_name > ? OR col_name > ? OR (col_name = ? AND ts >= ?))       (**)
//
// Neither is perfect: unfortunately, Oracle's optimizer won't realize that the query can be
// answered with a single range scan starting from a given (row_name, col_name, ts) tuple.
//
// For query (*), Oracle will do a concatenation of two range scans. Throwing in an ordering clause
// 'ORDER BY row_name, col_name, ts' will force Oracle to actually sort the rows after the concatenation,
// which is slow.
//
// For query (**), Oracle will do a single range scan using 'row_name >= ?' as the access predicate,
// but the rest of the condition will be used as the filter predicate. Even though no extra sorting
// is necessary afterwards, this means that all cells from the beginning of the row will be scanned.
// This way, paging through a very wide row would take quadratic time.
//
// As a workaround, we have two modes: in "normal" mode, we perform the (**)-like query. We maintain
// a counter of cells that we have already gone through in the current row. If the counter exceeds
// some threshold, we switch to the "single row" mode. In this mode, we do queries like
//
//     AND row_name = ? AND col_name >= ? AND (col_name > ? OR ts >= ?)
//
// For this query, Oracle does a range scan using 'row_name = ? AND col_name >= ?' as the access
// predicate and '(col_name > ? OR ts >= ?)' as the filter predicate. Once we exhaust all results
// in the row, we switch back to the "normal" mode, starting from the beginning of the lexicographically
// next row.
public class OracleCellTsPageLoader implements CellTsPairLoader {
    private final SqlConnectionSupplier connectionPool;
    private final OracleTableNameGetter tableNameGetter;
    private final TableValueStyleCache valueStyleCache;
    private final OracleDdlConfig config;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public OracleCellTsPageLoader(
            SqlConnectionSupplier connectionPool,
            OracleTableNameGetter tableNameGetter,
            TableValueStyleCache valueStyleCache,
            OracleDdlConfig config) {
        this.connectionPool = connectionPool;
        this.tableNameGetter = tableNameGetter;
        this.valueStyleCache = valueStyleCache;
        this.config = config;
    }

    @Override
    public Iterator<List<CellTsPairInfo>> createPageIterator(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        TableDetails tableDetails = getTableDetailsUsingNewConnection(tableRef);
        return new PageIterator(
                connectionPool,
                request,
                tableDetails,
                Math.max(1, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)),
                request.startRowInclusive());
    }

    private static class PageIterator implements Iterator<List<CellTsPairInfo>> {
        final SqlConnectionSupplier connectionPool;
        final CandidateCellForSweepingRequest request;
        final TableDetails tableDetails;
        final int sqlRowLimit;

        CellTsPairToken token;
        long cellTsPairsAlreadyExaminedInCurrentRow = 0L;

        PageIterator(
                SqlConnectionSupplier connectionPool,
                CandidateCellForSweepingRequest request,
                TableDetails tableDetails,
                int sqlRowLimit,
                byte[] startRowInclusive) {
            this.connectionPool = connectionPool;
            this.request = request;
            this.tableDetails = tableDetails;
            this.sqlRowLimit = sqlRowLimit;
            this.token = CellTsPairToken.startRow(startRowInclusive);
        }

        @Override
        public boolean hasNext() {
            return !token.reachedEnd();
        }

        // We don't use AbstractIterator to make sure hasNext() is fast and doesn't actually load the next page.
        // As a downside, our iterator can return an empty page at the end.
        // However, we can just filter out empty pages later.
        @Override
        public List<CellTsPairInfo> next() {
            Preconditions.checkState(hasNext());
            boolean singleRow = shouldScanSingleRow();
            List<CellTsPairInfo> cellTsPairs = loadPage(singleRow);
            updateCountOfExaminedCellTsPairsInCurrentRow(cellTsPairs);
            token = computeNextStartPosition(cellTsPairs, singleRow);
            return cellTsPairs;
        }

        private boolean shouldScanSingleRow() {
            // The idea is that we don't want to throw away more than N database rows in order to get N results.
            // This only matters for tables with wide rows. We could tweak this.
            return cellTsPairsAlreadyExaminedInCurrentRow > sqlRowLimit;
        }

        private List<CellTsPairInfo> loadPage(boolean singleRow) {
            FullQuery query = getQuery(singleRow);
            try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                    AgnosticLightResultSet resultSet = executeQuery(conns.get(), query)) {
                List<CellTsPairInfo> ret = new ArrayList<>();
                for (AgnosticLightResultRow row : resultSet) {
                    ret.add(new CellTsPairInfo(
                            row.getBytes("row_name"),
                            row.getBytes("col_name"),
                            row.getLong("ts"),
                            request.shouldCheckIfLatestValueIsEmpty() && row.getInteger("empty_val") == 1));
                }
                return ret;
            }
        }

        private FullQuery getQuery(boolean singleRow) {
            String pkIndex = PrimaryKeyConstraintNames.get(tableDetails.shortName);
            FullQuery.Builder queryBuilder = FullQuery.builder()
                    .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING */ ")
                    .append("SELECT * FROM (")
                    .append("  SELECT /*+ INDEX_ASC(t ")
                    .append(pkIndex)
                    .append(") ")
                    .append("             NO_INDEX_SS(t ")
                    .append(pkIndex)
                    .append(")")
                    .append("             NO_INDEX_FFS(t ")
                    .append(pkIndex)
                    .append(") */")
                    .append("  row_name, col_name, ts");
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                appendEmptyValueFlagSelector(queryBuilder);
            }
            queryBuilder
                    .append("  FROM ")
                    .append(tableDetails.shortName)
                    .append(" t")
                    .append("  WHERE ts < ? ", request.maxTimestampExclusive());
            SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
            appendRangePredicates(singleRow, queryBuilder);
            return queryBuilder
                    .append("  ORDER BY row_name, col_name, ts")
                    .append(") WHERE rownum <= ")
                    .append(sqlRowLimit)
                    .append(" ORDER BY row_name, col_name, ts")
                    .build();
        }

        private void appendRangePredicates(boolean singleRow, FullQuery.Builder builder) {
            if (singleRow) {
                builder.append(" AND row_name = ?", token.startRowInclusive());
                if (token.startColInclusive().length > 0) {
                    builder.append(" AND col_name >= ?", token.startColInclusive());
                    if (token.startTsInclusive() != null) {
                        builder.append(
                                " AND (col_name > ? OR ts >= ?)", token.startColInclusive(), token.startTsInclusive());
                    }
                }
            } else {
                RangePredicateHelper.create(false, DBType.ORACLE, builder)
                        .startCellTsInclusive(
                                token.startRowInclusive(), token.startColInclusive(), token.startTsInclusive());
            }
        }

        private void appendEmptyValueFlagSelector(FullQuery.Builder builder) {
            // One can't simply select a boolean in Oracle... We need "CASE WHEN" to cast a boolean to a number.
            // Also note that we don't need to check whether 'val' is "empty" -- Oracle always converts empty
            // RAW values into NULLs.
            builder.append(", CASE WHEN val IS NULL");
            if (tableDetails.hasOverflow) {
                builder.append(" AND overflow IS NULL");
            }
            builder.append(" THEN 1 ELSE 0 END AS empty_val");
        }

        private AgnosticLightResultSet executeQuery(SqlConnection conn, FullQuery query) {
            return conn.selectLightResultSetUnregisteredQueryWithFetchSize(
                    query.getQuery(), sqlRowLimit, query.getArgs());
        }

        private void updateCountOfExaminedCellTsPairsInCurrentRow(List<CellTsPairInfo> results) {
            byte[] currentRow = token.startRowInclusive();
            for (CellTsPairInfo cellTs : results) {
                if (!Arrays.equals(currentRow, cellTs.rowName)) {
                    currentRow = cellTs.rowName;
                    cellTsPairsAlreadyExaminedInCurrentRow = 0L;
                }
                cellTsPairsAlreadyExaminedInCurrentRow += 1;
            }
        }

        private CellTsPairToken computeNextStartPosition(List<CellTsPairInfo> results, boolean scannedSingleRow) {
            if (results.size() < sqlRowLimit) {
                if (scannedSingleRow) {
                    // If we scanned a single row and reached the end, we just restart
                    // from the lexicographically next row
                    byte[] nextRow = RangeRequests.getNextStartRowUnlessTerminal(false, token.startRowInclusive());
                    if (nextRow == null) {
                        return CellTsPairToken.end();
                    } else {
                        cellTsPairsAlreadyExaminedInCurrentRow = 0L;
                        return CellTsPairToken.startRow(nextRow);
                    }
                } else {
                    return CellTsPairToken.end();
                }
            } else {
                CellTsPairInfo lastResult = Iterables.getLast(results);
                return CellTsPairToken.continueRow(lastResult);
            }
        }
    }

    private static class TableDetails {
        final String shortName;
        final boolean hasOverflow;

        TableDetails(String shortName, boolean hasOverflow) {
            this.shortName = shortName;
            this.hasOverflow = hasOverflow;
        }
    }

    private TableDetails getTableDetailsUsingNewConnection(TableReference tableRef) {
        try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool)) {
            String shortName = tableNameGetter.getInternalShortTableName(conns, tableRef);
            TableValueStyle style = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
            boolean hasOverflow = style == TableValueStyle.OVERFLOW;
            return new TableDetails(shortName, hasOverflow);
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
