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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.ArrayList;
import java.util.List;

import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.CellTsPairInfo;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoaderFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

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
// some threshold, we switch to the "single row" mode.
public class OracleCellTsPageLoaderFactory implements CellTsPairLoaderFactory {
    private final SqlConnectionSupplier connectionPool;
    private final OracleTableNameGetter tableNameGetter;
    private final TableValueStyleCache valueStyleCache;
    private final OracleDdlConfig config;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public OracleCellTsPageLoaderFactory(SqlConnectionSupplier connectionPool,
                                         OracleTableNameGetter tableNameGetter,
                                         TableValueStyleCache valueStyleCache,
                                         OracleDdlConfig config) {
        this.connectionPool = connectionPool;
        this.tableNameGetter = tableNameGetter;
        this.valueStyleCache = valueStyleCache;
        this.config = config;
    }

    @Override
    public CellTsPairLoader createCellTsLoader(TableReference tableRef, CandidateCellForSweepingRequest request) {
        TableDetails tableDetails = getTableDetailsUsingNewConnection(tableRef);
        return new Loader(connectionPool, request, tableDetails, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE));
    }

    private static class Loader implements CellTsPairLoader {
        private final CandidateCellForSweepingRequest request;
        private final TableDetails tableDetails;
        private final int sqlRowLimit;
        private SqlConnectionSupplier connectionPool;

        Loader(SqlConnectionSupplier connectionPool,
                CandidateCellForSweepingRequest request,
                TableDetails tableDetails,
                int sqlRowLimit) {
            this.connectionPool = connectionPool;
            this.request = request;
            this.tableDetails = tableDetails;
            this.sqlRowLimit = sqlRowLimit;
        }

        @Override
        public Page loadNextPage(StartingPosition startInclusive,
                                 long cellsAlreadyExaminedInStartingRow) {
            boolean singleRow = shouldScanSingleRow(cellsAlreadyExaminedInStartingRow);
            List<CellTsPairInfo> cellTsPairs = loadPage(startInclusive, singleRow);
            return new Page(cellTsPairs, singleRow, cellTsPairs.size() < sqlRowLimit);
        }

        private boolean shouldScanSingleRow(long cellsAlreadyExaminedInCurrentRow) {
            // The idea is that we don't want to throw away more than N database rows in order to get N results.
            // This only matters for tables with wide rows. We could tweak this.
            return cellsAlreadyExaminedInCurrentRow > sqlRowLimit;
        }

        private List<CellTsPairInfo> loadPage(StartingPosition startInclusive, boolean singleRow) {
            FullQuery query = getQuery(startInclusive, singleRow);
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

        private AgnosticLightResultSet executeQuery(SqlConnection conn, FullQuery query) {
            return conn.selectLightResultSetUnregisteredQueryWithFetchSize(
                    query.getQuery(),
                    sqlRowLimit,
                    query.getArgs());
        }

        private FullQuery getQuery(StartingPosition startingPos, boolean singleRow) {
            String pkIndex = PrimaryKeyConstraintNames.get(tableDetails.shortName);
            FullQuery.Builder queryBuilder = FullQuery.builder()
                    .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING */ ")
                    .append("SELECT * FROM (")
                    .append("  SELECT /*+ INDEX_ASC(t ").append(pkIndex).append(") ")
                    .append("             NO_INDEX_SS(t ").append(pkIndex).append(")")
                    .append("             NO_INDEX_FFS(t ").append(pkIndex).append(") */")
                    .append("  row_name, col_name, ts");
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                appendEmptyValueFlagSelector(queryBuilder);
            }
            queryBuilder
                    .append("  FROM ").append(tableDetails.shortName).append(" t")
                    .append("  WHERE ts < ? ", request.sweepTimestamp());
            SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
            appendRangePredicates(startingPos, singleRow, queryBuilder);
            return queryBuilder
                    .append("  ORDER BY row_name, col_name, ts")
                    .append(") WHERE rownum <= ").append(sqlRowLimit)
                    .append(" ORDER BY row_name, col_name, ts")
                    .build();
        }

        private void appendRangePredicates(StartingPosition startingPos, boolean singleRow, FullQuery.Builder builder) {
            if (singleRow) {
                builder.append(" AND row_name = ?", startingPos.rowName);
                if (startingPos.colName.length > 0) {
                    builder.append(" AND col_name >= ?", startingPos.colName);
                    if (startingPos.timestamp != null) {
                        builder.append(" AND (col_name > ? OR ts >= ?)", startingPos.colName, startingPos.timestamp);
                    }
                }
            } else {
                RangePredicateHelper.create(false, DBType.ORACLE, builder)
                        .startCellTsInclusive(startingPos.rowName, startingPos.colName, startingPos.timestamp);
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
