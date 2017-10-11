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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoaderFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

public class PostgresCellTsPageLoaderFactory implements CellTsPairLoaderFactory {

    private final PostgresPrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public PostgresCellTsPageLoaderFactory(PostgresPrefixedTableNames prefixedTableNames,
                                                SqlConnectionSupplier connectionPool) {
        this.prefixedTableNames = prefixedTableNames;
        this.connectionPool = connectionPool;
    }

    @Override
    public CellTsPairLoader createCellTsLoader(TableReference tableRef, CandidateCellForSweepingRequest request) {
        return new Loader(
                request,
                request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE),
                DbKvs.internalTableName(tableRef),
                prefixedTableNames.get(tableRef));
    }

    private class Loader implements CellTsPairLoader {
        final CandidateCellForSweepingRequest request;
        final int sqlRowLimit;
        final String tableName;
        final String prefixedTableName;

        Loader(CandidateCellForSweepingRequest request, int sqlRowLimit, String tableName, String prefixedTableName) {
            this.request = request;
            this.sqlRowLimit = sqlRowLimit;
            this.tableName = tableName;
            this.prefixedTableName = prefixedTableName;
        }

        @Override
        public Page loadNextPage(StartingPosition startInclusive,
                                 long cellsAlreadyExaminedInStartingRow) {
            try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                    AgnosticLightResultSet resultSet = selectNextPage(conns, startInclusive)) {
                List<CandidatePagingState.CellTsPairInfo> cellTsPairs = readResultSet(resultSet);
                return new Page(cellTsPairs, false, cellTsPairs.size() < sqlRowLimit);
            }
        }

        private AgnosticLightResultSet selectNextPage(ConnectionSupplier conns, StartingPosition startingPosition) {
            FullQuery query = getQuery(startingPosition);
            return conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
        }

        private List<CandidatePagingState.CellTsPairInfo> readResultSet(AgnosticLightResultSet resultSet) {
            List<CandidatePagingState.CellTsPairInfo> ret = new ArrayList<>();
            for (AgnosticLightResultRow row : resultSet) {
                byte[] rowName = row.getBytes("row_name");
                byte[] colName = row.getBytes("col_name");
                if (request.shouldCheckIfLatestValueIsEmpty()) {
                    long[] sortedTimestamps = castAndSortTimestamps((Object[]) row.getArray("timestamps"));
                    boolean isLatestValEmpty = row.getBoolean("latest_val_empty");
                    for (int i = 0; i < sortedTimestamps.length - 1; ++i) {
                        ret.add(new CandidatePagingState.CellTsPairInfo(rowName, colName, sortedTimestamps[i], false));
                    }
                    // For the maximum timestamp, we know whether its value is empty or not, so we handle it separately
                    ret.add(new CandidatePagingState.CellTsPairInfo(
                            rowName, colName, sortedTimestamps[sortedTimestamps.length - 1], isLatestValEmpty));
                } else {
                    long ts = row.getLong("ts");
                    ret.add(new CandidatePagingState.CellTsPairInfo(rowName, colName, ts, false));
                }
            }
            return ret;
        }

        private FullQuery getQuery(StartingPosition startingPos) {
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                FullQuery.Builder queryBuilder = FullQuery.builder()
                        .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING_THOROUGH(").append(tableName).append(") */")
                        .append("  SELECT cells.row_name, cells.col_name, cells.timestamps, ")
                        .append("         length(v.val) = 0 AS latest_val_empty")
                        .append("  FROM (")
                        .append("    SELECT")
                        .append("      row_name, col_name, MAX(ts) AS max_ts, ARRAY_AGG(ts) AS timestamps")
                        .append("    FROM (")
                        .append("      SELECT row_name, col_name, ts")
                        .append("      FROM ").append(prefixedTableName)
                        .append("      WHERE ts < ? ", request.sweepTimestamp());
                SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
                RangePredicateHelper.create(false, DBType.POSTGRESQL, queryBuilder)
                        .startCellTsInclusive(startingPos.rowName, startingPos.colName, startingPos.timestamp);
                return queryBuilder
                        .append("      ORDER BY row_name, col_name, ts")
                        .append("      LIMIT ").append(sqlRowLimit)
                        .append("    ) sub")
                        .append("    GROUP BY row_name, col_name")
                        .append("    ORDER BY row_name, col_name")
                        .append("  ) cells")
                        .append("  JOIN ").append(prefixedTableName).append(" v")
                        .append("  ON cells.row_name = v.row_name")
                        .append("  AND cells.col_name = v.col_name")
                        .append("  AND cells.max_ts = v.ts")
                        .append("  ORDER BY cells.row_name, cells.col_name")
                        .build();
            } else {
                FullQuery.Builder queryBuilder = FullQuery.builder()
                        .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING_CONSERVATIVE(").append(tableName).append(" */")
                        .append("  SELECT row_name, col_name, ts")
                        .append("  FROM ").append(prefixedTableName)
                        .append("  WHERE ts < ? ", request.sweepTimestamp());
                SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
                RangePredicateHelper.create(false, DBType.POSTGRESQL, queryBuilder)
                        .startCellTsInclusive(startingPos.rowName, startingPos.colName, startingPos.timestamp);
                return queryBuilder
                        .append("  ORDER BY row_name, col_name, ts")
                        .append("  LIMIT ").append(sqlRowLimit)
                        .build();
            }
        }
    }

    // Postgres doesn't guarantee the order of results of ARRAY_AGG, so we sort the timestamps ourselves.
    private static long[] castAndSortTimestamps(Object[] timestampJdbcArray) {
        long[] sortedTimestamps = new long[timestampJdbcArray.length];
        for (int i = 0; i < timestampJdbcArray.length; ++i) {
            sortedTimestamps[i] = (long) timestampJdbcArray[i];
        }
        Arrays.sort(sortedTimestamps);
        return sortedTimestamps;
    }

}
