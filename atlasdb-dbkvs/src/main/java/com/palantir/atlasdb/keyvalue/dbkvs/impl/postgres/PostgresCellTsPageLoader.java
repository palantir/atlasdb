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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairInfo;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CellTsPairToken;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PostgresCellTsPageLoader implements CellTsPairLoader {

    private final PostgresPrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public PostgresCellTsPageLoader(
            PostgresPrefixedTableNames prefixedTableNames, SqlConnectionSupplier connectionPool) {
        this.prefixedTableNames = prefixedTableNames;
        this.connectionPool = connectionPool;
    }

    @Override
    public Iterator<List<CellTsPairInfo>> createPageIterator(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        return new PageIterator(
                connectionPool,
                request,
                Math.max(1, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)),
                DbKvs.internalTableName(tableRef),
                prefixedTableNames.get(tableRef),
                request.startRowInclusive());
    }

    private static class PageIterator implements Iterator<List<CellTsPairInfo>> {
        final SqlConnectionSupplier connectionPool;
        final CandidateCellForSweepingRequest request;
        final int sqlRowLimit;
        final String tableName;
        final String prefixedTableName;

        CellTsPairToken token;

        PageIterator(
                SqlConnectionSupplier connectionPool,
                CandidateCellForSweepingRequest request,
                int sqlRowLimit,
                String tableName,
                String prefixedTableName,
                byte[] startRowInclusive) {
            this.connectionPool = connectionPool;
            this.request = request;
            this.sqlRowLimit = sqlRowLimit;
            this.tableName = tableName;
            this.prefixedTableName = prefixedTableName;
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
            List<CellTsPairInfo> cellTsPairs = loadNextPage();
            token = computeNextStartPosition(cellTsPairs);
            return cellTsPairs;
        }

        private List<CellTsPairInfo> loadNextPage() {
            try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                    AgnosticLightResultSet resultSet = selectNextPage(conns)) {
                List<CellTsPairInfo> ret = new ArrayList<>();
                for (AgnosticLightResultRow row : resultSet) {
                    byte[] rowName = row.getBytes("row_name");
                    byte[] colName = row.getBytes("col_name");
                    if (request.shouldCheckIfLatestValueIsEmpty()) {
                        long[] sortedTimestamps = castAndSortTimestamps((Object[]) row.getArray("timestamps"));
                        boolean isLatestValEmpty = row.getBoolean("latest_val_empty");
                        for (int i = 0; i < sortedTimestamps.length - 1; ++i) {
                            ret.add(new CellTsPairInfo(rowName, colName, sortedTimestamps[i], false));
                        }
                        // For the maximum timestamp, we know whether its value is empty or not,
                        // so we handle it separately
                        ret.add(new CellTsPairInfo(
                                rowName, colName, sortedTimestamps[sortedTimestamps.length - 1], isLatestValEmpty));
                    } else {
                        long ts = row.getLong("ts");
                        ret.add(new CellTsPairInfo(rowName, colName, ts, false));
                    }
                }
                return ret;
            }
        }

        private AgnosticLightResultSet selectNextPage(ConnectionSupplier conns) {
            FullQuery fullQuery = getFullQuery();
            return conns.get().selectLightResultSetUnregisteredQuery(fullQuery.getQuery(), fullQuery.getArgs());
        }

        private FullQuery getFullQuery() {
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                FullQuery.Builder queryBuilder = FullQuery.builder()
                        .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING_THOROUGH(")
                        .append(tableName)
                        .append(") */")
                        .append("  SELECT cells.row_name, cells.col_name, cells.timestamps, ")
                        .append("         length(v.val) = 0 AS latest_val_empty")
                        .append("  FROM (")
                        .append("    SELECT")
                        .append("      row_name, col_name, MAX(ts) AS max_ts, ARRAY_AGG(ts) AS timestamps")
                        .append("    FROM (")
                        .append("      SELECT row_name, col_name, ts")
                        .append("      FROM ")
                        .append(prefixedTableName)
                        .append("      WHERE ts < ? ", request.maxTimestampExclusive());
                SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
                RangePredicateHelper.create(false, DBType.POSTGRESQL, queryBuilder)
                        .startCellTsInclusive(
                                token.startRowInclusive(), token.startColInclusive(), token.startTsInclusive());
                return queryBuilder
                        .append("      ORDER BY row_name, col_name, ts")
                        .append("      LIMIT ")
                        .append(sqlRowLimit)
                        .append("    ) sub")
                        .append("    GROUP BY row_name, col_name")
                        .append("    ORDER BY row_name, col_name")
                        .append("  ) cells")
                        .append("  JOIN ")
                        .append(prefixedTableName)
                        .append(" v")
                        .append("  ON cells.row_name = v.row_name")
                        .append("  AND cells.col_name = v.col_name")
                        .append("  AND cells.max_ts = v.ts")
                        .append("  ORDER BY cells.row_name, cells.col_name")
                        .build();
            } else {
                FullQuery.Builder queryBuilder = FullQuery.builder()
                        .append("/* GET_CANDIDATE_CELLS_FOR_SWEEPING_CONSERVATIVE(")
                        .append(tableName)
                        .append(" */")
                        .append("  SELECT row_name, col_name, ts")
                        .append("  FROM ")
                        .append(prefixedTableName)
                        .append("  WHERE ts < ? ", request.maxTimestampExclusive());
                SweepQueryHelpers.appendIgnoredTimestampPredicate(request, queryBuilder);
                RangePredicateHelper.create(false, DBType.POSTGRESQL, queryBuilder)
                        .startCellTsInclusive(
                                token.startRowInclusive(), token.startColInclusive(), token.startTsInclusive());
                return queryBuilder
                        .append("  ORDER BY row_name, col_name, ts")
                        .append("  LIMIT ")
                        .append(sqlRowLimit)
                        .build();
            }
        }

        private CellTsPairToken computeNextStartPosition(List<CellTsPairInfo> results) {
            if (results.size() < sqlRowLimit) {
                return CellTsPairToken.end();
            } else {
                CellTsPairInfo lastResult = Iterables.getLast(results);
                return CellTsPairToken.continueRow(lastResult);
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
