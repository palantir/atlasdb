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
import java.util.Optional;

import com.google.common.collect.AbstractIterator;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeBoundPredicates;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.DbKvsGetCandidateCellsForSweeping;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

public class PostgresGetCandidateCellsForSweeping implements DbKvsGetCandidateCellsForSweeping {

    private final PostgresPrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public PostgresGetCandidateCellsForSweeping(PostgresPrefixedTableNames prefixedTableNames,
                                                SqlConnectionSupplier connectionPool) {
        this.prefixedTableNames = prefixedTableNames;
        this.connectionPool = connectionPool;
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        String tableName = DbKvs.internalTableName(tableRef);
        RequestWithDetails requestWithDetails = new RequestWithDetails(
                request,
                request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE),
                tableName,
                prefixedTableNames.get(tableRef));
        CandidatePagingState state = CandidatePagingState.create(request.startRowInclusive());
        PageIterator rawIterator = new PageIterator(requestWithDetails, state);
        return ClosableIterators.wrap(
                // Pages returned from rawIterator can be small or even empty,
                // so we need to re-partition them
                new CandidatePageJoiningIterator(rawIterator, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)));
    }

    private static class RequestWithDetails {
        final CandidateCellForSweepingRequest request;
        final int sqlRowLimit;
        final String tableName;
        final String prefixedTableName;

        RequestWithDetails(CandidateCellForSweepingRequest request, int sqlRowLimit, String tableName,
                String prefixedTableName) {
            this.request = request;
            this.sqlRowLimit = sqlRowLimit;
            this.tableName = tableName;
            this.prefixedTableName = prefixedTableName;
        }
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final RequestWithDetails requestWithDetails;
        private final CandidatePagingState state;

        PageIterator(RequestWithDetails requestWithDetails, CandidatePagingState state) {
            this.requestWithDetails = requestWithDetails;
            this.state = state;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<CandidateCellForSweeping> computeNext() {
            Optional<StartingPosition> startingPosition = state.getNextStartingPosition();
            if (startingPosition.isPresent()) {
                try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                        AgnosticLightResultSet resultSet = selectNextPage(conns, startingPosition.get())) {
                    List<CandidatePagingState.KvsEntryInfo> cellTsPairs = readResultSet(resultSet);
                    return state.processBatch(cellTsPairs, requestWithDetails.sqlRowLimit).candidates;
                }
            } else {
                return endOfData();
            }
        }

        private AgnosticLightResultSet selectNextPage(ConnectionSupplier conns, StartingPosition startingPosition) {
            FullQuery query = getQuery(requestWithDetails, startingPosition);
            return conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
        }

        private List<CandidatePagingState.KvsEntryInfo> readResultSet(AgnosticLightResultSet resultSet) {
            List<CandidatePagingState.KvsEntryInfo> ret = new ArrayList<>();
            for (AgnosticLightResultRow row : resultSet) {
                byte[] rowName = row.getBytes("row_name");
                byte[] colName = row.getBytes("col_name");
                if (requestWithDetails.request.shouldCheckIfLatestValueIsEmpty()) {
                    long[] sortedTimestamps = castAndSortTimestamps((Object[]) row.getArray("timestamps"));
                    boolean isLatestValEmpty = row.getBoolean("latest_val_empty");
                    for (int i = 0; i < sortedTimestamps.length - 1; ++i) {
                        ret.add(new CandidatePagingState.KvsEntryInfo(rowName, colName, sortedTimestamps[i], false));
                    }
                    // For the maximum timestamp, we know whether its value is empty or not, so we handle it separately
                    ret.add(new CandidatePagingState.KvsEntryInfo(
                            rowName, colName, sortedTimestamps[sortedTimestamps.length - 1], isLatestValEmpty));
                } else {
                    long ts = row.getLong("ts");
                    ret.add(new CandidatePagingState.KvsEntryInfo(rowName, colName, ts, false));
                }
            }
            return ret;
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


    private static FullQuery getQuery(RequestWithDetails rwd, StartingPosition startingPos) {
        RangeBoundPredicates bounds = RangeBoundPredicates.builder(DBType.POSTGRESQL, false)
                .startCellTsInclusive(startingPos.rowName, startingPos.colName, startingPos.timestamp)
                .build();
        if (rwd.request.shouldCheckIfLatestValueIsEmpty()) {
            String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING_THOROUGH(" + rwd.tableName + ") */"
                    + "  SELECT cells.row_name, cells.col_name, cells.timestamps, "
                    + "         length(v.val) = 0 AS latest_val_empty"
                    + "  FROM ("
                    + "    SELECT"
                    + "      row_name, col_name, MAX(ts) AS max_ts, ARRAY_AGG(ts) AS timestamps"
                    + "    FROM ("
                    + "      SELECT row_name, col_name, ts"
                    + "      FROM " + rwd.prefixedTableName
                    + "      WHERE ts < ? " + bounds.predicates
                    +           SweepQueryHelpers.getIgnoredTimestampPredicate(rwd.request)
                    + "      ORDER BY row_name, col_name, ts"
                    + "      LIMIT " + rwd.sqlRowLimit
                    + "    ) sub"
                    + "    GROUP BY row_name, col_name"
                    + "    ORDER BY row_name, col_name"
                    + "  ) cells"
                    + "  JOIN " + rwd.prefixedTableName + " v"
                    + "  ON cells.row_name = v.row_name"
                    + "  AND cells.col_name = v.col_name"
                    + "  AND cells.max_ts = v.ts"
                    + "  ORDER BY cells.row_name, cells.col_name";
            return new FullQuery(query)
                    .withArg(rwd.request.sweepTimestamp()) // "WHERE ts < ?"
                    .withArgs(bounds.args);
        } else {
            String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING_CONSERVATIVE(" + rwd.tableName + ") */"
                    + "  SELECT row_name, col_name, ts"
                    + "  FROM " + rwd.prefixedTableName
                    + "  WHERE ts < ? " + bounds.predicates
                    +       SweepQueryHelpers.getIgnoredTimestampPredicate(rwd.request)
                    + "  ORDER BY row_name, col_name, ts"
                    + "  LIMIT " + rwd.sqlRowLimit;
            return new FullQuery(query)
                    .withArg(rwd.request.sweepTimestamp()) // "WHERE ts < ?"
                    .withArgs(bounds.args);
        }
    }
}
