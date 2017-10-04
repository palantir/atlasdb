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

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeBoundPredicates;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.DbKvsGetCandidateCellsForSweeping;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

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
            CandidateCellForSweepingRequest request,
            KeyValueService kvs) {
        String tableName = DbKvs.internalTableName(tableRef);
        PageIterator rawIterator = new PageIterator(
                request,
                request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE),
                tableName,
                prefixedTableNames.get(tableRef),
                request.startRowInclusive());
        return ClosableIterators.wrap(
                // Pages returned from rawIterator can be small or even empty,
                // so we need to re-partition them
                new CandidatePageJoiningIterator(rawIterator, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)));
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CandidateCellForSweepingRequest request;
        private final int sqlRowLimit;
        private final String tableName;
        private final String prefixedTableName;

        private byte[] currentRowName;
        private byte[] currentColName = PtBytes.EMPTY_BYTE_ARRAY;
        private Long firstCellStartTimestampInclusive = null;
        private boolean reachedEnd = false;
        private final TLongList currentCellTimestamps = new TLongArrayList();
        private boolean currentIsLatestValueEmpty = false;
        private long cellTsPairsExaminedTotal = 0;
        private long cellTsPairsExaminedCurrentBatch = 0;

        PageIterator(CandidateCellForSweepingRequest request, int sqlRowLimit, String tableName,
                String prefixedTableName, byte[] currentRowName) {
            this.request = request;
            this.sqlRowLimit = sqlRowLimit;
            this.tableName = tableName;
            this.prefixedTableName = prefixedTableName;
            this.currentRowName = currentRowName;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected List<CandidateCellForSweeping> computeNext() {
            if (reachedEnd) {
                return endOfData();
            } else {
                try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                        ClosableIterator<AgnosticLightResultRow> iter = selectNextPage(conns)) {
                    List<CandidateCellForSweeping> results = new ArrayList<>();
                    boolean noResults = true;
                    cellTsPairsExaminedCurrentBatch = 0;
                    while (iter.hasNext()) {
                        noResults = false;
                        AgnosticLightResultRow sqlRow = iter.next();
                        byte[] rowName = sqlRow.getBytes("row_name");
                        byte[] colName = sqlRow.getBytes("col_name");
                        if (!isCurrentCell(rowName, colName)) {
                            getCurrentCandidate().ifPresent(results::add);
                            currentCellTimestamps.clear();
                            currentRowName = rowName;
                            currentColName = colName;
                        }
                        if (request.shouldCheckIfLatestValueIsEmpty()) {
                            // We group timestamps in an array when we check the latest values
                            Object[] timestamps = (Object[]) sqlRow.getArray("timestamps");
                            cellTsPairsExaminedCurrentBatch += timestamps.length;
                            for (Object ts : timestamps) {
                                currentCellTimestamps.add((Long) ts);
                            }
                            currentIsLatestValueEmpty = sqlRow.getBoolean("latest_val_empty");
                        } else {
                            // Otherwise, we return one row per (cell, ts) pair
                            cellTsPairsExaminedCurrentBatch += 1;
                            currentCellTimestamps.add(sqlRow.getLong("ts"));
                        }
                    }
                    if (noResults || cellTsPairsExaminedCurrentBatch < sqlRowLimit) {
                        getCurrentCandidate().ifPresent(results::add);
                        reachedEnd = true;
                    } else {
                        computeNextStartPosition();
                    }
                    cellTsPairsExaminedTotal += cellTsPairsExaminedCurrentBatch;
                    return results;
                }
            }
        }

        private boolean isCurrentCell(byte[] rowName, byte[] colName) {
            return Arrays.equals(currentRowName, rowName) && Arrays.equals(currentColName, colName);
        }

        private void computeNextStartPosition() {
            long lastTs = currentCellTimestamps.get(currentCellTimestamps.size() - 1);
            // This can never happen because we request 'WHERE ts < ?', where '?' is some 'long' value.
            // So if the timestamp is strictly less than another 'long', it can not be Long.MAX_VALUE.
            // But we check anyway for general paranoia reasons.
            Preconditions.checkState(lastTs != Long.MAX_VALUE);
            firstCellStartTimestampInclusive = lastTs + 1;
        }

        private Optional<CandidateCellForSweeping> getCurrentCandidate() {
            if (currentCellTimestamps.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(ImmutableCandidateCellForSweeping.builder()
                        .cell(Cell.create(currentRowName, currentColName))
                        .sortedTimestamps(getSortedTimestamps())
                        .isLatestValueEmpty(currentIsLatestValueEmpty)
                        .numCellsTsPairsExamined(cellTsPairsExaminedTotal + cellTsPairsExaminedCurrentBatch)
                        .build());
            }
        }

        private long[] getSortedTimestamps() {
            long[] sortedTimestamps = currentCellTimestamps.toArray();
            Arrays.sort(sortedTimestamps);
            return sortedTimestamps;
        }

        private ClosableIterator<AgnosticLightResultRow> selectNextPage(ConnectionSupplier conns) {
            FullQuery query = getQuery();
            AgnosticLightResultSet rs = conns.get().selectLightResultSetUnregisteredQuery(
                    query.getQuery(), query.getArgs());
            return ClosableIterators.wrap(rs.iterator(), rs);
        }

        private FullQuery getQuery() {
            RangeBoundPredicates bounds = RangeBoundPredicates.builder(false)
                    .startCellTsInclusive(currentRowName, currentColName, firstCellStartTimestampInclusive)
                    .build();
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING_THOROUGH(" + tableName + ") */"
                        + "  SELECT cells.row_name, cells.col_name, cells.timestamps, "
                        + "         length(v.val) = 0 AS latest_val_empty"
                        + "  FROM ("
                        + "    SELECT"
                        + "      row_name, col_name, MAX(ts) AS max_ts, ARRAY_AGG(ts) AS timestamps"
                        + "    FROM ("
                        + "      SELECT row_name, col_name, ts"
                        + "      FROM " + prefixedTableName
                        + "      WHERE ts < ? " + bounds.predicates + getIgnoredTimestampPredicate()
                        + "      ORDER BY row_name, col_name, ts"
                        + "      LIMIT " + sqlRowLimit
                        + "    ) sub"
                        + "    GROUP BY row_name, col_name"
                        + "    ORDER BY row_name, col_name"
                        + "  ) cells"
                        + "  JOIN " + prefixedTableName + " v"
                        + "  ON cells.row_name = v.row_name"
                        + "  AND cells.col_name = v.col_name"
                        + "  AND cells.max_ts = v.ts"
                        + "  ORDER BY cells.row_name, cells.col_name";
                return new FullQuery(query)
                        .withArg(request.sweepTimestamp()) // "WHERE ts < ?"
                        .withArgs(bounds.args);
            } else {
                String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING_CONSERVATIVE(" + tableName + ") */"
                        + "  SELECT row_name, col_name, ts"
                        + "  FROM " + prefixedTableName
                        + "  WHERE ts < ? " + bounds.predicates + getIgnoredTimestampPredicate()
                        + "  ORDER BY row_name, col_name, ts"
                        + "  LIMIT " + sqlRowLimit;
                return new FullQuery(query)
                        .withArg(request.sweepTimestamp()) // "WHERE ts < ?"
                        .withArgs(bounds.args);
            }
        }

        private String getIgnoredTimestampPredicate() {
            StringBuilder ret = new StringBuilder();
            for (long ts : request.timestampsToIgnore()) {
                // In practice this will always be -1, so we don't bother with binds
                ret.append(" AND ts <> ").append(ts);
            }
            return ret.toString();
        }
    }

}
