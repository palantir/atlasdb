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
import java.util.function.IntToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
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

// Two considerations that influenced the implementation:
//
//   1. Window functions (OVER) on Postgres seem to be significantly slower than equivalent aggregates (GROUP BY).
//
//   2. Limiting an outer query doesn't seem to work well, e.g.:
//          SELECT ... FROM (
//              SELECT ... FROM my_table
//              ...
//              ORDER BY row_name, col_name, ts
//              ...
//          ) sub
//          WHERE ...
//          ORDER BY row_name, col_name, ts
//          LIMIT 1000
//      Postgres will often choose a full table scan for the inner SELECT query, even when an index
//      range scan is sufficient (because of the limit on the outer query). Postgres also doesn't
//      have hints like Oracle, so one can't force it to use a certain plan. A trick with specifying
//      a very large limit for the inner query seems to work, hinting the optimizer that only
//      the first few rows matter:
//          SELECT ... FROM (
//              SELECT ... FROM my_table
//              ...
//              ORDER BY row_name, col_name, ts
//              ...
//              LIMIT 100000000000000000000 -- a very large number that is guaranteed to exceed the table size
//          ) sub
//          WHERE ...
//          ORDER BY row_name, col_name, ts
//          LIMIT 1000
//      However, this looks very hacky and fragile, so instead we resort to simply limiting the inner query
//      (i.e., limiting the number of examined cells rather than the number of returned candidates).
//
// So, our SQL query does the following:
//
//   1. Grab a page of SQL rows, starting at some (row_name, col_name, ts).
//   2. Number the rows, starting with 1.
//   3. Group the rows by cell key, i.e. (row_name, col_name), and compute a few aggregates:
//          - Minimum and maximum row number
//          - Minimum and maximum timestamp
//          - An array of all timestamps (using ARRAY_AGG)
//   4. If the THOROUGH strategy is being used, do a self-join to check if the values are empty.
//   5. Filter out cells that don't satisfy the definition of a candidate, but keep the first
//      and last SQL row in each page (except for the last page).
//
//
public class PostgresGetCandidateCellsForSweeping implements DbKvsGetCandidateCellsForSweeping {

    private final PostgresPrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;
    private final IntToLongFunction sqlRowLimitProvider;

    private final long[] emptyLongArray = new long[] {};
    private static final int DEFAULT_BATCH_SIZE = 1000;

    public static PostgresGetCandidateCellsForSweeping create(
            PostgresPrefixedTableNames prefixedTableNames,
            SqlConnectionSupplier connectionPool) {
        return new PostgresGetCandidateCellsForSweeping(
                prefixedTableNames,
                connectionPool,
                // Since in our SQL query we limit the number of examined cells rather than the number of
                // returned candidates, we set the limit higher than the requested batch size.
                batchHint -> 4 * batchHint);
    }

    @VisibleForTesting
    /* package */ PostgresGetCandidateCellsForSweeping(
            PostgresPrefixedTableNames prefixedTableNames,
            SqlConnectionSupplier connectionPool,
            IntToLongFunction sqlRowLimitProvider) {
        this.prefixedTableNames = prefixedTableNames;
        this.connectionPool = connectionPool;
        this.sqlRowLimitProvider = sqlRowLimitProvider;
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request,
            KeyValueService kvs) {
        String tableName = DbKvs.internalTableName(tableRef);
        PageIterator rawIterator = new PageIterator(
                request,
                (int) sqlRowLimitProvider.applyAsLong(request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)),
                tableName,
                prefixedTableNames.get(tableRef),
                request.startRowInclusive());
        return ClosableIterators.wrap(
                // Pages returned from rawIterator can be small or even empty,
                // so we need to re-partition them
                new PageJoiningIterator<>(rawIterator, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)));
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CandidateCellForSweepingRequest request;
        private final int sqlRowLimit;
        private final String tableName;
        private final String prefixedTableName;

        private byte[] currentRowName;
        private byte[] currentColName = PtBytes.EMPTY_BYTE_ARRAY;
        private Long firstCellStartTimestampInclusive = null;
        private boolean endOfResults = false;
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
            if (endOfResults) {
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
                        Object[] timestamps = (Object[]) sqlRow.getArray("timestamps");
                        for (Object ts : timestamps) {
                            currentCellTimestamps.add((Long) ts);
                        }
                        cellTsPairsExaminedCurrentBatch = sqlRow.getLong("max_row_number");
                        if (request.shouldCheckIfLatestValueIsEmpty()) {
                            currentIsLatestValueEmpty = sqlRow.getBoolean("latest_val_empty");
                        }
                    }
                    if (noResults || cellTsPairsExaminedCurrentBatch < sqlRowLimit) {
                        getCurrentCandidate().ifPresent(results::add);
                        endOfResults = true;
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
            if (isCandidate()) {
                long[] sortedTimestamps = currentCellTimestamps.toArray();
                Arrays.sort(sortedTimestamps);
                return sortedTimestamps;
            } else {
                return emptyLongArray;
            }
        }

        private boolean isCandidate() {
            return currentCellTimestamps.size() > 1
                    || currentCellTimestamps.get(currentCellTimestamps.size() - 1)
                            >= request.minUncommittedStartTimestamp()
                    || currentCellTimestamps.contains(Value.INVALID_VALUE_TIMESTAMP)
                    || currentIsLatestValueEmpty;
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
            boolean ignoreSentinels = areSentinelsIgnored();
            String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING(" + tableName + ") */"
                    + "  SELECT cells.row_name, cells.col_name, cells.timestamps, cells.max_rn AS max_row_number"
                    +    (request.shouldCheckIfLatestValueIsEmpty() ? ", length(v.val) = 0 AS latest_val_empty" : "")
                    + "  FROM ("
                    + "    SELECT"
                    + "      row_name,"
                    + "      col_name,"
                    + "      MIN(rn) AS min_rn,"
                    + "      MAX(rn) AS max_rn,"
                    + "      MIN(ts) AS min_ts,"
                    + "      MAX(ts) AS max_ts,"
                    +        (ignoreSentinels
                                ? ""
                                : " MAX((ts=" + Value.INVALID_VALUE_TIMESTAMP + ")::int) AS have_sentinel,")
                    + "      ARRAY_AGG(ts) AS timestamps"
                    + "    FROM ("
                    + "      SELECT row_name, col_name, ts, ROW_NUMBER() OVER (ORDER BY row_name, col_name, ts) AS rn"
                    + "      FROM " + prefixedTableName
                    + "      WHERE ts < ? " + bounds.predicates + getIgnoredTimestampPredicate()
                    + "      ORDER BY row_name, col_name, ts"
                    + "      LIMIT " + sqlRowLimit
                    + "    ) sub"
                    + "    GROUP BY row_name, col_name"
                    + "    ORDER BY row_name, col_name"
                    + "  ) cells"
                    +    (request.shouldCheckIfLatestValueIsEmpty()
                            ? "  JOIN " + prefixedTableName + " v"
                              + "  ON cells.row_name = v.row_name"
                              + "  AND cells.col_name = v.col_name"
                              + "  AND cells.max_ts = v.ts"
                            : "")
                    + "  WHERE"
                      // See KVS.getCandidateCellsForSweeping() docs for the definition of a candidate cell:
                      // (1) The set T has more than one element
                    + "    min_ts <> max_ts"
                      // (2) The set T contains an element that is greater than or equal to Tu
                    + "    OR max_ts >= ?"
                      // (3) The set T contains Value.INVALID_VALUE_TIMESTAMP
                    +      (ignoreSentinels ? "" : " OR have_sentinel = 1")
                      // (4) V is true and the cell value corresponding to the maximum element of T is empty
                    +      (request.shouldCheckIfLatestValueIsEmpty() ? " OR length(v.val) = 0" : "")
                      // Also, always get the first cell, as well as the last one if the limit was reached
                    + "    OR min_rn = 1 OR max_rn = " + sqlRowLimit
                    + "  ORDER BY cells.row_name, cells.col_name";
            return new FullQuery(query)
                    .withArg(request.sweepTimestamp()) // "WHERE ts < ?"
                    .withArgs(bounds.args)
                    .withArg(request.minUncommittedStartTimestamp()); // "OR max_ts >= ?"
        }

        private boolean areSentinelsIgnored() {
            return Longs.contains(request.timestampsToIgnore(), Value.INVALID_VALUE_TIMESTAMP);
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
