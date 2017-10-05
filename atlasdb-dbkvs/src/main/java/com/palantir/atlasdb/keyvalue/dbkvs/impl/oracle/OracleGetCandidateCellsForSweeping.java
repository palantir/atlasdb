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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntToLongFunction;

import org.slf4j.LoggerFactory;

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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OraclePrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.CandidatePageJoiningIterator;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeBoundPredicates;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.DbKvsGetCandidateCellsForSweeping;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

public class OracleGetCandidateCellsForSweeping implements DbKvsGetCandidateCellsForSweeping {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(OracleTableInitializer.class);

    private final OraclePrefixedTableNames prefixedTableNames;
    private final SqlConnectionSupplier connectionPool;

    private final IntToLongFunction sqlRowLimitProvider;
    private final long[] emptyLongArray = new long[] {};
    private static final int DEFAULT_BATCH_SIZE = 1000;

    public static OracleGetCandidateCellsForSweeping create(
            OraclePrefixedTableNames prefixedTableNames,
            SqlConnectionSupplier connectionPool) {
        return new OracleGetCandidateCellsForSweeping(
                prefixedTableNames,
                connectionPool,
                // Since in our SQL query we limit the number of examined cells rather than the number of
                // returned candidates, we set the limit higher than the requested batch size.
                batchHint -> 4 * batchHint);
    }

    @VisibleForTesting
    /* package */ OracleGetCandidateCellsForSweeping(
            OraclePrefixedTableNames prefixedTableNames,
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
                prefixedTableNames.get(tableRef, new ConnectionSupplier(connectionPool)),
                request.startRowInclusive());
        return ClosableIterators.wrap(
                // Pages returned from rawIterator can be small or even empty,
                // so we need to re-partition them
                new CandidatePageJoiningIterator(rawIterator, request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE)));
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CandidateCellForSweepingRequest request;
        private final int sqlRowLimit;
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
                        BigDecimal[] timestamps = (BigDecimal[]) sqlRow.getArray("timestamps");
                        for (BigDecimal ts : timestamps) {
                            currentCellTimestamps.add(ts.longValue());
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
            executeIgnoringError(conns, "CREATE TYPE num_list AS TABLE OF NUMBER(20)", "ORA-00955");

            FullQuery query = getQuery();
            AgnosticLightResultSet rs = conns.get().selectLightResultSetUnregisteredQuery(
                    query.getQuery(), query.getArgs());
            return ClosableIterators.wrap(rs.iterator(), rs);
        }

        private FullQuery getQuery() {
            RangeBoundPredicates bounds = RangeBoundPredicates.builder(false)
                    .startCellTsInclusiveOracle(currentRowName, currentColName, firstCellStartTimestampInclusive)
                    .build();
            boolean ignoreSentinels = areSentinelsIgnored();
            String query =  "SELECT cells.row_name, cells.col_name, cells.timestamps, cells.max_rn AS max_row_number"
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
                    + "      CAST(COLLECT(ts) AS num_list) timestamps"
                    + "    FROM ("
                    + "      SELECT row_name, col_name, ts, ROW_NUMBER() OVER (ORDER BY row_name, col_name, ts) AS rn"
                    + "      FROM " + prefixedTableName
                    + "      WHERE ts < ? " + bounds.predicates + getIgnoredTimestampPredicate()
                    + "      ORDER BY row_name, col_name, ts"
//                    + "      LIMIT " + sqlRowLimit
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

    private void executeIgnoringError(ConnectionSupplier conns, String sql, String errorToIgnore) {
        try {
            conns.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error("Error occurred trying to execute the query {}", sql, e);
                throw e;
            }
        }
    }


}
