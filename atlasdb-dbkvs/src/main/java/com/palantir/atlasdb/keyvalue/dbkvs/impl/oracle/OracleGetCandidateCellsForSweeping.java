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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeBoundPredicates;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.BatchResult;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.DbKvsGetCandidateCellsForSweeping;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.SweepQueryHelpers;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public class OracleGetCandidateCellsForSweeping implements DbKvsGetCandidateCellsForSweeping {
    private final SqlConnectionSupplier connectionPool;
    private final OracleTableNameGetter tableNameGetter;
    private final TableValueStyleCache valueStyleCache;
    private final OracleDdlConfig config;

    private static final int DEFAULT_BATCH_SIZE = 1000;

    public OracleGetCandidateCellsForSweeping(SqlConnectionSupplier connectionPool,
                                              OracleTableNameGetter tableNameGetter,
                                              TableValueStyleCache valueStyleCache,
                                              OracleDdlConfig config) {
        this.connectionPool = connectionPool;
        this.tableNameGetter = tableNameGetter;
        this.valueStyleCache = valueStyleCache;
        this.config = config;
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        TableDetails tableDetails = getTableDetailsUsingNewConnection(tableRef);
        PageIterator rawIterator = new PageIterator(
                request,
                tableDetails,
                request.batchSizeHint().orElse(DEFAULT_BATCH_SIZE),
                CandidatePagingState.create(request.startRowInclusive()));
        return ClosableIterators.wrap(rawIterator);
    }

    private static class TableDetails {
        final String shortName;
        final boolean hasOverflow;

        public TableDetails(String shortName, boolean hasOverflow) {
            this.shortName = shortName;
            this.hasOverflow = hasOverflow;
        }
    }

    private TableDetails getTableDetailsUsingNewConnection(TableReference tableRef) {
        try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool)) {
            String shortName = tableNameGetter.getInternalShortTableName(conns, tableRef);
            boolean hasOverflow = valueStyleCache.getTableType(conns, tableRef, config.metadataTable()) == TableValueStyle.OVERFLOW;
            return new TableDetails(shortName, hasOverflow);
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CellsInCurrentRowTracker {
        byte[] currentRowName = PtBytes.EMPTY_BYTE_ARRAY;
        long cellsAlreadyExaminedInCurrentRow = 0L;

        void visitBatch(List<CandidateCellForSweeping> batch) {
            for (CandidateCellForSweeping candidate : batch) {
                if (!Arrays.equals(candidate.cell().getRowName(), currentRowName)) {
                    currentRowName = candidate.cell().getRowName();
                    cellsAlreadyExaminedInCurrentRow = 0L;
                }
                cellsAlreadyExaminedInCurrentRow++;
            }
        }

        void reset() {
            cellsAlreadyExaminedInCurrentRow = 0L;
        }
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CandidateCellForSweepingRequest request;
        private final TableDetails tableDetails;
        private final int sqlRowLimit;

        private final CandidatePagingState state;
        private final CellsInCurrentRowTracker cellsInCurrentRowTracker = new CellsInCurrentRowTracker();

        PageIterator(CandidateCellForSweepingRequest request, TableDetails tableDetails, int sqlRowLimit,
                CandidatePagingState state) {
            this.request = request;
            this.tableDetails = tableDetails;
            this.sqlRowLimit = sqlRowLimit;
            this.state = state;
        }

        @Override
        protected List<CandidateCellForSweeping> computeNext() {
            Optional<StartingPosition> startingPosition = state.getNextStartingPosition();
            if (startingPosition.isPresent()) {
                boolean singleRow = shouldScanSingleRow();
                List<CandidatePagingState.KvsEntryInfo> cellTsPairs = loadNextPage(startingPosition.get(), singleRow);
                BatchResult batch = state.processBatch(cellTsPairs, sqlRowLimit);
                cellsInCurrentRowTracker.visitBatch(batch.candidates);
                if (singleRow && batch.reachedEnd) {
                    // If we reached the end of results in the single-row mode, it doesn't mean that we reached
                    // the end of the table. We need to restart from the next row in the normal mode.
                    cellsInCurrentRowTracker.reset();
                    state.restartFromNextRow();
                }
                return batch.candidates;
            } else {
                return endOfData();
            }
        }

        private boolean shouldScanSingleRow() {
            // The idea is that we don't want to throw away more than N database rows in order to get N results.
            // This only matters for tables with wide rows. We could tweak this.
            return cellsInCurrentRowTracker.cellsAlreadyExaminedInCurrentRow > sqlRowLimit;
        }

        private List<CandidatePagingState.KvsEntryInfo> loadNextPage(StartingPosition startingPosition,
                                                                     boolean singleRow) {
            FullQuery query = getQuery(startingPosition, singleRow);
            try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool);
                  AgnosticLightResultSet resultSet = executeQuery(conns.get(), query)) {
                List<CandidatePagingState.KvsEntryInfo> ret = new ArrayList<>();
                for (AgnosticLightResultRow row : resultSet) {
                    ret.add(new CandidatePagingState.KvsEntryInfo(
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
            FullQuery rangePredicates = getRangePredicates(startingPos, singleRow);
            String query = "/* GET_CANDIDATE_CELLS_FOR_SWEEPING */ "
                    + "SELECT * FROM ("
                    + "  SELECT /*+ INDEX_ASC(t " + pkIndex + ") "
                    + "             NO_INDEX_SS(t " + pkIndex + ")"
                    + "             NO_INDEX_FFS(t " + pkIndex + ") */"
                    + "  row_name, col_name, ts" + getEmptyValueFlagSelector()
                    + "  FROM " + tableDetails.shortName + " t"
                    + "  WHERE ts < ? " + SweepQueryHelpers.getIgnoredTimestampPredicate(request)
                    +       rangePredicates.getQuery()
                    + "  ORDER BY row_name, col_name, ts"
                    + ") WHERE rownum <= " + sqlRowLimit
                    + " ORDER BY row_name, col_name, ts";
                    ;
            return new FullQuery(query).withArg(request.sweepTimestamp()).withArgs(rangePredicates.getArgList());
        }

        private FullQuery getRangePredicates(StartingPosition startingPos, boolean singleRow) {
            if (singleRow) {
                List<Object> args = Lists.newArrayListWithCapacity(3);
                StringBuilder q = new StringBuilder(" AND row_name = ?");
                args.add(startingPos.rowName);
                if (startingPos.colName.length > 0) {
                    q.append(" AND col_name >= ?");
                    args.add(startingPos.colName);
                    if (startingPos.timestamp != null) {
                        q.append(" AND (col_name > ? OR ts >= ?)");
                        args.add(startingPos.colName);
                        args.add(startingPos.timestamp);
                    }
                }
                return new FullQuery(q.toString()).withArgs(args);
            } else {
                return RangeBoundPredicates.builder(DBType.ORACLE, false)
                        .startCellTsInclusive(startingPos.rowName, startingPos.colName, startingPos.timestamp)
                        .build()
                        .asFullQuery();
            }
        }

        private String getEmptyValueFlagSelector() {
            if (request.shouldCheckIfLatestValueIsEmpty()) {
                StringBuilder ret = new StringBuilder();
                // One can't simply select a boolean in Oracle... We need "CASE WHEN" to cast a boolean to a number.
                // Also note that we don't need to check whether 'val' is "empty" -- Oracle always converts empty
                // RAW values into NULLs.
                ret.append(", CASE WHEN val IS NULL");
                if (tableDetails.hasOverflow) {
                    ret.append(" AND overflow IS NULL");
                }
                ret.append(" THEN 1 ELSE 0 END AS empty_val");
                return ret.toString();
            } else {
                return "";
            }
        }

    }
}
