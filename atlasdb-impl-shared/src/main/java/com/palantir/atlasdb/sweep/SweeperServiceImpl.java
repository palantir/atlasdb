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
package com.palantir.atlasdb.sweep;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.remoting2.servers.jersey.WebPreconditions;

public final class SweeperServiceImpl implements SweeperService {
    private static final int DEFAULT_SWEEP_READ_LIMIT = 1_000_000;
    private static final int DEFAULT_SWEEP_CANDIDATE_BATCH_HINT = 100;
    private static final int DEFAULT_SWEEP_DELETE_BATCH_HINT = 1000;

    private SpecificTableSweeper specificTableSweeper;

    public SweeperServiceImpl(SpecificTableSweeper specificTableSweeper) {
        this.specificTableSweeper = specificTableSweeper;
    }

    @Override
    public void sweepTable(String tableName) {
        TableReference tableRef = getTableRef(tableName);
        checkTableExists(tableName, tableRef);

        runSweepWithoutSavingResults(tableRef);
    }

    @Override
    public void sweepTableFromStartRow(String tableName, @Nonnull String startRow) {
        WebPreconditions.checkArgument(startRow != null, "startRow must not be null.");

        TableReference tableRef = getTableRef(tableName);
        checkTableExists(tableName, tableRef);

        ImmutableSweepProgress sweepProgress = getSweepProgress(startRow, tableRef);

        runSweepWithoutSavingResults(tableRef, sweepProgress);
    }

    @Override
    public void sweepTableFromStartRowWithBatchConfig(String tableName,
            @Nullable String startRow,
            @Nullable Integer maxCellTsPairsToExamine,
            @Nullable Integer candidateBatchSize,
            @Nullable Integer deleteBatchSize) {
        TableReference tableRef = getTableRef(tableName);
        checkTableExists(tableName, tableRef);

        ImmutableSweepProgress sweepProgress = getSweepProgress(startRow, tableRef);

        WebPreconditions.checkArgument(
                !(maxCellTsPairsToExamine == null && candidateBatchSize == null && deleteBatchSize == null),
                "No batch size config parameters were provided");

        ImmutableSweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(
                        maxCellTsPairsToExamine == null ? DEFAULT_SWEEP_READ_LIMIT : maxCellTsPairsToExamine)
                .candidateBatchSize(
                        candidateBatchSize == null ? DEFAULT_SWEEP_CANDIDATE_BATCH_HINT : candidateBatchSize)
                .deleteBatchSize(deleteBatchSize == null ? DEFAULT_SWEEP_DELETE_BATCH_HINT : deleteBatchSize)
                .build();

        runSweepWithoutSavingResults(tableRef, sweepProgress, Optional.of(sweepBatchConfig));
    }

    private TableReference getTableRef(String tableName) {
        WebPreconditions.checkArgument(TableReference.isFullyQualifiedName(tableName),
                "Table name {} is not fully qualified", tableName);
        return TableReference.createFromFullyQualifiedName(tableName);
    }

    private void checkTableExists(String tableName, TableReference tableRef) {
        Preconditions.checkState(specificTableSweeper.getKvs().getAllTableNames().contains(tableRef),
                String.format("Table requested to sweep %s does not exist", tableName));
    }

    private ImmutableSweepProgress getSweepProgress(String startRow, TableReference tableRef) {
        return ImmutableSweepProgress.builder()
                .tableRef(tableRef)
                .staleValuesDeleted(0)
                .cellTsPairsExamined(0)
                .minimumSweptTimestamp(0)
                .startRow(decodeStartRow(startRow))
                .build();
    }

    private byte[] decodeStartRow(String startRow) {
        if (startRow == null) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        }
        return BaseEncoding.base16().decode(startRow);
    }

    private void runSweepWithoutSavingResults(TableReference tableRef) {
        runSweepWithoutSavingResults(tableRef, null);
    }

    private void runSweepWithoutSavingResults(TableReference tableRef, SweepProgress sweepProgress) {
        runSweepWithoutSavingResults(tableRef, sweepProgress, Optional.empty());
    }

    private void runSweepWithoutSavingResults(
            TableReference tableRef,
            SweepProgress sweepProgress,
            Optional<SweepBatchConfig> sweepBatchConfig) {
        TableToSweep tableToSweep = getTableToSweep(tableRef, sweepProgress);
        specificTableSweeper.runOnceForTable(tableToSweep, sweepBatchConfig, false);
    }

    private TableToSweep getTableToSweep(TableReference tableRef, SweepProgress sweepProgress) {
        return new TableToSweep(tableRef, sweepProgress);
    }
}

