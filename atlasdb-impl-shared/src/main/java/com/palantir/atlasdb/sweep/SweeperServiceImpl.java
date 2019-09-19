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
package com.palantir.atlasdb.sweep;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.conjure.java.server.jersey.WebPreconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SweeperServiceImpl implements SweeperService {
    private static final Logger log = LoggerFactory.getLogger(SweeperServiceImpl.class);

    private final SpecificTableSweeper specificTableSweeper;
    private final AdjustableSweepBatchConfigSource sweepBatchConfigSource;

    public SweeperServiceImpl(SpecificTableSweeper specificTableSweeper,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource) {
        this.specificTableSweeper = specificTableSweeper;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
    }

    @Override
    public SweepTableResponse sweepTable(
            String tableName,
            Optional<String> startRow,
            Optional<Boolean> fullSweep,
            Optional<Integer> maxCellTsPairsToExamine,
            Optional<Integer> candidateBatchSize,
            Optional<Integer> deleteBatchSize) {
        TableReference tableRef = getTableRef(tableName);
        checkTableExists(tableName, tableRef);

        byte[] decodedStartRow = startRow.map(PtBytes::decodeHexString).orElse(PtBytes.EMPTY_BYTE_ARRAY);
        SweepBatchConfig config = buildConfigWithOverrides(maxCellTsPairsToExamine, candidateBatchSize,
                deleteBatchSize);

        return SweepTableResponse.from(runSweep(fullSweep, tableRef, decodedStartRow, config));
    }

    private SweepResults runSweep(Optional<Boolean> fullSweep, TableReference tableRef, byte[] decodedStartRow,
            SweepBatchConfig config) {
        if (!fullSweep.isPresent()) {
            log.warn("fullSweep parameter was not specified, defaulting to true");
        }

        if (fullSweep.orElse(true)) {
            log.info("Running sweep of full table {}, "
                            + "with maxCellTsPairsToExamine: {}, candidateBatchSize: {}, deleteBatchSize: {}, "
                            + "starting from row {}",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("maxCellTsPairsToExamine", config.maxCellTsPairsToExamine()),
                    SafeArg.of("candidateBatchSize", config.candidateBatchSize()),
                    SafeArg.of("deleteBatchSize", config.deleteBatchSize()),
                    UnsafeArg.of("decodedStartRow", decodedStartRow));
            return runFullSweepWithoutSavingResults(tableRef, decodedStartRow, config);
        } else {
            log.info("Running sweep of a single batch on table {}, "
                            + "with maxCellTsPairsToExamine: {}, candidateBatchSize: {}, deleteBatchSize: {}, "
                            + "starting from row {}",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("maxCellTsPairsToExamine", config.maxCellTsPairsToExamine()),
                    SafeArg.of("candidateBatchSize", config.candidateBatchSize()),
                    SafeArg.of("deleteBatchSize", config.deleteBatchSize()),
                    UnsafeArg.of("decodedStartRow", decodedStartRow));
            return runOneBatchWithoutSavingResults(tableRef, decodedStartRow, config);
        }
    }

    private SweepBatchConfig buildConfigWithOverrides(
            Optional<Integer> maxCellTsPairsToExamine,
            Optional<Integer> candidateBatchSize,
            Optional<Integer> deleteBatchSize) {
        ImmutableSweepBatchConfig.Builder batchConfigBuilder = ImmutableSweepBatchConfig.builder()
                .from(sweepBatchConfigSource.getAdjustedSweepConfig());

        maxCellTsPairsToExamine.ifPresent(batchConfigBuilder::maxCellTsPairsToExamine);
        candidateBatchSize.ifPresent(batchConfigBuilder::candidateBatchSize);
        deleteBatchSize.ifPresent(batchConfigBuilder::deleteBatchSize);

        return batchConfigBuilder.build();
    }

    private TableReference getTableRef(String tableName) {
        WebPreconditions.checkArgument(TableReference.isFullyQualifiedName(tableName),
                "Table name {} is not fully qualified", tableName);
        return TableReference.createFromFullyQualifiedName(tableName);
    }

    private void checkTableExists(String tableName, TableReference tableRef) {
        WebPreconditions.checkArgument(specificTableSweeper.getKvs().getAllTableNames().contains(tableRef),
                "Table requested to sweep %s does not exist", tableName);
    }

    private SweepResults runFullSweepWithoutSavingResults(
            TableReference tableRef,
            byte[] startRow,
            SweepBatchConfig sweepBatchConfig) {
        SweepResults cumulativeResults = SweepResults.createEmptySweepResult(Optional.of(startRow));

        while (cumulativeResults.getNextStartRow().isPresent()) {
            SweepResults results = runOneBatchWithoutSavingResults(
                    tableRef,
                    cumulativeResults.getNextStartRow().get(),
                    sweepBatchConfig);

            specificTableSweeper.updateTimeMetricsOneIteration(results.getTimeInMillis(),
                    results.getTimeElapsedSinceStartedSweeping());
            cumulativeResults = cumulativeResults.accumulateWith(results);
        }

        return cumulativeResults;
    }

    private SweepResults runOneBatchWithoutSavingResults(
            TableReference tableRef,
            byte[] startRow,
            SweepBatchConfig sweepBatchConfig) {
        return specificTableSweeper.runOneIteration(
                    tableRef,
                    startRow,
                    sweepBatchConfig);
    }

}
