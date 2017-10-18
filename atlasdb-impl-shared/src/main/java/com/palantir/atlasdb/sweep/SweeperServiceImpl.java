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

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.remoting3.servers.jersey.WebPreconditions;

public final class SweeperServiceImpl implements SweeperService {
    private SpecificTableSweeper specificTableSweeper;

    public SweeperServiceImpl(SpecificTableSweeper specificTableSweeper) {
        this.specificTableSweeper = specificTableSweeper;
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

        SweepResults sweepResults = fullSweep.orElse(true)
                ? runFullSweepWithoutSavingResults(
                        tableRef,
                        decodedStartRow,
                        config)
                : runOneBatchWithoutSavingResults(
                        tableRef,
                        decodedStartRow,
                        config);

        return SweepTableResponse.from(sweepResults);
    }

    private SweepBatchConfig buildConfigWithOverrides(
            Optional<Integer> maxCellTsPairsToExamine,
            Optional<Integer> candidateBatchSize,
            Optional<Integer> deleteBatchSize) {
        ImmutableSweepBatchConfig.Builder batchConfigBuilder = ImmutableSweepBatchConfig.builder()
                .from(specificTableSweeper.getAdjustedBatchConfig());

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
        Preconditions.checkState(specificTableSweeper.getKvs().getAllTableNames().contains(tableRef),
                String.format("Table requested to sweep %s does not exist", tableName));
    }

    private SweepResults runFullSweepWithoutSavingResults(
            TableReference tableRef,
            byte[] startRow,
            SweepBatchConfig sweepBatchConfig) {
        SweepResults cumulativeResults = SweepResults.createEmptySweepResult(
                Optional.of(startRow));

        while (cumulativeResults.getNextStartRow().isPresent()) {
            SweepResults results = runOneBatchWithoutSavingResults(
                    tableRef,
                    cumulativeResults.getNextStartRow().get(),
                    sweepBatchConfig);

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

