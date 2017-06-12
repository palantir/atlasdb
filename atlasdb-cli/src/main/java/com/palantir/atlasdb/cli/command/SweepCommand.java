/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cli.command;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "sweep", description = "Sweep old table rows")
public class SweepCommand extends SingleBackendCommand {

    private static final OutputPrinter printer = new OutputPrinter(LoggerFactory.getLogger(SweepCommand.class));

    @Option(name = {"-n", "--namespace"},
            description = "An atlas namespace to sweep")
    String namespace;

    @Option(name = {"-t", "--table"},
            description = "An atlas table to sweep")
    String table;

    @Option(name = {"-r", "--row"},
            description = "A row to start from (hex encoded bytes)")
    String row;

    @Option(name = {"-a", "--all"},
            description = "Sweep all tables")
    boolean sweepAllTables;

    /**
     * @deprecated Use --candidate-batch-hint instead.
     */
    @Deprecated
    @Option(name = {"--batch-size"},
            description = "Sweeper row batch size. This option has been deprecated "
                    + "in favor of --candidate-batch-hint")
    Integer batchSize;

    /**
     * @deprecated Use --read-limit instead.
     */
    @Deprecated
    @Option(name = {"--cell-batch-size"},
            description = "Sweeper cell batch size. This option has been deprecated "
                    + "in favor of --read-limit")
    Integer cellBatchSize;

    @Option(name = {"--delete-batch-hint"},
            description = "Target number of (cell, timestamp) pairs to delete in a single batch (default: "
                    + AtlasDbConstants.DEFAULT_SWEEP_DELETE_BATCH_HINT + ")")
    int deleteBatchHint = AtlasDbConstants.DEFAULT_SWEEP_DELETE_BATCH_HINT;

    @Option(name = {"--candidate-batch-hint"},
            description = "Approximate number of candidate (cell, timestamp) pairs to load at once (default: "
                    + AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT + ")")
    Integer candidateBatchHint;

    @Option(name = {"--read-limit"},
            description = "Target number of (cell, timestamp) pairs to examine (default: "
                    + AtlasDbConstants.DEFAULT_SWEEP_READ_LIMIT + ")")
    Integer readLimit;

    @Option(name = {"--sleep"},
            description = "Time to wait in milliseconds after each sweep batch"
                    + " (throttles long-running sweep jobs, default: 0)")
    long sleepTimeInMs = 0;

    @Option(name = {"--dry-run"},
            description = "Run sweep in dry run mode to get how much would have been deleted and check safety.")
    boolean dryRun = false;

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    @Override
    public int execute(final AtlasDbServices services) {
        SweepTaskRunner sweepRunner = services.getSweepTaskRunner();

        if (!((namespace != null) ^ (table != null) ^ sweepAllTables)) {
            printer.error("Specify one of --namespace, --table, or --all options.");
            return 1;
        }
        if ((namespace != null) && (row != null)) {
            printer.error("Cannot specify a start row (" + row
                    + ") when sweeping multiple tables (in namespace " + namespace + ")");
            return 1;
        }

        Map<TableReference, byte[]> tableToStartRow = Maps.newHashMap();

        if (table != null) {
            TableReference tableToSweep = TableReference.createUnsafe(table);
            if (!services.getKeyValueService().getAllTableNames().contains(tableToSweep)) {
                printer.info("The table {} passed in to sweep does not exist", tableToSweep);
                return 1;
            }
            byte[] startRow = PtBytes.EMPTY_BYTE_ARRAY;
            if (row != null) {
                startRow = decodeStartRow(row);
            }
            tableToStartRow.put(tableToSweep, startRow);
        } else if (namespace != null) {
            Set<TableReference> tablesInNamespace = services.getKeyValueService().getAllTableNames()
                    .stream()
                    .filter(tableRef -> tableRef.getNamespace().getName().equals(namespace))
                    .collect(Collectors.toSet());
            for (TableReference tableInNamespace : tablesInNamespace) {
                tableToStartRow.put(tableInNamespace, new byte[0]);
            }
        } else if (sweepAllTables) {
            tableToStartRow.putAll(Maps.asMap(
                    Sets.difference(
                            services.getKeyValueService().getAllTableNames(), AtlasDbConstants.hiddenTables),
                    Functions.constant(new byte[0])));
        }

        SweepBatchConfig batchConfig = getSweepBatchConfig();

        for (Map.Entry<TableReference, byte[]> entry : tableToStartRow.entrySet()) {
            final TableReference tableToSweep = entry.getKey();
            Optional<byte[]> startRow = Optional.of(entry.getValue());

            final AtomicLong cellsExamined = new AtomicLong();
            final AtomicLong cellsDeleted = new AtomicLong();

            while (startRow.isPresent()) {
                Stopwatch watch = Stopwatch.createStarted();

                SweepResults results = dryRun
                        ? sweepRunner.dryRun(tableToSweep, batchConfig, startRow.get())
                        : sweepRunner.run(tableToSweep, batchConfig, startRow.get());
                printer.info(
                        "Swept from {} to {} in table {} in {} ms, examined {} cell values,"
                                + " {}deleted {} stale versions of those cells.",
                        encodeStartRow(startRow),
                        encodeEndRow(results.getNextStartRow()),
                        tableToSweep,
                        watch.elapsed(TimeUnit.MILLISECONDS),
                        results.getCellTsPairsExamined(),
                        dryRun ? "would have " : "",
                        results.getStaleValuesDeleted());
                startRow = results.getNextStartRow();
                cellsDeleted.addAndGet(results.getStaleValuesDeleted());
                cellsExamined.addAndGet(results.getCellTsPairsExamined());
                maybeSleep();
            }

            if (!dryRun) {
                services.getTransactionManager().runTaskWithRetry((TxTask) t -> {
                    SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
                    SweepPriorityTable.SweepPriorityRow row1 = SweepPriorityTable.SweepPriorityRow.of(
                            tableToSweep.getQualifiedName());
                    priorityTable.putWriteCount(row1, 0L);
                    priorityTable.putCellsDeleted(row1, cellsDeleted.get());
                    priorityTable.putCellsExamined(row1, cellsExamined.get());
                    priorityTable.putLastSweepTime(row1, System.currentTimeMillis());
                    return null;
                });
            }

            printer.info(
                    "Finished sweeping {}, examined {} cell values, {}deleted {} stale versions of those cells.",
                    tableToSweep,
                    cellsExamined.get(),
                    dryRun ? "would have " : "",
                    cellsDeleted.get());

            if (!dryRun && cellsDeleted.get() > 0) {
                Stopwatch watch = Stopwatch.createStarted();
                services.getKeyValueService().compactInternally(tableToSweep);
                printer.info("Finished performing compactInternally on {} in {} ms.",
                        tableToSweep, watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }
        return 0;
    }

    private SweepBatchConfig getSweepBatchConfig() {
        if (batchSize != null || cellBatchSize != null) {
            printer.warn("Options 'batchSize' and 'cellBatchSize' have been deprecated in favor of 'deleteBatchHint', "
                    + "'candidateBatchHint' and 'readLimit'. Please use the new options in the future.");
        }
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(chooseBestValue(
                        readLimit,
                        cellBatchSize,
                        AtlasDbConstants.DEFAULT_SWEEP_READ_LIMIT))
                .candidateBatchSize(chooseBestValue(
                        candidateBatchHint,
                        batchSize,
                        AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT))
                .deleteBatchSize(deleteBatchHint)
                .build();
    }

    private static int chooseBestValue(@Nullable Integer newOption, @Nullable Integer oldOption, int defaultValue) {
        if (newOption != null) {
            return newOption;
        } else if (oldOption != null) {
            return oldOption;
        } else {
            return defaultValue;
        }
    }

    private void maybeSleep() {
        if (sleepTimeInMs > 0) {
            try {
                Thread.sleep(sleepTimeInMs);
            } catch (InterruptedException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }

    private static final byte[] FIRST_ROW = Arrays.copyOf(new byte[0], 12);
    private static final byte[] LAST_ROW = lastRow();

    private static byte[] lastRow() {
        byte[] last = new byte[12];
        Arrays.fill(last, (byte) 0xFF);
        return last;
    }

    private String encodeStartRow(Optional<byte[]> rowBytes) {
        return BaseEncoding.base16().encode(rowBytes.orElse(FIRST_ROW));
    }

    private String encodeEndRow(Optional<byte[]> rowBytes) {
        if (rowBytes.isPresent() && !Arrays.equals(rowBytes.get(), FIRST_ROW)) {
            return BaseEncoding.base16().encode(rowBytes.get());
        } else {
            return BaseEncoding.base16().encode(LAST_ROW);
        }
    }

    private byte[] decodeStartRow(String rowString) {
        return BaseEncoding.base16().decode(rowString);
    }
}
