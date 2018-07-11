/*
 * Copyright 2015 Palantir Technologies
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.services.AtlasDbServices;
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

    @Option(name = {"--batch-size"},
            description = "Sweeper row batch size (default: " + AtlasDbConstants.DEFAULT_SWEEP_BATCH_SIZE + ")")
    int batchSize = AtlasDbConstants.DEFAULT_SWEEP_BATCH_SIZE;

    @Option(name = {"--cell-batch-size"},
            description = "Sweeper cell batch size (default: " + AtlasDbConstants.DEFAULT_SWEEP_CELL_BATCH_SIZE+ ")")
    int cellBatchSize = AtlasDbConstants.DEFAULT_SWEEP_CELL_BATCH_SIZE;

    @Option(name = {"--sleep"},
            description = "Time to wait in milliseconds after each sweep batch (throttles long-running sweep jobs, default: 0)")
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
            printer.error("Cannot specify a start row (" + row + ") when sweeping multiple tables (in namespace " + namespace + ")");
            return 1;
        }

        Map<TableReference, byte[]> tableToStartRow = Maps.newHashMap();

        if (table != null) {
            TableReference tableToSweep = TableReference.createUnsafe(table);
            if (!services.getKeyValueService().getAllTableNames().contains(tableToSweep)) {
                printer.info("The table {} passed in to sweep does not exist", tableToSweep);
                return 1;
            }
            byte[] startRow = new byte[0];
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
            tableToStartRow.putAll(
                    Maps.asMap(
                            Sets.difference(services.getKeyValueService().getAllTableNames(), AtlasDbConstants.hiddenTables),
                            Functions.constant(new byte[0])));
        }

        for (Map.Entry<TableReference, byte[]> entry : tableToStartRow.entrySet()) {
            final TableReference tableToSweep = entry.getKey();
            Optional<byte[]> startRow = Optional.of(entry.getValue());

            final AtomicLong cellsExamined = new AtomicLong();
            final AtomicLong cellsDeleted = new AtomicLong();

            while (startRow.isPresent()) {
                Stopwatch watch = Stopwatch.createStarted();

                SweepResults results = dryRun
                        ? sweepRunner.dryRun(tableToSweep, batchSize, cellBatchSize, startRow.get())
                        : sweepRunner.run(tableToSweep, batchSize, cellBatchSize, startRow.get());
                printer.info(
                        "Swept from {} to {} in table {} in {} ms, examined {} unique cells, {}deleted {} stale versions of those cells.",
                        encodeStartRow(startRow),
                        encodeEndRow(results.getNextStartRow()),
                        tableToSweep,
                        watch.elapsed(TimeUnit.MILLISECONDS),
                        results.getCellsExamined(),
                        dryRun ? "would have " : "",
                        results.getCellsDeleted());
                startRow = results.getNextStartRow();
                cellsDeleted.addAndGet(results.getCellsDeleted());
                cellsExamined.addAndGet(results.getCellsExamined());
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
                    "Finished sweeping {}, examined {} unique cells, {}deleted {} stale versions of those cells.",
                    tableToSweep,
                    cellsExamined.get(),
                    dryRun ? "would have " : "",
                    cellsDeleted.get());
        }
        return 0;
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
        return BaseEncoding.base16().encode(rowBytes.or(FIRST_ROW));
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
