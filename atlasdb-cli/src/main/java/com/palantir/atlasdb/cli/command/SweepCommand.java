/**
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

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "sweep", description = "Sweep old table rows")
public class SweepCommand extends SingleBackendCommand {

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

    @Option(name = {"--sleep"},
            description = "Time to wait in milliseconds after each sweep batch (throttles long-running sweep jobs, default: 0)")
    long sleepTimeInMs = 0;

	@Override
	public int execute(final AtlasDbServices services) {
        SweepTaskRunner sweepRunner = services.getSweepTaskRunner();

        if (!((namespace != null) ^ (table != null) ^ sweepAllTables)) {
            System.err.println("Specify one of --namespace, --table, or --all options.");
            return 1;
        }
        if ((namespace != null) && (row != null)) {
            System.err.println("Cannot specify a start row (" + row + ") when sweeping multiple tables (in namespace " + namespace + ")");
            return 1;
        }

        Map<TableReference, Optional<byte[]>> tableToStartRow = Maps.newHashMap();

        if ((table != null)) {
            Optional<byte[]> startRow = Optional.of(new byte[0]);
            if (row != null) {
                startRow = Optional.of(decodeStartRow(row));
            }
            tableToStartRow.put(TableReference.createUnsafe(table), startRow);
        } else if (namespace != null) {
            Set<TableReference> tablesInNamespace = services.getKeyValueService().getAllTableNames()
                    .stream()
                    .filter(tableRef -> tableRef.getNamespace().getName().equals(namespace))
                    .collect(Collectors.toSet());
            for (TableReference table : tablesInNamespace) {
                tableToStartRow.put(table, Optional.of(new byte[0]));
            }
        } else if (sweepAllTables) {
            tableToStartRow.putAll(
                    Maps.asMap(
                            Sets.difference(services.getKeyValueService().getAllTableNames(), AtlasDbConstants.hiddenTables),
                            Functions.constant(Optional.of(new byte[0]))));
        }

        for (Map.Entry<TableReference, Optional<byte[]>> entry : tableToStartRow.entrySet()) {
            final TableReference table = entry.getKey();
            Optional<byte[]> startRow = entry.getValue();

            final AtomicLong cellsExamined = new AtomicLong();
            final AtomicLong cellsDeleted = new AtomicLong();

            while (startRow.isPresent()) {
                Stopwatch watch = Stopwatch.createStarted();
                SweepResults results = sweepRunner.run(table, batchSize, startRow.get());
                System.out.println(String.format("Swept from %s to %s in table %s in %d ms, examined %d unique cells, deleted %d cells.",
                        encodeStartRow(startRow), encodeEndRow(results.getNextStartRow()),
                        table, watch.elapsed(TimeUnit.MILLISECONDS),
                        results.getCellsExamined(), results.getCellsDeleted()));
                startRow = results.getNextStartRow();
                cellsDeleted.addAndGet(results.getCellsDeleted());
                cellsExamined.addAndGet(results.getCellsExamined());
                maybeSleep();
            }

            services.getTransactionManager().runTaskWithRetry((TxTask) t -> {
                SweepPriorityTable priorityTable = SweepTableFactory.of().getSweepPriorityTable(t);
                SweepPriorityTable.SweepPriorityRow row1 = SweepPriorityTable.SweepPriorityRow.of(table.getQualifiedName());
                priorityTable.putWriteCount(row1, 0L);
                priorityTable.putCellsDeleted(row1, cellsDeleted.get());
                priorityTable.putCellsExamined(row1, cellsExamined.get());
                priorityTable.putLastSweepTime(row1, System.currentTimeMillis());

                System.out.println(String.format("Finished sweeping %s, examined %d unique cells, deleted %d cells.",
                        table, cellsExamined.get(), cellsDeleted.get()));

                if (cellsDeleted.get() > 0) {
                    Stopwatch watch = Stopwatch.createStarted();
                    services.getKeyValueService().compactInternally(table);
                    System.out.println(String.format("Finished performing compactInternally on %s in %d ms.",
                            table, watch.elapsed(TimeUnit.MILLISECONDS)));
                }
                return null;
            });
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
        if (rowBytes.isPresent() && !rowBytes.equals(FIRST_ROW)) {
            return BaseEncoding.base16().encode(rowBytes.get());
        } else {
            return BaseEncoding.base16().encode(LAST_ROW);
        }
    }

    private byte[] decodeStartRow(String rowString) {
        return BaseEncoding.base16().decode(rowString);
    }
}
