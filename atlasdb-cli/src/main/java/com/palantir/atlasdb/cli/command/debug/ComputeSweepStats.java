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

package com.palantir.atlasdb.cli.command.debug;

import java.util.Map;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.keyvalue.api.WriteReferencePersister;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;

import io.airlift.airline.Command;

@Command(name = "compute-sweep-stats",
        description = "Output some statistics about the live sweep queue.")
public class ComputeSweepStats extends SingleBackendCommand {
    private static final OutputPrinter printer = new OutputPrinter(LoggerFactory.getLogger(ComputeSweepStats.class));

    private Map<TableReference, Long> countByTable = Maps.newHashMap();
    private Map<Boolean, Long> countByType = Maps.newHashMapWithExpectedSize(2);
    private Map<TableReference, Boolean> tableToType = Maps.newHashMap();

    @Override
    public int execute(AtlasDbServices services) {
        KeyValueService kvs = services.getKeyValueService();
        WriteReferencePersister writeReferencePersister = new WriteReferencePersister(new SweepTableIndices(kvs));

        services.getTransactionManager().runTaskReadOnly(t -> {
            TargetedSweepTableFactory.of().getSweepableCellsTable(t).getAllRowsUnordered().hintBatchSize(10_000)
                    .forEach(rowResult -> {
                        TargetedSweepMetadata metadata =
                                TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(
                                        rowResult.getRowName().getMetadata());

                        rowResult.getColumnValues().stream().forEach(columnValue -> {
                            WriteReference writeRef = writeReferencePersister.unpersist(columnValue.getValue());
                            TableReference tableRef = writeRef.tableRef();
                            updateStats(tableRef, metadata.conservative());
                        });
                    });
            return 0;
        });
        printStats();
        return 0;
    }

    private void updateStats(TableReference tableRef, boolean isConservative) {
        countByTable.merge(tableRef, 1L, (table, count) -> count.longValue() + 1);
        countByType.merge(isConservative, 1L, (conservative, count) -> count.longValue() + 1);
        tableToType.putIfAbsent(tableRef, isConservative);
    }

    private void printStats() {
        printer.info("Overall count per table:");
        countByTable.forEach((table, count) -> printer.info(table + ", " + count));

        printer.info("\nOverall count per conservative type:");
        countByType.forEach((conservative, count) -> printer.info(isConservative(conservative) + ", " + count));

        printer.info("\nTables by conservative write status:");
        tableToType.forEach((table, conservative) -> printer.info(table + ", " + isConservative(conservative)));
    }

    private String isConservative(boolean isConservative) {
        return isConservative ? "conservative" : "normal";
    }

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }
}
