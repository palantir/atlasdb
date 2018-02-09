/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.compact;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.LockService;

public class BackgroundCompactor implements Runnable {
    private final TransactionManager transactionManager;
    private final KeyValueService keyValueService;
    private final Supplier<CompactorConfig> config;

    public static void createAndRun(TransactionManager transactionManager,
            KeyValueService keyValueService,
            Supplier<CompactorConfig> compactorConfigSupplier) {
        if (!(keyValueService instanceof DbKvs)) {
            return;
        }

        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(transactionManager,
                keyValueService,
                compactorConfigSupplier);
    }

    public BackgroundCompactor(TransactionManager transactionManager,
            KeyValueService keyValueService,
            Supplier<CompactorConfig> config) {
        this.transactionManager = transactionManager;
        this.keyValueService = keyValueService;
        this.config = config;
    }

    @Override
    public void run() {
        while (true) {
            Optional<String> tableToCompactOptional = transactionManager.runTaskReadOnly(this::selectTableToCompact);
            if (!tableToCompactOptional.isPresent()) {
                continue;
            }

            String tableToCompact = tableToCompactOptional.get();
            compactTable(tableToCompact);
            registerCompactedTable(tableToCompact);
        }
    }

    private void registerCompactedTable(String tableToCompact) {
        transactionManager.runTaskWithRetry(tx -> {
            CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
            compactMetadataTable.putLastCompactTime(
                    CompactMetadataTable.CompactMetadataRow.of(tableToCompact),
                    System.currentTimeMillis());

            return null;
        });
    }

    private void compactTable(String tableToCompact) {
        keyValueService.compactInternally(TableReference.createFromFullyQualifiedName(tableToCompact));
    }

    private Optional<String> selectTableToCompact(Transaction tx) {
        Map<String, Long> tableToLastTimeSwept = new HashMap<>();
        SweepPriorityTable sweepPriorityTable = SweepTableFactory.of().getSweepPriorityTable(tx);
        sweepPriorityTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastSweepTime = row.getLastSweepTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeSwept.put(tableName, lastSweepTime);
                });

        Map<String, Long> tableToLastTimeCompacted = new HashMap<>();
        CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
        compactMetadataTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastCompactTime = row.getLastCompactTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeCompacted.put(tableName, lastCompactTime);
                });

        List<String> uncompactedTables = tableToLastTimeSwept.keySet().stream()
                .filter(table -> !tableToLastTimeCompacted.keySet().contains(table))
                .collect(Collectors.toList());

        if (uncompactedTables.size() > 0) {
            int randomTableIndex = ThreadLocalRandom.current().nextInt(uncompactedTables.size());
            return Optional.of(uncompactedTables.get(randomTableIndex));
        }

        String tableToCompact = null;
        long maxSweptAfterCompact = Long.MIN_VALUE;
        for (Map.Entry<String, Long> entry : tableToLastTimeSwept.entrySet()){
            String table = entry.getKey();
            long lastSweptTime = entry.getValue();
            long lastCompactTime = tableToLastTimeCompacted.get(table);
            long sweptAfterCompact = lastSweptTime - lastCompactTime;

            if (sweptAfterCompact > maxSweptAfterCompact) {
                tableToCompact = table;
                maxSweptAfterCompact = sweptAfterCompact;
            }
        }

        return Optional.ofNullable(tableToCompact);
    }
}
