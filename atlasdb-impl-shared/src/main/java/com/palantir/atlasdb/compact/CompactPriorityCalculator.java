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
import java.util.stream.Collectors;

import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

final class CompactPriorityCalculator {
    private final TransactionManager transactionManager;

    CompactPriorityCalculator(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Optional<String> selectTableToCompact() {
        return transactionManager.runTaskReadOnly(this::selectTableToCompactInternal);
    }

    private Optional<String> selectTableToCompactInternal(Transaction tx) {
        Map<java.lang.String, Long> tableToLastTimeSwept = new HashMap<>();
        SweepPriorityTable sweepPriorityTable = SweepTableFactory.of().getSweepPriorityTable(tx);
        sweepPriorityTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastSweepTime = row.getLastSweepTime();
                    java.lang.String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeSwept.put(tableName, lastSweepTime);
                });

        Map<java.lang.String, Long> tableToLastTimeCompacted = new HashMap<>();
        CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
        compactMetadataTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastCompactTime = row.getLastCompactTime();
                    java.lang.String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeCompacted.put(tableName, lastCompactTime);
                });

        List<java.lang.String> uncompactedTables = tableToLastTimeSwept.keySet().stream()
                .filter(table -> !tableToLastTimeCompacted.keySet().contains(table))
                .collect(Collectors.toList());

        if (uncompactedTables.size() > 0) {
            int randomTableIndex = ThreadLocalRandom.current().nextInt(uncompactedTables.size());
            return Optional.of(uncompactedTables.get(randomTableIndex));
        }

        java.lang.String tableToCompact = null;
        long maxSweptAfterCompact = Long.MIN_VALUE;
        for (Map.Entry<java.lang.String, Long> entry : tableToLastTimeSwept.entrySet()) {
            java.lang.String table = entry.getKey();
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
