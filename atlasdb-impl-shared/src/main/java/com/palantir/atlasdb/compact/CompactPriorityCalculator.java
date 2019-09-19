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
package com.palantir.atlasdb.compact;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CompactPriorityCalculator {
    private static final Logger log = LoggerFactory.getLogger(CompactPriorityCalculator.class);

    private final TransactionManager transactionManager;
    private final CompactionHistoryProvider compactionHistoryProvider;
    private final SweepHistoryProvider sweepHistoryProvider;

    static CompactPriorityCalculator create(TransactionManager transactionManager) {
        return new CompactPriorityCalculator(transactionManager,
                new SweepHistoryProvider(),
                new CompactionHistoryProvider());
    }

    @VisibleForTesting
    CompactPriorityCalculator(TransactionManager transactionManager,
            SweepHistoryProvider sweepHistoryProvider,
            CompactionHistoryProvider compactionHistoryProvider) {
        this.transactionManager = transactionManager;
        this.sweepHistoryProvider = sweepHistoryProvider;
        this.compactionHistoryProvider = compactionHistoryProvider;
    }

    Optional<String> selectTableToCompact() {
        return transactionManager.runTaskReadOnly(this::selectTableToCompactInternal);
    }

    @VisibleForTesting
    Optional<String> selectTableToCompactInternal(Transaction tx) {
        Map<String, Long> tableToLastTimeSwept = sweepHistoryProvider.getHistory(tx);
        Map<String, Long> tableToLastTimeCompacted = compactionHistoryProvider.getHistory(tx);

        Optional<String> tableToCompact = maybeChooseUncompactedTable(tableToLastTimeSwept, tableToLastTimeCompacted);
        if (!tableToCompact.isPresent()) {
            tableToCompact = maybeChooseTableSweptAfterCompact(tableToLastTimeSwept, tableToLastTimeCompacted);
        }
        if (!tableToCompact.isPresent()) {
            tableToCompact = maybeChooseTableCompactedOver1HourAgo(tableToLastTimeCompacted);
        }
        if (!tableToCompact.isPresent()) {
            log.info("Not compacting, because it does not appear that any table has been swept"
                    + " or they were compacted too recently (the past hour).");
        }
        return tableToCompact;
    }

    private static Optional<String> maybeChooseUncompactedTable(
            Map<String, Long> tableToLastTimeSwept,
            Map<String, Long> tableToLastTimeCompacted) {

        List<String> uncompactedTables = tableToLastTimeSwept.keySet().stream()
                .filter(table -> !tableToLastTimeCompacted.keySet().contains(table))
                .collect(Collectors.toList());

        if (uncompactedTables.size() > 0) {
            int randomTableIndex = ThreadLocalRandom.current().nextInt(uncompactedTables.size());
            String randomlyChosenTable = uncompactedTables.get(randomTableIndex);
            log.info("There are some tables which have been swept, but not compacted. Choosing {} at random.",
                    safeTableRef(randomlyChosenTable));
            return Optional.of(randomlyChosenTable);
        }
        return Optional.empty();
    }

    private static Optional<String> maybeChooseTableSweptAfterCompact(
            Map<String, Long> tableToLastTimeSwept,
            Map<String, Long> tableToLastTimeCompacted) {

        String tableToCompact = null;
        long maxSweptAfterCompact = 0L;
        for (Map.Entry<String, Long> entry : tableToLastTimeSwept.entrySet()) {
            String table = entry.getKey();
            long lastSweptTime = entry.getValue();
            long lastCompactTime = tableToLastTimeCompacted.get(table);
            long sweptAfterCompact = lastSweptTime - lastCompactTime;

            if (sweptAfterCompact > maxSweptAfterCompact) {
                tableToCompact = table;
                maxSweptAfterCompact = sweptAfterCompact;
            }
        }

        if (tableToCompact != null) {
            log.info("Choosing to compact {}, because it was swept {} milliseconds after the last compaction",
                    safeTableRef(tableToCompact),
                    SafeArg.of("millisFromCompactionToSweep", maxSweptAfterCompact));
            return Optional.of(tableToCompact);
        }
        return Optional.empty();
    }

    private static Optional<String> maybeChooseTableCompactedOver1HourAgo(
            Map<String, Long> tableToLastTimeCompacted) {

        List<String> filteredTablesCompactedAfterSweep = tableToLastTimeCompacted.entrySet().stream()
                .filter(entry -> entry.getValue() < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1L))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (filteredTablesCompactedAfterSweep.size() > 0) {
            int randomTableIndex = ThreadLocalRandom.current().nextInt(filteredTablesCompactedAfterSweep.size());
            String randomlyChosenTable = filteredTablesCompactedAfterSweep.get(randomTableIndex);
            log.info("All swept tables have been compacted after the last sweep. Choosing to compact {} at random"
                            + " between tables which were compacted more than 1 hour ago.",
                    safeTableRef(randomlyChosenTable));
            return Optional.of(randomlyChosenTable);
        }
        return Optional.empty();
    }

    private static Arg<String> safeTableRef(String fullyQualifiedName) {
        return LoggingArgs.safeInternalTableName(fullyQualifiedName);
    }

}
