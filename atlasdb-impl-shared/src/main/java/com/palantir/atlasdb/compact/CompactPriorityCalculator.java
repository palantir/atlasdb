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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;

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

        String tableToCompact = null;
        long maxSweptAfterCompact = Long.MIN_VALUE;
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

        logCompactionChoice(tableToCompact, maxSweptAfterCompact);
        return Optional.ofNullable(tableToCompact);
    }

    private void logCompactionChoice(String tableToCompact, long maxSweptAfterCompact) {
        if (maxSweptAfterCompact > 0) {
            log.info("Choosing to compact {}, because it was swept {} milliseconds after the last compaction",
                    safeTableRef(tableToCompact),
                    SafeArg.of("millisFromCompactionToSweep", maxSweptAfterCompact));
        } else {
            log.info("All swept tables have been compacted after the last sweep. Choosing to compact {} anyway - "
                    + "this may be a no-op. It was last compacted {} milliseconds after it was last swept.",
                    safeTableRef(tableToCompact),
                    SafeArg.of("millisFromSweepToCompaction", maxSweptAfterCompact));
        }
    }

    private Arg<String> safeTableRef(String fullyQualifiedName) {
        return LoggingArgs.safeInternalTableName(fullyQualifiedName);
    }

}
