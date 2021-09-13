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
package com.palantir.atlasdb.sweep.priority;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.BackgroundSweepThread;
import com.palantir.atlasdb.sweep.TableToSweep;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class NextTableToSweepProvider {
    private static final SafeLogger log = SafeLoggerFactory.get(NextTableToSweepProvider.class);

    private final LockService lockService;
    private final StreamStoreRemappingSweepPriorityCalculator calculator;

    @VisibleForTesting
    NextTableToSweepProvider(
            LockService lockService,
            StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator) {
        this.lockService = lockService;
        this.calculator = streamStoreRemappingSweepPriorityCalculator;
    }

    public static NextTableToSweepProvider create(
            KeyValueService kvs, LockService lockService, SweepPriorityStore sweepPriorityStore) {
        SweepPriorityCalculator basicCalculator = new SweepPriorityCalculator(kvs, sweepPriorityStore);
        StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator =
                new StreamStoreRemappingSweepPriorityCalculator(basicCalculator, sweepPriorityStore);

        return new NextTableToSweepProvider(lockService, streamStoreRemappingSweepPriorityCalculator);
    }

    public Optional<TableToSweep> getNextTableToSweep(Transaction tx, long conservativeSweepTimestamp) {
        return getNextTableToSweep(tx, conservativeSweepTimestamp, SweepPriorityOverrideConfig.defaultConfig());
    }

    public Optional<TableToSweep> getNextTableToSweep(
            Transaction tx, long conservativeSweepTimestamp, SweepPriorityOverrideConfig overrideConfig) {
        if (!overrideConfig.priorityTables().isEmpty()) {
            return attemptToChooseTable(overrideConfig);
        }

        Map<TableReference, Double> scores = calculator.calculateSweepPriorityScores(tx, conservativeSweepTimestamp);

        Map<TableReference, Double> tablesWithNonZeroPriority =
                getTablesToBeConsideredForSweepAndScores(overrideConfig, scores);
        if (tablesWithNonZeroPriority.isEmpty()) {
            return logDecision(Optional.empty(), scores, overrideConfig);
        }

        List<TableReference> tablesOrderedByPriority = orderTablesByPriority(tablesWithNonZeroPriority);

        Optional<TableToSweep> chosenTable =
                attemptToChooseTableFromPrioritisedList(tablesOrderedByPriority, "it has a high priority score");

        return logDecision(chosenTable, scores, overrideConfig);
    }

    private Optional<TableToSweep> attemptToChooseTable(SweepPriorityOverrideConfig overrideConfig) {
        List<TableReference> priorityTableRefs = overrideConfig.priorityTablesAsList().stream()
                .map(TableReference::createFromFullyQualifiedName)
                .collect(Collectors.toList());

        // If there are multiple priority tables, we don't want to consistently use the same ordering.
        // It is true that this operation is O(list length) while an O(min(list length, cluster size)) algorithm
        // exists, but the priority table reference list is expected to be small.
        Collections.shuffle(priorityTableRefs);

        return attemptToChooseTableFromPrioritisedList(priorityTableRefs, "it is on the sweep priority list");
    }

    private Optional<TableToSweep> attemptToChooseTableFromPrioritisedList(
            List<TableReference> priorityTables, String reason) {
        for (TableReference tableRefToSweep : priorityTables) {
            SingleLockService sweepLockForTable = SingleLockService.createNamedLockServiceForTable(
                    lockService, BackgroundSweepThread.TABLE_LOCK_PREFIX, tableRefToSweep);
            try {
                sweepLockForTable.lockOrRefresh();
                if (sweepLockForTable.haveLocks()) {
                    log.info(
                            "Decided to start sweeping {} because {}.",
                            LoggingArgs.tableRef(LoggingArgs.safeTableOrPlaceholder(tableRefToSweep)),
                            SafeArg.of("reason", reason));
                    return Optional.of(TableToSweep.newTable(tableRefToSweep, sweepLockForTable));
                }
            } catch (InterruptedException e) {
                log.info(
                        "Got interrupted while attempting to lock {} for sweeping.",
                        LoggingArgs.tableRef(LoggingArgs.safeTableOrPlaceholder(tableRefToSweep)),
                        e);
            }
            log.info(
                    "Did not start sweeping {}, because it is being swept elsewhere. Another table will be chosen.",
                    LoggingArgs.tableRef(LoggingArgs.safeTableOrPlaceholder(tableRefToSweep)));
        }

        return Optional.empty();
    }

    private Map<TableReference, Double> getTablesToBeConsideredForSweepAndScores(
            SweepPriorityOverrideConfig overrideConfig, Map<TableReference, Double> scores) {
        Set<TableReference> blacklistedTableReferences = overrideConfig.blacklistTables().stream()
                .map(TableReference::createFromFullyQualifiedName)
                .collect(Collectors.toSet());
        return Maps.filterEntries(scores, entry -> shouldTableBeConsideredForSweep(blacklistedTableReferences, entry));
    }

    private static boolean shouldTableBeConsideredForSweep(
            Set<TableReference> blacklistedTables, Map.Entry<TableReference, Double> entry) {
        return !blacklistedTables.contains(entry.getKey()) && entry.getValue() > 0.0;
    }

    private List<TableReference> orderTablesByPriority(Map<TableReference, Double> scores) {
        return scores.entrySet().stream()
                .sorted(Comparator.comparing(entry -> -entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private Optional<TableToSweep> logDecision(
            Optional<TableToSweep> chosenTable,
            Map<TableReference, Double> scores,
            SweepPriorityOverrideConfig sweepPriorityOverrideConfig) {
        if (!log.isDebugEnabled()) {
            return chosenTable;
        }

        String safeTableNamesToScore = scores.entrySet().stream()
                .sorted(Comparator.comparingDouble(Map.Entry::getValue))
                .map(entry -> LoggingArgs.safeTableOrPlaceholder(entry.getKey()) + "->" + entry.getValue())
                .collect(Collectors.joining(", ", "[", "]"));

        String chosenTableString = chosenTable.isPresent()
                ? LoggingArgs.safeTableOrPlaceholder(chosenTable.get().getTableRef())
                        .toString()
                : "no table";

        log.debug(
                "Chose {} from scores: {}, unsafeScores: {}, overrides: {}",
                SafeArg.of("chosenTable", chosenTableString),
                SafeArg.of("scores", safeTableNamesToScore),
                UnsafeArg.of("unsafeScores", scores),
                UnsafeArg.of("overrides", sweepPriorityOverrideConfig));

        return chosenTable;
    }
}
