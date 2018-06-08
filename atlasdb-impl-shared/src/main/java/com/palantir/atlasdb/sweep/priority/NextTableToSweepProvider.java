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
package com.palantir.atlasdb.sweep.priority;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.TableToSweep;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class NextTableToSweepProvider {
    private static final Logger log = LoggerFactory.getLogger(NextTableToSweepProvider.class);

    private final LockService lockService;
    private final StreamStoreRemappingSweepPriorityCalculator calculator;

    @VisibleForTesting
    NextTableToSweepProvider(LockService lockService,
            StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator) {
        this.lockService = lockService;
        this.calculator = streamStoreRemappingSweepPriorityCalculator;
    }

    public static NextTableToSweepProvider create(KeyValueService kvs, LockService lockService,
            SweepPriorityStore sweepPriorityStore) {
        SweepPriorityCalculator basicCalculator = new SweepPriorityCalculator(kvs, sweepPriorityStore);
        StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator =
                new StreamStoreRemappingSweepPriorityCalculator(basicCalculator, sweepPriorityStore);

        return new NextTableToSweepProvider(lockService, streamStoreRemappingSweepPriorityCalculator);
    }

    public Optional<TableToSweep> getNextTableToSweep(Transaction tx,
            long conservativeSweepTimestamp) {
        return getNextTableToSweep(tx, conservativeSweepTimestamp, SweepPriorityOverrideConfig.defaultConfig());
    }

    public Optional<TableToSweep> getNextTableToSweep(
            Transaction tx,
            long conservativeSweepTimestamp,
            SweepPriorityOverrideConfig overrideConfig) {
        if (!overrideConfig.priorityTables().isEmpty()) {
            TableReference tableRefToSweep =
                    TableReference.createFromFullyQualifiedName(
                            getRandomValueFromList(overrideConfig.priorityTablesAsList()));
            log.info("Decided to start sweeping {} because it is on the sweep priority list.",
                    LoggingArgs.safeTableOrPlaceholder(tableRefToSweep));
            return Optional.of(TableToSweep.newTable(tableRefToSweep));
        }

        Map<TableReference, Double> scores = calculator.calculateSweepPriorityScores(tx, conservativeSweepTimestamp);

        Map<TableReference, Double> tablesWithNonZeroPriority
                = getTablesToBeConsideredForSweepAndScores(overrideConfig, scores);
        if (tablesWithNonZeroPriority.isEmpty()) {
            return logDecision(Optional.empty(), scores, overrideConfig);
        }

        List<TableReference> tablesWithHighestPriority = findTablesWithHighestPriority(tablesWithNonZeroPriority);

        Optional<TableToSweep> chosenTable = tablesWithHighestPriority.size() > 0
                ? Optional.of(
                TableToSweep.newTable(getRandomValueFromList(tablesWithHighestPriority)))
                : Optional.empty();

        return logDecision(chosenTable, scores, overrideConfig);
    }

    private Map<TableReference, Double> getTablesToBeConsideredForSweepAndScores(
            SweepPriorityOverrideConfig overrideConfig, Map<TableReference, Double> scores) {
        Set<TableReference> blacklistedTableReferences = overrideConfig.blacklistTables().stream()
                .map(TableReference::createFromFullyQualifiedName)
                .collect(Collectors.toSet());
        return Maps.filterEntries(scores, entry -> shouldTableBeConsideredForSweep(
                blacklistedTableReferences, entry));
    }

    private static boolean shouldTableBeConsideredForSweep(Set<TableReference> blacklistedTables,
            Map.Entry<TableReference, Double> entry) {
        return !blacklistedTables.contains(entry.getKey()) && entry.getValue() > 0.0;
    }

    private static List<TableReference> findTablesWithHighestPriority(
            Map<TableReference, Double> scores) {
        Double maxPriority = Collections.max(scores.values());

        return scores.entrySet().stream()
                .filter(entry -> Objects.equals(entry.getValue(), maxPriority))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private static <T> T getRandomValueFromList(List<T> values) {
        return values.get(ThreadLocalRandom.current().nextInt(values.size()));
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
                ? LoggingArgs.safeTableOrPlaceholder(chosenTable.get().getTableRef()).toString()
                : "no table";

        log.debug("Chose {} from scores: {}, unsafeScores: {}, overrides: {}",
                SafeArg.of("chosenTable", chosenTableString),
                SafeArg.of("scores", safeTableNamesToScore),
                UnsafeArg.of("unsafeScores", scores),
                UnsafeArg.of("overrides", sweepPriorityOverrideConfig));

        return chosenTable;
    }
}
