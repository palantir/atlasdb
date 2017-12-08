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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.logsafe.SafeArg;

public class NextTableToSweepProviderImpl implements NextTableToSweepProvider {

    private static final Logger log = LoggerFactory.getLogger(NextTableToSweepProviderImpl.class);

    public static final int WAIT_BEFORE_SWEEPING_IF_WE_GENERATE_THIS_MANY_TOMBSTONES = 1_000_000;
    public static final int DAYS_TO_WAIT_BEFORE_SWEEPING_STREAM_STORE_VALUE_TABLE = 3;
    public static final int STREAM_STORE_VALUES_TO_SWEEP = 1_000;

    // weights one month of no sweeping with the same priority as about 100000 expected cells to sweep.
    private static final double MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT =
            100_000.0 / TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);

    private final KeyValueService kvs;
    private final SweepPriorityStore sweepPriorityStore;

    public NextTableToSweepProviderImpl(KeyValueService kvs, SweepPriorityStore sweepPriorityStore) {
        this.kvs = kvs;
        this.sweepPriorityStore = sweepPriorityStore;
    }

    @Override
    public Optional<TableReference> chooseNextTableToSweep(Transaction tx, long conservativeSweepTs) {
        Set<TableReference> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);

        // We read priorities from the past because we should prioritize based on what the sweeper will
        // actually be able to sweep. We read priorities from the present to make sure we don't repeatedly
        // sweep the same table while waiting for the past to catch up.
        List<SweepPriority> oldPriorities = sweepPriorityStore.loadOldPriorities(tx, conservativeSweepTs);
        List<SweepPriority> newPriorities = sweepPriorityStore.loadNewPriorities(tx);
        Map<TableReference, SweepPriority> newPrioritiesByTableName = newPriorities.stream().collect(
                Collectors.toMap(SweepPriority::tableRef, Function.identity()));
        return getTableToSweep(tx, allTables, oldPriorities, newPrioritiesByTableName);
    }

    private Optional<TableReference> getTableToSweep(
            Transaction tx,
            Set<TableReference> allTables,
            List<SweepPriority> oldPriorities,
            Map<TableReference, SweepPriority> newPrioritiesByTableName) {
        // Arbitrarily pick the first table alphabetically from the never-before-swept tables
        List<TableReference> unsweptTables = Sets.difference(allTables, newPrioritiesByTableName.keySet())
                .stream().sorted(Comparator.comparing(TableReference::getTablename)).collect(Collectors.toList());
        if (!unsweptTables.isEmpty()) {
            TableReference nextTableToSweep = unsweptTables.get(0);
            LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafeTableReferences =
                    LoggingArgs.tableRefs(unsweptTables);
            log.debug("Found {} and {} unswept tables, choosing to sweep table {}",
                    safeAndUnsafeTableReferences.safeTableRefs(),
                    safeAndUnsafeTableReferences.unsafeTableRefs(),
                    LoggingArgs.tableRef(nextTableToSweep));
            return Optional.of(nextTableToSweep);
        } else {
            Collection<TableReference> toDelete = Lists.newArrayList();
            List<TableWithPriority> tablesWithPriority = new ArrayList<>(oldPriorities.size());

            for (SweepPriority oldPriority : oldPriorities) {
                if (allTables.contains(oldPriority.tableRef())) {
                    SweepPriority newPriority = newPrioritiesByTableName.get(oldPriority.tableRef());
                    double priority = getSweepPriority(oldPriority, newPriority);
                    tablesWithPriority.add(TableWithPriority.create(oldPriority.tableRef(), priority));
                } else {
                    toDelete.add(oldPriority.tableRef());
                }
            }

            // Clean up rows for tables that no longer exist.
            sweepPriorityStore.delete(tx, toDelete);

            tablesWithPriority.sort((o1, o2) -> {
                if (o1.priority() > o2.priority()) {
                    return 1;
                }
                if (o1.priority() < o2.priority()) {
                    return -1;
                }
                return 0;
            });

            logPrioritiesByTable(tablesWithPriority);

            // TODO(ssouza): move this to the BackgroundSweeperImpl when we make this method return a list.
            Optional<TableReference> toSweep = Optional.empty();
            List<TableReference> tablesWithMaxPriority = Lists.newArrayList();
            double maxValue = 0;
            for (TableWithPriority tableWithPriority : tablesWithPriority) {
                if (tableWithPriority.priority() == Double.MAX_VALUE) {
                    tablesWithMaxPriority.add(tableWithPriority.tableRefence());
                }
                if (tableWithPriority.priority() > maxValue) {
                    maxValue = tableWithPriority.priority();
                    toSweep = Optional.of(tableWithPriority.tableRefence());
                }
            }

            return tablesWithMaxPriority.size() > 0 ?
                    Optional.of(getRandomValueFromList(tablesWithMaxPriority)) :
                    toSweep;
        }
    }

    private void logPrioritiesByTable(List<TableWithPriority> tableWithPriorities) {
        List<TableWithPriority> safeTables = new ArrayList<>();
        List<TableWithPriority> unsafeTables = new ArrayList<>();

        for (TableWithPriority tableWithPriority : tableWithPriorities) {
            if (LoggingArgs.tableRef(tableWithPriority.tableRefence()).isSafeForLogging()) {
                safeTables.add(tableWithPriority);
            } else {
                unsafeTables.add(tableWithPriority);
            }
        }

        log.debug("Sweep priorities per table: {} and {}",
                SafeArg.of("tableWithPriorities", safeTables.toString()),
                SafeArg.of("unsafeTableWithPriorities", unsafeTables.toString()));
    }

    private TableReference getRandomValueFromList(List<TableReference> tablesWithMaxPriority) {
        return tablesWithMaxPriority.get(ThreadLocalRandom.current().nextInt(tablesWithMaxPriority.size()));
    }

    private double getSweepPriority(SweepPriority oldPriority, SweepPriority newPriority) {
        if (AtlasDbConstants.hiddenTables.contains(newPriority.tableRef())) {
            // Never sweep hidden tables.
            return 0.0;
        }
        if (!newPriority.lastSweepTimeMillis().isPresent()) {
            // Highest priority if we've never swept it before.
            return Double.MAX_VALUE;
        }
        if (oldPriority.writeCount() > newPriority.writeCount()) {
            // We just swept this, or it got truncated.
            return 0.0;
        }

        if (StreamTableType.isStreamStoreValueTable(newPriority.tableRef())) {
            return getStreamStorePriority(newPriority);
        } else {
            return getNonStreamStorePriority(oldPriority, newPriority);
        }
    }

    private double getStreamStorePriority(SweepPriority newPriority) {
        long millisSinceSweep = System.currentTimeMillis() - newPriority.lastSweepTimeMillis().getAsLong();

        if (TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS)
                < DAYS_TO_WAIT_BEFORE_SWEEPING_STREAM_STORE_VALUE_TABLE) {
            return 0L;
        }
        if (newPriority.writeCount() > STREAM_STORE_VALUES_TO_SWEEP) {
            return Double.MAX_VALUE;
        }

        // Since almost every stream store value is 1MB long, give each cell a weight of 100.
        // It should grow up to just 100 * STREAM_STORE_VALUES_TO_SWEEP = 100k anyway due to the above check.
        return 100 * newPriority.writeCount();
    }

    private double getNonStreamStorePriority(SweepPriority oldPriority, SweepPriority newPriority) {
        long staleValuesDeleted = Math.max(1, oldPriority.staleValuesDeleted());
        long cellTsPairsExamined = Math.max(1, oldPriority.cellTsPairsExamined());
        long writeCount = Math.max(1, oldPriority.writeCount());
        double previousEfficacy = 1.0 * staleValuesDeleted / cellTsPairsExamined;
        double estimatedCellTsPairsToSweep = previousEfficacy * writeCount;
        long millisSinceSweep = System.currentTimeMillis() - newPriority.lastSweepTimeMillis().getAsLong();

        long daysSinceLastSweep = TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS);
        if (writeCount <= 100 + cellTsPairsExamined / 100 && daysSinceLastSweep < 180) {
            // Not worth the effort if fewer than 1% of cells are new and we've swept in the last 6 months.
            return 0.0;
        }

        if (newPriority.staleValuesDeleted() > WAIT_BEFORE_SWEEPING_IF_WE_GENERATE_THIS_MANY_TOMBSTONES
                && daysSinceLastSweep < 1
                && kvs.performanceIsSensitiveToTombstones()) {
            // we created many tombstones on the last run - wait a bit before sweeping again.
            return 0.0;
        }

        // This ordering function weights one month of no sweeping
        // with the same priority as about 100000 expected cells to sweep.
        return estimatedCellTsPairsToSweep + millisSinceSweep * MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT;
    }

    @Value.Immutable
    public interface TableWithPriority {
        TableReference tableRefence();
        Double priority();

        static TableWithPriority create(TableReference tableReference, Double priority) {
            return ImmutableTableWithPriority.builder()
                    .tableRefence(tableReference)
                    .priority(priority)
                    .build();
        }
    }
}
