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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public static final Duration WAIT_BEFORE_SWEEPING_STREAM_STORE_VALUE_TABLE = Duration.ofDays(3);
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
    public Map<TableReference, Double> computeSweepPriorities(Transaction tx, long conservativeSweepTs) {
        Set<TableReference> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);

        // We read priorities from the past because we should prioritize based on what the sweeper will
        // actually be able to sweep. We read priorities from the present to make sure we don't repeatedly
        // sweep the same table while waiting for the past to catch up.
        List<SweepPriority> oldPriorities = sweepPriorityStore.loadOldPriorities(tx, conservativeSweepTs);
        List<SweepPriority> newPriorities = sweepPriorityStore.loadNewPriorities(tx);

        return getTableToSweep(tx, allTables, oldPriorities, newPriorities);
    }

    private Map<TableReference, Double> getTableToSweep(
            Transaction tx,
            Set<TableReference> allTables,
            List<SweepPriority> oldPriorities,
            List<SweepPriority> newPriorities) {

        Set<TableReference> unsweptTables = Sets.difference(allTables,
                newPriorities.stream().map(SweepPriority::tableRef).collect(Collectors.toSet()));
        if (!unsweptTables.isEmpty()) {
            // Always sweep unswept tables first
            logUnsweptTables(unsweptTables);
            return unsweptTables.stream().collect(Collectors.toMap(Function.identity(), tableReference -> 100.0));
        }

        Map<TableReference, SweepPriority> newPrioritiesByTableName = newPriorities.stream().collect(
                Collectors.toMap(SweepPriority::tableRef, Function.identity()));

        // Compute priority for tables that do have a priority table.
        Map<TableReference, Double> tableToPriority = new HashMap<>(oldPriorities.size());
        Collection<TableReference> toDelete = Lists.newArrayList();
        for (SweepPriority oldPriority : oldPriorities) {
            TableReference tableReference = oldPriority.tableRef();

            if (allTables.contains(tableReference)) {
                SweepPriority newPriority = newPrioritiesByTableName.get(tableReference);
                tableToPriority.put(tableReference, getSweepPriority(oldPriority, newPriority));
            } else {
                toDelete.add(tableReference);
            }
        }

        // Clean up rows for tables that no longer exist.
        sweepPriorityStore.delete(tx, toDelete);

        logPrioritiesByTable(tableToPriority);

        return tableToPriority;
    }

    private void logUnsweptTables(Set<TableReference> unsweptTables) {
        if (!log.isDebugEnabled()) {
            return;
        }

        LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafeTableReferences = LoggingArgs.tableRefs(unsweptTables);

        log.debug("Unswept tables: {} and {}",
                SafeArg.of("tables", safeAndUnsafeTableReferences.safeTableRefs()),
                SafeArg.of("unsafeTables", safeAndUnsafeTableReferences.unsafeTableRefs()));
    }

    private void logPrioritiesByTable(Map<TableReference, Double> tableToPriority) {
        if (!log.isDebugEnabled()) {
            return;
        }

        List<Map.Entry<TableReference, Double>> tablesSortedByPriority = tableToPriority.entrySet().stream()
                .sorted(Comparator.comparingDouble(Map.Entry::getValue))
                .collect(Collectors.toList());

        List<Map.Entry<TableReference, Double>> safeTablesEntries = new ArrayList<>();
        List<Map.Entry<TableReference, Double>> unsafeTablesEntries = new ArrayList<>();

        for (Map.Entry<TableReference, Double> tableEntry : tablesSortedByPriority) {
            if (LoggingArgs.tableRef(tableEntry.getKey()).isSafeForLogging()) {
                safeTablesEntries.add(tableEntry);
            } else {
                unsafeTablesEntries.add(tableEntry);
            }
        }

        log.debug("Sweep priorities per table: {} and {}",
                SafeArg.of("tables", safeTablesEntries.toString()),
                SafeArg.of("unsafeTables", unsafeTablesEntries.toString()));
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

        if (millisSinceSweep < WAIT_BEFORE_SWEEPING_STREAM_STORE_VALUE_TABLE.toMillis()) {
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
        long writeCount = Math.max(1, oldPriority.writeCount()); // TODO(tboam): should this be newPriority?
        double previousEfficacy = 1.0 * staleValuesDeleted / cellTsPairsExamined;
        double estimatedCellTsPairsToSweep = previousEfficacy * writeCount;
        long millisSinceSweep = System.currentTimeMillis() - newPriority.lastSweepTimeMillis().getAsLong();

        long daysSinceLastSweep = TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS);
        if (writeCount <= 100 + cellTsPairsExamined / 100 && daysSinceLastSweep < 180) {
            // Not worth the effort if fewer than 1% of cells are new and we've swept in the last 6 months.
            // TODO(tboam): is this really 1%?
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
}
