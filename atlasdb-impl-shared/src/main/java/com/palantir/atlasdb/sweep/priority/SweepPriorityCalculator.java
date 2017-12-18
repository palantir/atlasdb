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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.logsafe.SafeArg;

public class SweepPriorityCalculator {
    private static final Logger log = LoggerFactory.getLogger(SweepPriorityCalculator.class);

    private static final int WAIT_BEFORE_SWEEPING_IF_WE_GENERATE_THIS_MANY_TOMBSTONES = 1_000_000;
    private static final Duration WAIT_BEFORE_SWEEPING_STREAM_STORE_VALUE_TABLE = Duration.ofDays(3);
    @VisibleForTesting static final int STREAM_STORE_VALUES_TO_SWEEP = 1_000;

    // weights one month of no sweeping with the same priority as about 100000 expected cells to sweep.
    private static final double MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT =
            100_000.0 / TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS);

    private final KeyValueService kvs;
    private final SweepPriorityStore sweepPriorityStore;

    public SweepPriorityCalculator(KeyValueService kvs, SweepPriorityStore sweepPriorityStore) {
        this.kvs = kvs;
        this.sweepPriorityStore = sweepPriorityStore;
    }

    Map<TableReference, Double> calculateSweepPriorityScores(Transaction tx, long conservativeSweepTs) {
        Set<TableReference> allTables = Sets.difference(kvs.getAllTableNames(), AtlasDbConstants.hiddenTables);

        // We read priorities from the past because we should prioritize based on what the sweeper will
        // actually be able to sweep. We read priorities from the present to make sure we don't repeatedly
        // sweep the same table while waiting for the past to catch up.
        List<SweepPriority> oldPriorities = sweepPriorityStore.loadOldPriorities(tx, conservativeSweepTs);
        List<SweepPriority> newPriorities = sweepPriorityStore.loadNewPriorities(tx);

        return getSweepScores(tx, allTables, oldPriorities, newPriorities);
    }

    private Map<TableReference, Double> getSweepScores(
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
        Map<TableReference, Double> scores = new HashMap<>(oldPriorities.size());
        Collection<TableReference> toDelete = Lists.newArrayList();
        for (SweepPriority oldPriority : oldPriorities) {
            TableReference tableReference = oldPriority.tableRef();

            if (allTables.contains(tableReference)) {
                SweepPriority newPriority = newPrioritiesByTableName.get(tableReference);
                scores.put(tableReference, getSweepPriorityScore(oldPriority, newPriority));
            } else {
                toDelete.add(tableReference);
            }
        }

        // Clean up rows for tables that no longer exist.
        sweepPriorityStore.delete(tx, toDelete);

        return scores;
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

    private double getSweepPriorityScore(SweepPriority oldPriority, SweepPriority newPriority) {
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
            return getStreamStorePriorityScore(newPriority);
        } else {
            return getNonStreamStorePriorityScore(oldPriority, newPriority);
        }
    }

    private double getStreamStorePriorityScore(SweepPriority newPriority) {
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

    private double getNonStreamStorePriorityScore(SweepPriority oldPriority, SweepPriority newPriority) {
        long staleValuesDeleted = Math.max(1, oldPriority.staleValuesDeleted());
        long cellTsPairsExamined = Math.max(1, oldPriority.cellTsPairsExamined());
        long writeCount = Math.max(1, oldPriority.writeCount()); // TODO(tboam): bug? should this be newPriority?
        double previousEfficacy = 1.0 * staleValuesDeleted / cellTsPairsExamined;
        double estimatedCellTsPairsToSweep = previousEfficacy * writeCount;
        long millisSinceSweep = System.currentTimeMillis() - newPriority.lastSweepTimeMillis().getAsLong();

        if (tooFewWritesToBother(writeCount, cellTsPairsExamined, millisSinceSweep)) {
            return 0.0;
        }

        if (weWantToAvoidOverloadingTheStoreWithTombstones(newPriority, millisSinceSweep)) {
            return 0.0;
        }

        // This ordering function weights one month of no sweeping
        // with the same priority as about 100000 expected cells to sweep.
        return estimatedCellTsPairsToSweep + millisSinceSweep * MILLIS_SINCE_SWEEP_PRIORITY_WEIGHT;
    }

    private boolean tooFewWritesToBother(long writeCount, long cellTsPairsExamined, long millisSinceSweep) {
        // don't bother sweeping a table that has had very few writes compared to its size last time sweep ran
        // for large tables we're essentially just comparing writeCount <= cellTsPairsExamined / 100
        boolean fewWrites = writeCount <= 100 + cellTsPairsExamined / 100;

        long daysSinceLastSweep = TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS);

        return fewWrites && daysSinceLastSweep < 180;
    }

    private boolean weWantToAvoidOverloadingTheStoreWithTombstones(SweepPriority newPriority,
            long millisSinceSweep) {
        long daysSinceLastSweep = TimeUnit.DAYS.convert(millisSinceSweep, TimeUnit.MILLISECONDS);

        return newPriority.staleValuesDeleted() > WAIT_BEFORE_SWEEPING_IF_WE_GENERATE_THIS_MANY_TOMBSTONES
                && daysSinceLastSweep < 1
                && kvs.performanceIsSensitiveToTombstones();
    }
}
