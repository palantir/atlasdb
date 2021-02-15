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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.annotation.Output;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StreamStoreRemappingSweepPriorityCalculator {
    public static final long INDEX_TO_VALUE_TABLE_SLEEP_TIME = TimeUnit.MINUTES.toMillis(65);
    private SweepPriorityCalculator delegate;
    private SweepPriorityStore sweepPriorityStore;

    public StreamStoreRemappingSweepPriorityCalculator(
            SweepPriorityCalculator delegate, SweepPriorityStore sweepPriorityStore) {
        this.delegate = delegate;
        this.sweepPriorityStore = sweepPriorityStore;
    }

    public Map<TableReference, Double> calculateSweepPriorityScores(Transaction tx, long conservativeSweepTs) {
        Map<TableReference, Double> scores = delegate.calculateSweepPriorityScores(tx, conservativeSweepTs);

        Map<TableReference, SweepPriority> tableToSweepPriority = getSweepPriorityMap(tx);

        for (TableReference table : scores.keySet()) {
            if (StreamTableType.isStreamStoreValueTable(table)) {
                adjustStreamStoreScores(table, scores, tableToSweepPriority);
            }
        }

        return scores;
    }

    private Map<TableReference, SweepPriority> getSweepPriorityMap(Transaction tx) {
        return sweepPriorityStore.loadNewPriorities(tx).stream()
                .collect(Collectors.toMap(SweepPriority::tableRef, Function.identity()));
    }

    private void adjustStreamStoreScores(
            TableReference valueTable,
            @Output Map<TableReference, Double> scores,
            Map<TableReference, SweepPriority> tableToSweepPriority) {

        TableReference indexTable = StreamTableType.getIndexTableFromValueTable(valueTable);
        if (!scores.containsKey(indexTable)) {
            // unlikely, but don't alter the score of something that hasn't been included as a candidate
            return;
        }

        long lastSweptTimeOfValueTable = getLastSweptTime(valueTable, tableToSweepPriority);
        long lastSweptTimeOfIndexTable = getLastSweptTime(indexTable, tableToSweepPriority);

        if (lastSweptTimeOfValueTable >= lastSweptTimeOfIndexTable) {
            // We want to sweep the value table but haven't yet done the index table.  Do the index table first.
            scores.put(indexTable, scores.get(valueTable));
            doNotSweepTable(valueTable, scores);
        } else if (System.currentTimeMillis() - lastSweptTimeOfIndexTable <= INDEX_TO_VALUE_TABLE_SLEEP_TIME) {
            // We've done the index table recently:
            // 1) wait a bit before we do the value table so that the unreadable timestamp has passed (wait > 1 hour).
            // 2) ensure we don't sweep index table again as we could starve the value table if index sweeps too often
            doNotSweepTable(valueTable, scores);
            doNotSweepTable(indexTable, scores);
        } else {
            // The index table has been swept long enough ago that we can now sweep the value table
        }
    }

    private long getLastSweptTime(TableReference table, Map<TableReference, SweepPriority> tableToSweepPriority) {
        if (!tableToSweepPriority.containsKey(table)) {
            return 0L;
        }
        return tableToSweepPriority.get(table).lastSweepTimeMillis().orElse(0L);
    }

    private void doNotSweepTable(TableReference table, @Output Map<TableReference, Double> scores) {
        scores.put(table, 0.0);
    }
}
