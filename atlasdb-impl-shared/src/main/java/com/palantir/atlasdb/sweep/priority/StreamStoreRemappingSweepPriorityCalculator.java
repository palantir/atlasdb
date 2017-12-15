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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.annotation.Output;

public class StreamStoreRemappingSweepPriorityCalculator {
    private SweepPriorityCalculator delegate;
    private SweepPriorityStore sweepPriorityStore;

    public StreamStoreRemappingSweepPriorityCalculator(SweepPriorityCalculator delegate,
            SweepPriorityStore sweepPriorityStore) {
        this.delegate = delegate;
        this.sweepPriorityStore = sweepPriorityStore;
    }

    public Map<TableReference, Double> calculateSweepPriorities(Transaction tx, long conservativeSweepTs) {
        Map<TableReference, Double> tableToScore = delegate.calculateSweepPriorities(tx, conservativeSweepTs);
        if (tableToScore.isEmpty()) {
            return tableToScore;
        }

        Map<TableReference, SweepPriority> tableToSweepPriority = new HashMap<>();

        for (SweepPriority sweepPriority : sweepPriorityStore.loadNewPriorities(tx)) {
            tableToSweepPriority.put(sweepPriority.tableRef(), sweepPriority);
        }

        for (TableReference table : tableToSweepPriority.keySet()) {
            if (StreamTableType.isStreamStoreValueTable(table)) {
                adjustStreamStorePriority(table, tableToScore, tableToSweepPriority);
            }
        }

        return tableToScore;
    }

    // if ss.value >= ss.index -> ignore value and sweep index
    // if ss.value <  ss.index && ss.index <= 1 hour ago -> ignore value
    // if ss.value <  ss.index && ss.index >  1 hour ago -> sweep value
    private void adjustStreamStorePriority(TableReference valueTable,
            @Output Map<TableReference, Double> tableToScore,
            Map<TableReference, SweepPriority> tableToSweepPriority) {

        TableReference indexTable = StreamTableType.getIndexTableFromValueTable(valueTable);
        if (!tableToScore.containsKey(indexTable)) {
            // unlikely, but don't boost the score of something that hasn't been included as a candidate
            return;
        }

        long lastSweptTimeOfValueTable = getLastSweptTime(tableToSweepPriority, valueTable);
        long lastSweptTimeOfIndexTable = getLastSweptTime(tableToSweepPriority, indexTable);

        if (lastSweptTimeOfValueTable >= lastSweptTimeOfIndexTable) {
            bumpIndexTablePriorityAndIgnoreValueTablePriority(tableToScore, valueTable, indexTable);
        } else if (System.currentTimeMillis() - lastSweptTimeOfIndexTable <= TimeUnit.HOURS.toMillis(1)) {
            doNotSweepTable(tableToScore, valueTable);
            doNotSweepTable(tableToScore, indexTable);
        }
    }

    private long getLastSweptTime(Map<TableReference, SweepPriority> tableToSweepPriority, TableReference table) {
        if (!tableToSweepPriority.containsKey(table)) {
            return 0L;
        }
        return tableToSweepPriority.get(table).lastSweepTimeMillis().orElse(0L);
    }

    private void bumpIndexTablePriorityAndIgnoreValueTablePriority(
            @Output Map<TableReference, Double> tableToPriority,
            TableReference valueTable,
            TableReference indexTable) {
        tableToPriority.put(indexTable, tableToPriority.get(valueTable));
        doNotSweepTable(tableToPriority, valueTable);
    }

    private void doNotSweepTable(@Output Map<TableReference, Double> tableToPriority, TableReference table) {
        tableToPriority.put(table, 0.0);
    }
}
