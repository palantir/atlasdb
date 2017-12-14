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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
        Map<TableReference, Double> tableToPriority = delegate.calculateSweepPriorities(tx, conservativeSweepTs);
        if (tableToPriority.isEmpty()) {
            return tableToPriority;
        }

        List<TableReference> tablesWithHighestPriority = NextTableToSweepProvider.findTablesWithHighestPriority(
                tableToPriority);

        List<TableReference> valueTablesWithHighestPriority = tablesWithHighestPriority.stream()
                .filter(StreamTableType::isStreamStoreValueTable)
                .collect(Collectors.toList());
        if (valueTablesWithHighestPriority.size() == 0) {
            return tableToPriority;
        }

        Map<TableReference, SweepPriority> indexToPriority = new HashMap<>(valueTablesWithHighestPriority.size());
        Map<TableReference, SweepPriority> valueToPriority = new HashMap<>(valueTablesWithHighestPriority.size());

        for (SweepPriority sweepPriority : sweepPriorityStore.loadNewPriorities(tx)) {
            if (StreamTableType.isStreamStoreIndexTable(sweepPriority.tableRef())) {
                indexToPriority.put(sweepPriority.tableRef(), sweepPriority);
            } else if (StreamTableType.isStreamStoreValueTable(sweepPriority.tableRef())) {
                valueToPriority.put(sweepPriority.tableRef(), sweepPriority);
            }
        }

        if (!didSweepAllIndexAndValueTables(indexToPriority, valueToPriority)) {
            return tableToPriority;
        }

        for (Map.Entry<TableReference, SweepPriority> valueEntry : valueToPriority.entrySet()) {
            adjustStreamStorePriority(tableToPriority, indexToPriority, valueEntry);
        }

        return tableToPriority;
    }

    private boolean didSweepAllIndexAndValueTables(
            Map<TableReference, SweepPriority> indexToPriority,
            Map<TableReference, SweepPriority> valueToPriority) {
        return indexToPriority.size() == valueToPriority.size() && indexToPriority.size() != 0;
    }

    // if ss.value >= ss.index -> ignore value and sweep index
    // if ss.value <  ss.index && ss.index <= 1 hour ago -> ignore value
    // if ss.value <  ss.index && ss.index >  1 hour ago -> sweep value
    private void adjustStreamStorePriority(@Output Map<TableReference, Double> tableToPriority,
            Map<TableReference, SweepPriority> indexToPriority,
            Map.Entry<TableReference, SweepPriority> valueEntry) {

        TableReference valueTable = valueEntry.getKey();
        long lastSweptTimeOfValueTable = valueEntry.getValue().lastSweepTimeMillis().orElse(0L);
        TableReference indexTable = StreamTableType.getIndexTableFromValueTable(valueTable);
        long lastSweptTimeOfIndexTable = indexToPriority.get(indexTable).lastSweepTimeMillis().orElse(0L);

        if (lastSweptTimeOfValueTable >= lastSweptTimeOfIndexTable) {
            bumpIndexTablePriorityAndIgnoreValueTablePriority(tableToPriority, valueTable, indexTable);
        } else if (System.currentTimeMillis() - lastSweptTimeOfIndexTable <= TimeUnit.HOURS.toMillis(1)) {
            doNotSweepTable(tableToPriority, valueTable);
            doNotSweepTable(tableToPriority, indexTable);
        }
    }

    private void bumpIndexTablePriorityAndIgnoreValueTablePriority(@Output Map<TableReference, Double> tableToPriority,
            TableReference valueTable,
            TableReference indexTable) {
        doNotSweepTable(tableToPriority, valueTable);
        tableToPriority.put(indexTable, Double.MAX_VALUE);
    }

    private void doNotSweepTable(@Output Map<TableReference, Double> tableToPriority, TableReference valueTable) {
        tableToPriority.put(valueTable, 0.0);
    }
}
