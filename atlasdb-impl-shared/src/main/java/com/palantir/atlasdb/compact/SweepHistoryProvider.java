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

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.HashMap;
import java.util.Map;

class SweepHistoryProvider {

    private static final int READ_BATCH_SIZE = 100;

    Map<String, Long> getHistory(Transaction tx) {
        Map<String, Long> tableToLastTimeSwept = new HashMap<>();
        SweepPriorityTable sweepPriorityTable = SweepTableFactory.of().getSweepPriorityTable(tx);
        sweepPriorityTable
                .getRange(RangeRequest.builder()
                        .retainColumns(SweepPriorityTable.getColumnSelection(SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                        .batchHint(READ_BATCH_SIZE)
                        .build())
                .forEach(row -> {
                    Long lastSweepTime = row.getLastSweepTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeSwept.put(tableName, lastSweepTime);
                });
        return tableToLastTimeSwept;
    }
}
