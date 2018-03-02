/*
 * Copyright 2018 Palantir Technologies
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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;

class SweepHistoryProvider {
    private static final Logger log = LoggerFactory.getLogger(SweepHistoryProvider.class);
    Map<String, Long> getHistory(Transaction tx) {
        Map<String, Long> tableToLastTimeSwept = new HashMap<>();
        log.info("compact - shp<1>");
        SweepPriorityTable sweepPriorityTable = SweepTableFactory.of().getSweepPriorityTable(tx);
        log.info("compact - shp<2>");
        sweepPriorityTable.getAllRowsUnordered(SweepPriorityTable.getColumnSelection(
                SweepPriorityTable.SweepPriorityNamedColumn.LAST_SWEEP_TIME))
                .forEach(row -> {
                    Long lastSweepTime = row.getLastSweepTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeSwept.put(tableName, lastSweepTime);
                });
        log.info("compact - shp<3>");
        return tableToLastTimeSwept;
    }
}
