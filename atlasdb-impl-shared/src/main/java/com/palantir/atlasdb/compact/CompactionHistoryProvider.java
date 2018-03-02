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

import com.palantir.atlasdb.schema.generated.CompactMetadataTable;
import com.palantir.atlasdb.schema.generated.CompactTableFactory;
import com.palantir.atlasdb.transaction.api.Transaction;

class CompactionHistoryProvider {
    private static final Logger log = LoggerFactory.getLogger(CompactionHistoryProvider.class);

    Map<String, Long> getHistory(Transaction tx) {
        log.info("compact - chp<1>");
        Map<String, Long> tableToLastTimeCompacted = new HashMap<>();
        CompactMetadataTable compactMetadataTable = CompactTableFactory.of().getCompactMetadataTable(tx);
        log.info("compact - chp<2>");
        compactMetadataTable.getAllRowsUnordered(CompactMetadataTable.getColumnSelection(
                CompactMetadataTable.CompactMetadataNamedColumn.LAST_COMPACT_TIME))
                .forEach(row -> {
                    Long lastCompactTime = row.getLastCompactTime();
                    String tableName = row.getRowName().getFullTableName();
                    tableToLastTimeCompacted.put(tableName, lastCompactTime);
                });
        log.info("compact - chp<3>");
        return tableToLastTimeCompacted;
    }
}
