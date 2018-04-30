/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public final class SimpleTransactionService implements TransactionService {
    private final KeyValueService keyValueService;

    public SimpleTransactionService(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    // The maximum key-value store timestamp (exclusive) at which data is stored
    // in transaction table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;

    @Override
    public Long get(long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        Map<Cell, Value> returnMap = keyValueService.get(
                TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(cell, MAX_TIMESTAMP));
        if (returnMap.containsKey(cell)) {
            return TransactionConstants.getTimestampForValue(returnMap
                    .get(cell).getContents());
        } else {
            return null;
        }
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Cell, Long> startTsMap = Maps.newHashMap();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = getTransactionCell(startTimestamp);
            startTsMap.put(cell, MAX_TIMESTAMP);
        }

        Map<Cell, Value> rawResults = keyValueService.get(
                TransactionConstants.TRANSACTION_TABLE, startTsMap);
        Map<Long, Long> result = Maps.newHashMapWithExpectedSize(rawResults
                .size());
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = TransactionConstants.getTimestampForValue(e.getKey()
                    .getRowName());
            long commitTs = TransactionConstants.getTimestampForValue(e
                    .getValue().getContents());
            result.put(startTs, commitTs);
        }

        return result;
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = TransactionConstants
                .getValueForTimestamp(commitTimestamp);
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE,
                ImmutableMap.of(key, value));
    }

    private Cell getTransactionCell(long startTimestamp) {
        return Cell.create(
                TransactionConstants.getValueForTimestamp(startTimestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
    }
}
