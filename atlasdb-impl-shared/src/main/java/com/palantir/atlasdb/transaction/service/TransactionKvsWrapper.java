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
package com.palantir.atlasdb.transaction.service;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.HashMap;
import java.util.Map;

public class TransactionKvsWrapper {
    // The maximum key-value store timestamp (exclusive) at which data is stored in transaction table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;

    private final KeyValueService keyValueService;

    public TransactionKvsWrapper(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    private static Cell getTransactionCell(long startTimestamp) {
        return Cell.create(
                TransactionConstants.getValueForTimestamp(startTimestamp), TransactionConstants.COMMIT_TS_COLUMN);
    }

    public Long get(Long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        Map<Cell, Value> returnMap =
                keyValueService.get(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(cell, MAX_TIMESTAMP));
        if (returnMap.containsKey(cell)) {
            return TransactionConstants.getTimestampForValue(returnMap.get(cell).getContents());
        } else {
            return null;
        }
    }

    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Long, Long> result = new HashMap<>();
        Map<Cell, Long> startTsMap = new HashMap<>();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = getTransactionCell(startTimestamp);
            startTsMap.put(cell, MAX_TIMESTAMP);
        }

        Map<Cell, Value> rawResults = keyValueService.get(TransactionConstants.TRANSACTION_TABLE, startTsMap);
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = TransactionConstants.getTimestampForValue(e.getKey().getRowName());
            long commitTs =
                    TransactionConstants.getTimestampForValue(e.getValue().getContents());
            result.put(startTs, commitTs);
        }

        return result;
    }

    // It works only if key-value store supports putUnlessExists.
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = TransactionConstants.getValueForTimestamp(commitTimestamp);
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(key, value));
    }

    public void putAll(Map<Long, Long> timestampMap) throws KeyAlreadyExistsException {
        Map<Cell, byte[]> kvMap = new HashMap<>();
        for (Map.Entry<Long, Long> entry : timestampMap.entrySet()) {
            kvMap.put(getTransactionCell(entry.getKey()), TransactionConstants.getValueForTimestamp(entry.getValue()));
        }

        keyValueService.put(TransactionConstants.TRANSACTION_TABLE, kvMap, 0); // This can throw unchecked exceptions
    }
}
