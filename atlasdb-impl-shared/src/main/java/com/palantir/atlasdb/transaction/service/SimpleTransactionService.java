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

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class SimpleTransactionService implements EncodingTransactionService {
    private final KeyValueService keyValueService;
    private final TimestampEncodingStrategy encodingStrategy = V1EncodingStrategy.INSTANCE;

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
            return encodingStrategy.decodeValueAsCommitTimestamp(startTimestamp, returnMap.get(cell).getContents());
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
        Map<Long, Long> result = Maps.newHashMapWithExpectedSize(rawResults.size());
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = encodingStrategy.decodeCellAsStartTimestamp(e.getKey());
            long commitTs = encodingStrategy.decodeValueAsCommitTimestamp(startTs, e.getValue().getContents());
            result.put(startTs, commitTs);
        }

        return result;
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = encodingStrategy.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp);
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(key, value));
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        Map<Cell, byte[]> values = startTimestampToCommitTimestamp.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> getTransactionCell(entry.getKey()),
                        entry -> encodingStrategy.encodeCommitTimestampAsValue(
                                entry.getKey(), entry.getValue())));
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, values);
    }

    private Cell getTransactionCell(long startTimestamp) {
        return encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public TimestampEncodingStrategy getEncodingStrategy() {
        return encodingStrategy;
    }
}
