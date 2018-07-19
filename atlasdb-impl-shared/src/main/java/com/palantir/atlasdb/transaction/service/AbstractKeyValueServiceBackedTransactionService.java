/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;

public abstract class AbstractKeyValueServiceBackedTransactionService implements TransactionService {
    // The maximum key-value store timestamp (exclusive) at which data is stored in the transactions table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;

    @Override
    public final Long get(long startTimestamp) {
        Cell cell = encodeTimestampAsCell(startTimestamp);
        Map<Cell, Value> returnMap = getKeyValueService().get(
                getTableReference(),
                ImmutableMap.of(cell, MAX_TIMESTAMP));
        if (returnMap.containsKey(cell)) {
            return decodeValueAsTimestamp(returnMap.get(cell).getContents());
        } else {
            return null;
        }
    }

    @Override
    public final Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Cell, Long> startTsMap = Maps.newHashMap();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = encodeTimestampAsCell(startTimestamp);
            startTsMap.put(cell, MAX_TIMESTAMP);
        }

        Map<Cell, Value> rawResults = getKeyValueService().get(getTableReference(), startTsMap);
        Map<Long, Long> result = Maps.newHashMapWithExpectedSize(rawResults.size());
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = decodeCellAsTimestamp(e.getKey());
            long commitTs = decodeValueAsTimestamp(e.getValue().getContents());
            result.put(startTs, commitTs);
        }
        return result;
    }

    @Override
    public final void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        Cell key = encodeTimestampAsCell(startTimestamp);
        byte[] value = encodeTimestampAsValue(commitTimestamp);
        getKeyValueService().putUnlessExists(getTableReference(), ImmutableMap.of(key, value));
    }

    public abstract KeyValueService getKeyValueService();

    public abstract TableReference getTableReference();

    public abstract Cell encodeTimestampAsCell(long startTimestamp);

    public abstract long decodeCellAsTimestamp(Cell cell);

    public abstract byte[] encodeTimestampAsValue(long startTimestamp);

    public abstract long decodeValueAsTimestamp(byte[] value);
}
