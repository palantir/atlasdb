/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class SimpleKvsTimestampBoundStore implements TimestampBoundStore {
    public static final String TIMESTAMP_TABLE = "_timestamp";
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final long KV_TS = 0L;
    private static final Cell TS_CELL = Cell.create(ROW_AND_COLUMN_NAME.getBytes(Charsets.UTF_8), ROW_AND_COLUMN_NAME.getBytes(Charsets.UTF_8));
    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
        NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("timestamp_name", ValueType.STRING))),
        new ColumnMetadataDescription(ImmutableList.of(
            new NamedColumnDescription(ROW_AND_COLUMN_NAME, "current_max_ts", ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
        ConflictHandler.IGNORE_ALL);

    private static final long INITIAL_VALUE = 10000L;

    public static TimestampBoundStore create(KeyValueService kv) {
        kv.createTable(TIMESTAMP_TABLE, TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new SimpleKvsTimestampBoundStore(kv);
    }

    @GuardedBy("this")
    private long currentLimit = -1;
    @GuardedBy("this")
    private Throwable lastWriteException = null;
    private final KeyValueService kv;

    private SimpleKvsTimestampBoundStore(KeyValueService kv) {
        this.kv = kv;
    }

    @Override
    public synchronized long getUpperLimit() {
        Map<Cell, Value> result = kv.get(TIMESTAMP_TABLE, ImmutableMap.of(TS_CELL, KV_TS+1));
        if (result.isEmpty()) {
            putValue(INITIAL_VALUE);
        }
        result = kv.get(TIMESTAMP_TABLE, ImmutableMap.of(TS_CELL, KV_TS+1));
        currentLimit = getValueFromResult(result);
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceError {
        Map<Cell, Value> result = kv.get(TIMESTAMP_TABLE, ImmutableMap.of(TS_CELL, KV_TS+1));
        long oldValue = getValueFromResult(result);
        if (oldValue != currentLimit) {
            String msg = "Timestamp limit changed underneath us (limit in memory: " + currentLimit
                    + "). This may indicate that "
                    + "another timestamp service is running against this key value store!";
            throw new MultipleRunningTimestampServiceError(msg);
        }
        putValue(limit);
        currentLimit = limit;
    }

    private void putValue(long value) {
        kv.put(TIMESTAMP_TABLE, ImmutableMap.of(TS_CELL, PtBytes.toBytes(value)), KV_TS);
    }

    private long getValueFromResult(Map<Cell, Value> result) {
        return PtBytes.toLong(result.get(TS_CELL).getContents());
    }
}
