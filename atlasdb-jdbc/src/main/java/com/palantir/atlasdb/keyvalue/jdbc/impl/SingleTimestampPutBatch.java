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
package com.palantir.atlasdb.keyvalue.jdbc.impl;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.collect.Maps2;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.jooq.InsertValuesStep4;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Row3;
import org.jooq.impl.DSL;

public class SingleTimestampPutBatch implements PutBatch {
    private final Map<Cell, byte[]> data;
    private final Long timestamp;

    public SingleTimestampPutBatch(Map<Cell, byte[]> data, Long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
    }

    public static SingleTimestampPutBatch create(List<Map.Entry<Cell, byte[]>> data, long timestamp) {
        return new SingleTimestampPutBatch(Maps2.fromEntries(data), timestamp);
    }

    @Override
    public InsertValuesStep4<Record, byte[], byte[], Long, byte[]> addValuesForInsert(
            InsertValuesStep4<Record, byte[], byte[], Long, byte[]> query) {
        for (Map.Entry<Cell, byte[]> entry : data.entrySet()) {
            query = query.values(
                    entry.getKey().getRowName(), entry.getKey().getColumnName(), timestamp, entry.getValue());
        }
        return query;
    }

    @Override
    public Collection<Row3<byte[], byte[], Long>> getRowsForSelect() {
        return Collections2.transform(
                data.keySet(), cell -> DSL.row(cell.getRowName(), cell.getColumnName(), timestamp));
    }

    @Override
    @Nullable
    public PutBatch getNextBatch(Result<? extends Record> existingRecords) {
        Map<Cell, byte[]> existing = Maps.newHashMapWithExpectedSize(existingRecords.size());
        for (Record record : existingRecords) {
            existing.put(
                    Cell.create(record.getValue(JdbcConstants.A_ROW_NAME), record.getValue(JdbcConstants.A_COL_NAME)),
                    record.getValue(JdbcConstants.A_VALUE));
        }
        Map<Cell, byte[]> nextBatch = new HashMap<>();
        for (Map.Entry<Cell, byte[]> entry : data.entrySet()) {
            Cell cell = entry.getKey();
            byte[] newValue = entry.getValue();
            byte[] oldValue = existing.get(cell);
            if (oldValue == null) {
                nextBatch.put(cell, newValue);
            } else if (!Arrays.equals(oldValue, newValue)) {
                return null;
            }
        }
        return new SingleTimestampPutBatch(nextBatch, timestamp);
    }
}
