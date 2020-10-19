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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestDataBuilder {
    private final KeyValueService kvs;
    private final TableReference table;

    private Map<Long, Map<Cell, byte[]>> cellsByTimestamp = new HashMap<>();

    public TestDataBuilder(KeyValueService kvs, TableReference table) {
        this.kvs = kvs;
        this.table = table;
    }

    public TestDataBuilder put(int row, int col, long ts, String val) {
        return put(row, col, ts, value(val));
    }

    public TestDataBuilder put(int row, int col, long ts) {
        return put(row, col, ts, value("foobar"));
    }

    public TestDataBuilder put(int row, int col, long ts, byte[] value) {
        cellsByTimestamp.computeIfAbsent(ts, key -> new HashMap<>()).put(cell(row, col), value);
        return this;
    }

    public TestDataBuilder putEmpty(int row, int col, long ts) {
        return put(row, col, ts, PtBytes.EMPTY_BYTE_ARRAY);
    }

    public void store() {
        for (Map.Entry<Long, Map<Cell, byte[]>> e : cellsByTimestamp.entrySet()) {
            kvs.put(table, e.getValue(), e.getKey());
        }
    }

    public static Cell cell(int rowNum, int colNum) {
        return Cell.create(row(rowNum), row(colNum));
    }

    public static byte[] row(int rowNum) {
        return Ints.toByteArray(rowNum);
    }

    public static byte[] value(String valueStr) {
        return valueStr.getBytes(StandardCharsets.UTF_8);
    }
}
