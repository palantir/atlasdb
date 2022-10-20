/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.Map;
import java.util.Map.Entry;

public final class ExpectationsUtils {
    private ExpectationsUtils() {}

    public static long longByCellByteSize(Map<Cell, Long> timestampByCell) {
        return timestampByCell.keySet().stream()
                .mapToLong(cell -> byteSize(cell) + 8)
                .sum();
    }

    public static long valueByCellByteSize(Map<Cell, Value> valueByCell) {
        return valueByCell.entrySet().stream()
                .mapToLong(ExpectationsUtils::valueByCellEntryByteSize)
                .sum();
    }

    public static long byteSize(Cell cell) {
        return cell.getRowName().length + cell.getColumnName().length;
    }

    public static long byteSize(Value value) {
        return value.getContents().length;
    }

    public static long byteSize(RowResult<Value> rowResult) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + byteSize(entry.getValue()))
                        .sum();
    }

    public static long valueByCellEntryByteSize(Entry<Cell, Value> entry) {
        return byteSize(entry.getKey()) + byteSize(entry.getValue());
    }
}
