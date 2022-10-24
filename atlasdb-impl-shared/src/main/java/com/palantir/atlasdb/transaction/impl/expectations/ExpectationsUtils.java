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

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

public final class ExpectationsUtils {
    private ExpectationsUtils() {}

    public static long longByCellByteSize(Map<Cell, Long> timestampByCell) {
        return timestampByCell.keySet().stream()
                .mapToLong(cell -> cell.byteSize() + Long.BYTES)
                .sum();
    }

    public static long longByCellByteSize(Multimap<Cell, Long> valueByCell) {
        return valueByCell.keys().stream()
                .mapToLong(cell -> cell.byteSize() + Long.BYTES)
                .sum();
    }

    public static long valueByCellByteSize(Map<Cell, Value> valueByCell) {
        return valueByCell.entrySet().stream()
                .mapToLong(ExpectationsUtils::byteSize)
                .sum();
    }

    public static long pageByRangeRequestByteSize(
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRange) {
        return pageByRange.values().stream()
                .mapToLong(page -> page.getTokenForNextPage().length
                        + page.getResults().stream()
                                .mapToLong(ExpectationsUtils::valueRowResultByteSize)
                                .sum())
                .sum();
    }

    public static long valueRowResultByteSize(RowResult<Value> rowResult) {
        return rowResultByteSize(rowResult, Value::byteSize);
    }

    public static long longSetRowResultByteSize(RowResult<Set<Long>> rowResult) {
        return rowResultByteSize(rowResult, set -> (long) set.size() * Long.BYTES);
    }

    private static <T> long rowResultByteSize(RowResult<T> rowResult, Function<T, Long> measurer) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + measurer.apply(entry.getValue()))
                        .sum();
    }

    public static long byteSize(Entry<Cell, Value> entry) {
        return entry.getKey().byteSize() + entry.getValue().byteSize();
    }

    public static long byteSize(List<byte[]> array) {
        return array.stream().mapToLong(Array::getLength).sum();
    }
}
