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
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class ExpectationsUtils {
    public static final long LONG_BYTES = 8;

    private ExpectationsUtils() {}

    public static long longByCellByteSize(Map<Cell, Long> timestampByCell) {
        return timestampByCell.keySet().stream()
                .mapToLong(cell -> byteSize(cell) + LONG_BYTES)
                .sum();
    }

    public static long longByCellByteSize(Multimap<Cell, Long> valueByCell) {
        return valueByCell.keys().stream()
                .mapToLong(ExpectationsUtils::byteSize)
                .sum();
    }

    public static long valueByCellByteSize(Map<Cell, Value> valueByCell) {
        return valueByCell.entrySet().stream()
                .mapToLong(ExpectationsUtils::valueByCellEntryByteSize)
                .sum();
    }

    public static long pageByRangeRequestByteSize(
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRange) {
        return pageByRange.values().stream()
                .map(TokenBackedBasicResultsPage::getResults)
                .flatMap(Collection::stream)
                .mapToLong(ExpectationsUtils::byteSize)
                .sum();
    }

    public static long byteSize(Cell cell) {
        return ((long) cell.getRowName().length) + cell.getColumnName().length;
    }

    public static long byteSize(Value value) {
        return value.getContents().length;
    }

    public static long byteSize(CandidateCellForSweeping candidate) {
        return byteSize(candidate.cell())
                + LONG_BYTES * candidate.sortedTimestamps().size();
    }

    public static long byteSize(List<byte[]> array) {
        return array.stream().mapToLong(Array::getLength).sum();
    }

    public static long byteSize(RowResult<Value> rowResult) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(ExpectationsUtils::valueByByteArrayByteSize)
                        .sum();
    }

    public static long valueByCellEntryByteSize(Entry<Cell, Value> entry) {
        return byteSize(entry.getKey()) + byteSize(entry.getValue());
    }

    public static long valueByByteArrayByteSize(Entry<byte[], Value> valueByArray) {
        return byteSize(valueByArray.getValue()) + valueByArray.getKey().length;
    }
}
