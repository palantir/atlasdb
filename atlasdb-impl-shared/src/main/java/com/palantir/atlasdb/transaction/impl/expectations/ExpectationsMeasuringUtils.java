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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

public final class ExpectationsMeasuringUtils {
    private ExpectationsMeasuringUtils() {}

    public static long sizeInBytes(Entry<Cell, Value> entry) {
        return entry.getKey().sizeInBytes() + entry.getValue().sizeInBytes();
    }

    public static long sizeInBytes(Multimap<Cell, Long> valueByCell) {
        return valueByCell.keys().stream()
                .mapToLong(cell -> cell.sizeInBytes() + Long.BYTES)
                .sum();
    }

    public static long sizeInBytes(Map<Cell, Long> longByCell) {
        return longByCell.keySet().stream()
                .mapToLong(cell -> cell.sizeInBytes() + Long.BYTES)
                .sum();
    }

    public static long sizeInBytes(RowResult<Value> rowResult) {
        return sizeInBytes(rowResult, Value::sizeInBytes);
    }

    private static <T> long sizeInBytes(RowResult<T> rowResult, Function<T, Long> measurer) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + measurer.apply(entry.getValue()))
                        .sum();
    }

    public static long arrayByRefSizeInBytes(Map<TableReference, byte[]> arrayByRef) {
        return arrayByRef.entrySet().stream()
                .mapToLong(entry -> entry.getKey().sizeInBytes() + entry.getValue().length)
                .sum();
    }

    public static long valueByCellSizeInBytes(Map<Cell, Value> valueByCell) {
        return valueByCell.entrySet().stream()
                .mapToLong(ExpectationsMeasuringUtils::sizeInBytes)
                .sum();
    }

    /**
     * Ignoring the size of token for next page because the interface method
     * {@link TokenBackedBasicResultsPage#getTokenForNextPage} might have side effects (e.g. interact with the kvs).
     * The interface specification is ambiguous. Also, the current token is not exposed.
     * todo(aalouane): SG search
     */
    public static long pageByRequestSizeInBytes(
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRange) {
        return pageByRange.values().stream()
                .map(TokenBackedBasicResultsPage::getResults)
                .flatMap(Collection::stream)
                .mapToLong(ExpectationsMeasuringUtils::sizeInBytes)
                .sum();
    }

    public static long setResultSizeInBytes(RowResult<Set<Long>> rowResult) {
        return sizeInBytes(rowResult, set -> (long) set.size() * Long.BYTES);
    }
}
