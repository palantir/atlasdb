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
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.util.Measurable;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

public final class ExpectationsMeasuringUtils {
    private ExpectationsMeasuringUtils() {}

    public static long sizeInBytes(Multimap<? extends Measurable, Long> map) {
        return map.keys().stream()
                .mapToLong(measurable -> measurable.sizeInBytes() + Long.BYTES)
                .sum();
    }

    public static long sizeInBytes(Entry<? extends Measurable, ? extends Measurable> entry) {
        return entry.getKey().sizeInBytes() + entry.getValue().sizeInBytes();
    }

    public static long sizeInBytes(Map<? extends Measurable, ? extends Measurable> map) {
        return map.entrySet().stream()
                .mapToLong(ExpectationsMeasuringUtils::sizeInBytes)
                .sum();
    }

    private static <T> long sizeInBytes(RowResult<T> rowResult, Function<T, Long> measurer) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + measurer.apply(entry.getValue()))
                        .sum();
    }

    private static <T> long sizeInBytes(Map<? extends Measurable, T> byMeasurable, Function<T, Long> valueMeasurer) {
        return byMeasurable.entrySet().stream()
                .mapToLong(entry -> entry.getKey().sizeInBytes() + valueMeasurer.apply(entry.getValue()))
                .sum();
    }

    public static long sizeInBytes(RowResult<? extends Measurable> rowResult) {
        return sizeInBytes(rowResult, Measurable::sizeInBytes);
    }

    public static long toLongSizeInBytes(Map<? extends Measurable, Long> map) {
        return sizeInBytes(map, unused -> Long.valueOf(Long.BYTES));
    }

    public static long toArraySizeInBytes(Map<? extends Measurable, byte[]> map) {
        return sizeInBytes(map, array -> Long.valueOf(array.length));
    }

    public static long setResultSizeInBytes(RowResult<Set<Long>> rowResult) {
        return sizeInBytes(rowResult, set -> (long) set.size() * Long.BYTES);
    }

    /**
     * Ignoring the size of token for next page because the interface method
     * {@link TokenBackedBasicResultsPage#getTokenForNextPage} might have side effects (e.g. interact with the kvs).
     * The interface specification is ambiguous.
     */
    public static long pageByRequestSizeInBytes(
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRange) {
        return pageByRange.values().stream()
                .map(TokenBackedBasicResultsPage::getResults)
                .flatMap(Collection::stream)
                .mapToLong(ExpectationsMeasuringUtils::sizeInBytes)
                .sum();
    }
}
