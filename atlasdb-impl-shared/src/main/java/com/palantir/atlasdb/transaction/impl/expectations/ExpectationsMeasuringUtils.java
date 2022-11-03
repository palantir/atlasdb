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
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.ToLongFunction;

public final class ExpectationsMeasuringUtils {
    private ExpectationsMeasuringUtils() {}

    public static long toLongSizeInBytes(Map<? extends Measurable, Long> map) {
        return sizeInBytes(map, unused -> Long.BYTES);
    }

    public static long toArraySizeInBytes(Map<? extends Measurable, byte[]> map) {
        return sizeInBytes(map, Array::getLength);
    }

    public static long byteArraysSizeInBytes(Collection<byte[]> byteArrays) {
        return sizeInBytes(byteArrays, Array::getLength);
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

    public static long sizeInBytes(Multimap<? extends Measurable, Long> map) {
        return sizeInBytes(map.keys()) + Long.BYTES * ((long) map.size());
    }

    public static long sizeInBytes(Entry<? extends Measurable, ? extends Measurable> entry) {
        return entry.getKey().sizeInBytes() + entry.getValue().sizeInBytes();
    }

    public static long sizeInBytes(Collection<? extends Measurable> collection) {
        return sizeInBytes(collection, Measurable::sizeInBytes);
    }

    public static long sizeInBytes(Map<? extends Measurable, ? extends Measurable> map) {
        return sizeInBytes(map, Measurable::sizeInBytes);
    }

    public static long sizeInBytes(RowResult<? extends Measurable> rowResult) {
        return sizeInBytes(rowResult, Measurable::sizeInBytes);
    }

    private static <T> long sizeInBytes(Collection<T> collection, ToLongFunction<T> measurer) {
        return collection.stream().mapToLong(measurer).sum();
    }

    private static <T> long sizeInBytes(Map<? extends Measurable, T> byMeasurable, ToLongFunction<T> valueMeasurer) {
        return byMeasurable.entrySet().stream()
                .mapToLong(entry -> entry.getKey().sizeInBytes() + valueMeasurer.applyAsLong(entry.getValue()))
                .sum();
    }

    private static <T> long sizeInBytes(RowResult<T> rowResult, ToLongFunction<T> measurer) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + measurer.applyAsLong(entry.getValue()))
                        .sum();
    }
}
