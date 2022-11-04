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

package com.palantir.atlasdb.util;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.ToLongFunction;

public final class MeasuringUtils {
    private MeasuringUtils() {}

    public static long sizeOfMeasurableLongMap(Map<? extends Measurable, Long> map) {
        return sizeOfMap(map, Measurable::sizeInBytes, _unused -> Long.BYTES);
    }

    public static long sizeOfMeasurableByteMap(Map<? extends Measurable, byte[]> map) {
        return sizeOfMap(map, Measurable::sizeInBytes, Array::getLength);
    }

    public static long sizeOfByteCollection(Collection<byte[]> byteArrays) {
        return sizeOfCollection(byteArrays, Array::getLength);
    }

    public static long sizeOfLongSetRowResult(RowResult<Set<Long>> rowResult) {
        return sizeOfRowResult(rowResult, set -> (long) set.size() * Long.BYTES);
    }

    /**
     * Ignoring the size of token for next page because the interface method
     * {@link TokenBackedBasicResultsPage#getTokenForNextPage} might have side effects (e.g. interact with the kvs).
     * The interface specification is ambiguous.
     */
    public static long sizeOfPageByRangeRequestMap(
            Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> pageByRange) {
        return sizeOfMap(pageByRange, _unused -> 0L, page -> page.getResults().stream()
                .mapToLong(MeasuringUtils::sizeOf)
                .sum());
    }

    public static long sizeOf(Multimap<? extends Measurable, Long> map) {
        return sizeOf(map.keys()) + Long.BYTES * ((long) map.size());
    }

    public static long sizeOf(Entry<? extends Measurable, ? extends Measurable> entry) {
        return entry.getKey().sizeInBytes() + entry.getValue().sizeInBytes();
    }

    public static long sizeOf(Collection<? extends Measurable> collection) {
        return sizeOfCollection(collection, Measurable::sizeInBytes);
    }

    public static long sizeOf(Map<? extends Measurable, ? extends Measurable> map) {
        return sizeOfMap(map, Measurable::sizeInBytes, Measurable::sizeInBytes);
    }

    public static long sizeOf(RowResult<? extends Measurable> rowResult) {
        return sizeOfRowResult(rowResult, Measurable::sizeInBytes);
    }

    private static <T> long sizeOfCollection(Collection<T> collection, ToLongFunction<T> measurer) {
        return collection.stream().mapToLong(measurer).sum();
    }

    private static <K, V> long sizeOfMap(
            Map<K, V> map, ToLongFunction<K> keyMeasurer, ToLongFunction<V> valueMeasurer) {
        return map.entrySet().stream()
                .mapToLong(
                        entry -> keyMeasurer.applyAsLong(entry.getKey()) + valueMeasurer.applyAsLong(entry.getValue()))
                .sum();
    }

    private static <T> long sizeOfRowResult(RowResult<T> rowResult, ToLongFunction<T> measurer) {
        return rowResult.getRowNameSize()
                + rowResult.getColumns().entrySet().stream()
                        .mapToLong(entry -> entry.getKey().length + measurer.applyAsLong(entry.getValue()))
                        .sum();
    }
}
