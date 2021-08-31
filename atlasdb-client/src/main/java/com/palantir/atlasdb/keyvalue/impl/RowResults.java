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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.collect.IterableView;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

public final class RowResults {
    private RowResults() {
        /* */
    }

    public static <T> IterableView<RowResult<T>> viewOfMap(Map<byte[], NavigableMap<byte[], T>> map) {
        return viewOfEntries(map.entrySet());
    }

    public static <T> IterableView<RowResult<T>> viewOfEntries(
            Iterable<Map.Entry<byte[], NavigableMap<byte[], T>>> mapEntries) {
        return IterableView.of(mapEntries).transform(RowResults::createRowResult);
    }

    public static <T> Iterator<RowResult<T>> viewOfEntries(
            Iterator<Map.Entry<byte[], NavigableMap<byte[], T>>> mapEntries) {
        return Iterators.transform(mapEntries, RowResults::createRowResult);
    }

    private static <T> RowResult<T> createRowResult(Map.Entry<byte[], NavigableMap<byte[], T>> entry) {
        return RowResult.create(entry.getKey(), entry.getValue());
    }

    public static <T> IterableView<Map.Entry<byte[], SortedMap<byte[], T>>> entriesViewFromRows(
            Iterable<RowResult<T>> rows) {
        return IterableView.of(rows).transform(row -> Maps.immutableEntry(row.getRowName(), row.getColumns()));
    }

    public static <T> NavigableMap<byte[], RowResult<T>> viewOfSortedMap(
            NavigableMap<byte[], NavigableMap<byte[], T>> map) {
        return Maps.transformEntries(map, RowResult::create);
    }

    public static <T> Predicate<RowResult<T>> createIsEmptyPredicate() {
        return input -> input.getColumns().isEmpty();
    }

    public static <T> Function<RowResult<T>, RowResult<T>> createFilterColumns(final Predicate<byte[]> keepColumn) {
        return row -> RowResult.create(row.getRowName(), Maps.filterKeys(row.getColumns(), keepColumn));
    }

    public static <T, U> Function<RowResult<T>, RowResult<U>> transformValues(final Function<T, U> transform) {
        return row -> RowResult.create(row.getRowName(), Maps.transformValues(row.getColumns(), transform));
    }

    public static <T> RowResult<T> merge(RowResult<T> base, RowResult<T> overwrite) {
        Preconditions.checkArgument(Arrays.equals(base.getRowName(), overwrite.getRowName()));
        ImmutableSortedMap.Builder<byte[], T> colBuilder =
                ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
        colBuilder.putAll(overwrite.getColumns());
        colBuilder.putAll(
                Maps.difference(base.getColumns(), overwrite.getColumns()).entriesOnlyOnLeft());
        return RowResult.create(base.getRowName(), colBuilder.build());
    }

    public static long getApproximateSizeOfRowResult(RowResult<byte[]> rr) {
        long size = rr.getRowName().length;

        for (Map.Entry<Cell, byte[]> entry : rr.getCells()) {
            size += Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().length;
        }
        return size;
    }
}
