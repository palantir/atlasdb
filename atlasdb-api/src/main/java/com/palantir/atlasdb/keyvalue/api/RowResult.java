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
package com.palantir.atlasdb.keyvalue.api;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.collect.Maps2;
import com.palantir.logsafe.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

public final class RowResult<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] row;
    private final ImmutableSortedMap<byte[], T> columns;

    public static <T> RowResult<T> of(Cell cell, T value) {
        return new RowResult<T>(cell.getRowName(),
            ImmutableSortedMap.<byte[], T>orderedBy(UnsignedBytes.lexicographicalComparator())
                .put(cell.getColumnName(), value).build());
    }

    public static <T> RowResult<T> create(byte[] row, SortedMap<byte[], T> columns) {
        return new RowResult<T>(row, columns);
    }

    private RowResult(byte[] row, SortedMap<byte[], T> columns) {
        Preconditions.checkArgument(Cell.isNameValid(row));
        Preconditions.checkArgument(UnsignedBytes.lexicographicalComparator().equals(columns.comparator()),
                "comparator for the map must be the bytes comparator");
        for (byte[] colName : columns.keySet()) {
            Preconditions.checkArgument(Cell.isNameValid(colName));
        }
        this.row = row.clone();
        this.columns = ImmutableSortedMap.copyOf(columns, UnsignedBytes.lexicographicalComparator());
    }

    public byte[] getRowName() {
        return row.clone();
    }

    public NavigableMap<byte[], T> getColumns() {
        return columns;
    }

    public Set<Cell> getCellSet() {
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(columns.size());
        for (byte[] column : columns.keySet()) {
            cells.add(Cell.create(row, column));
        }
        return cells;
    }

    public static <T> Ordering<RowResult<T>> getOrderingByRowName() {
        return Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(RowResult.<T>getRowNameFun());
    }

    public static <T> Ordering<RowResult<T>> getOrderingByRowName(boolean reverse) {
        Ordering<RowResult<T>> ordering = getOrderingByRowName();
        if (reverse) {
            return ordering.reverse();
        }
        return ordering;
    }

    public static <T> Function<RowResult<T>, byte[]> getRowNameFun() {
        return input -> input.getRowName();
    }

    public T getOnlyColumnValue() {
        Preconditions.checkState(columns.size() == 1,
                "Works only when the row result has a single column value.");
        return Iterables.getOnlyElement(columns.values());
    }

    public Iterable<Map.Entry<Cell, T>> getCells() {
        return Collections2.transform(columns.entrySet(),
                from -> Maps.immutableEntry(Cell.create(row, from.getKey()), from.getValue()));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("row", PtBytes.encodeHexString(row))
                .add("columns", Maps2.transformKeys(columns, PtBytes.BYTES_TO_HEX_STRING))
                .toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + Arrays.hashCode(row);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        RowResult other = (RowResult) obj;
        if (columns == null) {
            if (other.columns != null) {
                return false;
            }
        } else if (!columns.equals(other.columns)) {
            return false;
        }
        if (!Arrays.equals(row, other.row)) {
            return false;
        }
        return true;
    }


}
