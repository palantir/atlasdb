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

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.Preconditions;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class ColumnSelection implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final ColumnSelection allColumnsSelected = new ColumnSelection(null);

    private final SortedSet<byte[]> selectedColumns;
    private transient int hashCode = 0;

    private ColumnSelection(SortedSet<byte[]> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    @SuppressWarnings({"BadAssert", "StringSplitter"}) // this code is performance sensitive!
    public static ColumnSelection valueOf(String serialized) {
        Set<byte[]> columns = new TreeSet<>(UnsignedBytes.lexicographicalComparator());
        for (String columnString : serialized.split("\\s*,\\s*")) {
            String trimmedColumnString = columnString.trim();
            if (trimmedColumnString.isEmpty()) {
                continue;
            }
            byte[] column = PtBytes.decodeBase64(trimmedColumnString);
            assert !columns.contains(column);
            columns.add(column);
        }
        if (columns.isEmpty()) {
            return all();
        }
        return ColumnSelection.create(columns);
    }

    @Override
    public String toString() {
        if (selectedColumns == null) {
            return "";
        }
        return Joiner.on(',').join(Collections2.transform(selectedColumns, PtBytes::encodeBase64String));
    }

    // Factory methods.
    public static ColumnSelection all() {
        return allColumnsSelected;
    }

    public static ColumnSelection create(Iterable<byte[]> selectedColumns) {
        if (Iterables.isEmpty(Preconditions.checkNotNull(selectedColumns, "selectedColumns cannot be null"))) {
            return allColumnsSelected;
        }

        // Copy contents of 'selectedColumns' into a new set with proper deep comparison semantics.
        return new ColumnSelection(
                ImmutableSortedSet.copyOf(UnsignedBytes.lexicographicalComparator(), selectedColumns));
    }

    public boolean contains(byte[] column) {
        return selectedColumns == null || selectedColumns.contains(column);
    }

    // allColumnsSelected() returns true if all columns are selected.  getSelectedColumns() should
    // only be called if allColumsSelected() returns false.
    public boolean allColumnsSelected() {
        return selectedColumns == null;
    }

    @SuppressWarnings("BadAssert") // performance sensitive asserts
    public Collection<byte[]> getSelectedColumns() {
        assert selectedColumns != null;
        return Collections.unmodifiableCollection(selectedColumns);
    }

    public Set<Cell> asCellsForRows(Iterable<byte[]> rows) {
        Preconditions.checkState(!allColumnsSelected(), "Cannot create cells if columns are not explicitly set.");
        return Streams.stream(rows)
                .flatMap(row -> getSelectedColumns().stream().map(col -> Cell.create(row, col)))
                .collect(Collectors.toSet());
    }

    /**
     * Returns true if no columns are selected.
     */
    public boolean noColumnsSelected() {
        return selectedColumns != null && selectedColumns.isEmpty();
    }

    @Override
    public int hashCode() {
        /*
         * Lazily compute and store hashcode since instances are frequently
         * accessed via hash collections, but computation can be expensive, and
         * allow for benign data races.
         */
        if (hashCode == 0) {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((selectedColumns == null) ? 0 : selectedColumns.hashCode());
            hashCode = result;
        }
        return hashCode;
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
        ColumnSelection other = (ColumnSelection) obj;
        if (selectedColumns == null) {
            if (other.selectedColumns != null) {
                return false;
            }
        } else if (!selectedColumns.equals(other.selectedColumns)) {
            return false;
        }
        return true;
    }
}
