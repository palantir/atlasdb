/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.api;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.UnsignedBytes;

public class ColumnSelection implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<byte[]> selectedColumns;
    private static final ColumnSelection allColumnsSelected = new ColumnSelection(null);

    private ColumnSelection(Set<byte[]> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    // Factory methods.
    public static ColumnSelection all() {
        return allColumnsSelected;
    }

    public static ColumnSelection create(Iterable<byte[]> selectedColumns) {
        assert selectedColumns != null;

        // Copy contents of 'selectedColumns' into a new set with proper deep comparison semantics.
        return new ColumnSelection(ImmutableSortedSet.copyOf(UnsignedBytes.lexicographicalComparator(), selectedColumns));
    }

    public boolean contains(byte[] column) {
        if (selectedColumns == null) {
            return true;
        }
        return selectedColumns.contains(column);
    }

    // allColumnsSelected() returns true if all columns are selected.  getSelectedColumns() should
    // only be called if allColumsSelected() returns false.
    public boolean allColumnsSelected() {
        return selectedColumns == null;
    }

    public Iterable<byte[]> getSelectedColumns() {
        assert selectedColumns != null;
        return Collections.unmodifiableCollection(selectedColumns);
    }

    /**
     * Returns true if no columns are selected.
     */
    public boolean noColumnsSelected() {
        return selectedColumns != null && selectedColumns.isEmpty();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((selectedColumns == null) ? 0 : selectedColumns.hashCode());
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
