/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import org.immutables.value.Value;

import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.AtlasLockDescriptorRanges;
import com.palantir.lock.LockDescriptor;

public final class TableElements {
    private TableElements() {
        // no
    }

    public static TableElement entireTable(TableReference tableRef) {
        return ImmutableEntireTable.of(tableRef);
    }

    public static TableElement rowPrefix(TableReference tableRef, byte[] rowPrefix) {
        return ImmutableRowPrefix.of(tableRef, rowPrefix);
    }

    public static TableElement rowRange(TableReference tableRef, byte[] startInclusive, byte[] endExclusive) {
        return ImmutableRowRange.of(tableRef, startInclusive, endExclusive);
    }

    public static TableElement exactRow(TableReference tableRef, byte[] row) {
        return ImmutableExactRow.of(tableRef, row);
    }

    public static TableElement exactCell(TableReference tableRef, Cell cell) {
        return ImmutableExactCell.of(tableRef, cell);
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    abstract static class EntireTable implements TableElement {
        @Value.Parameter
        abstract TableReference tableRef();

        @Override
        public Range<LockDescriptor> getAsRange() {
            return AtlasLockDescriptorRanges.fullTable(tableRef().getQualifiedName());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    abstract static class RowPrefix implements TableElement {
        @Value.Parameter
        abstract TableReference tableRef();
        @Value.Parameter
        abstract byte[] row();

        @Override
        public Range<LockDescriptor> getAsRange() {
            return AtlasLockDescriptorRanges.rowPrefix(tableRef().getQualifiedName(), row());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    abstract static class RowRange implements TableElement {
        @Value.Parameter
        abstract TableReference tableRef();
        @Value.Parameter
        abstract byte[] startInclusive();
        @Value.Parameter
        abstract byte[] endExclusive();

        @Override
        public Range<LockDescriptor> getAsRange() {
            return AtlasLockDescriptorRanges.rowRange(tableRef().getQualifiedName(), startInclusive(), endExclusive());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    abstract static class ExactRow implements TableElement {
        @Value.Parameter
        abstract TableReference tableRef();
        @Value.Parameter
        abstract byte[] row();

        @Override
        public Range<LockDescriptor> getAsRange() {
            return AtlasLockDescriptorRanges.exactRow(tableRef().getQualifiedName(), row());
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    abstract static class ExactCell implements TableElement {
        @Value.Parameter
        abstract TableReference tableRef();
        @Value.Parameter
        abstract Cell cell();

        @Override
        public Range<LockDescriptor> getAsRange() {
            return AtlasLockDescriptorRanges
                    .exactCell(tableRef().getQualifiedName(), cell().getRowName(), cell().getColumnName());
        }
    }
}
