/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api;

import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.palantir.atlasdb.v2.api.NewIds.Column;
import com.palantir.atlasdb.v2.api.NewIds.Row;

@Value.Immutable
public abstract class ScanFilter {

    @Value.Parameter
    public abstract RowsFilter rows();

    @Value.Parameter
    public abstract ColumnsFilter columns();

    public static ScanFilter of(RowsFilter rows, ColumnsFilter columns) {
        return ImmutableScanFilter.of(rows, columns);
    }

    public static RowsFilter allRows() {
        return AllRowsFilter.INSTANCE;
    }

    public static RowsFilter exactRows(Set<Row> rows) {
        return ImmutableExactRows.of(rows);
    }

    public static RowsFilter rowRange(Optional<Row> from, Optional<Row> to) {
        return ImmutableRowRange.of(from, to);
    }

    public static ColumnsFilter allColumns() {
        return AllColumnsFilter.INSTANCE;
    }

    public static ColumnsFilter exactColumns(Set<Column> columns) {
        return ImmutableExactColumns.of(columns);
    }

    public static ColumnsFilter columnRange(Optional<Column> from, Optional<Column> to) {
        return ImmutableColumnRange.of(from, to);
    }

    public interface RowsFilter {
        <T> T accept(Visitor<T> visitor);

        interface Visitor<T> {
            T visitAllRows();
            T visitExactRows(Set<Row> rows);
            T visitRowRange(Optional<Row> fromInclusive, Optional<Row> toExclusive);
        }
    }

    public interface ColumnsFilter {
        <T> T accept(Visitor<T> visitor);

        interface Visitor<T> {
            T visitAllColumns();
            T visitExactColumns(Set<Column> columns);
            T visitColumnRange(Optional<Column> fromInclusive, Optional<Column> toExclusive);
        }
    }

    private enum AllRowsFilter implements RowsFilter {
        INSTANCE;

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visitAllRows();
        }
    }

    private enum AllColumnsFilter implements ColumnsFilter {
        INSTANCE;

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visitAllColumns();
        }
    }

    @Value.Immutable
    interface ExactRows extends RowsFilter {
        @Value.Parameter
        Set<Row> rows();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitExactRows(rows());
        }
    }

    @Value.Immutable
    interface ExactColumns extends ColumnsFilter {
        @Value.Parameter
        Set<Column> columns();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitExactColumns(columns());
        }
    }

    @Value.Immutable
    interface RowRange extends RowsFilter {
        @Value.Parameter
        Optional<Row> fromInclusive();

        @Value.Parameter
        Optional<Row> toExclusive();

        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitRowRange(fromInclusive(), toExclusive());
        }
    }

    @Value.Immutable
    interface ColumnRange extends ColumnsFilter {
        @Value.Parameter
        Optional<Column> fromInclusive();

        @Value.Parameter
        Optional<Column> toExclusive();

        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitColumnRange(fromInclusive(), toExclusive());
        }
    }
}
