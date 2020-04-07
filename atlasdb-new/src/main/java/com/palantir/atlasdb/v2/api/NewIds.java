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

import static com.palantir.logsafe.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.atlasdb.encoding.PtBytes;

public final class NewIds {
    private NewIds() {}

    public static Table table(String name) {
        return ImmutableTable.of(name);
    }

    public static Row row(byte[] row) {
        return new Row(row);
    }

    public static Column column(byte[] column) {
        return new Column(column);
    }

    public static StoredValue value(byte[] value) {
        return new StoredValue(value);
    }

    public static Cell cell(Row row, Column column) {
        return ImmutableCell.of(row, column);
    }

    public static StoredCell storedCell(Cell cell, Optional<StoredValue> value) {
        return ImmutableStoredCell.of(cell, value);
    }

    @Value.Immutable
    public static abstract class Table {
        @Value.Parameter
        abstract String getName();
    }

    @Value.Immutable
    public static abstract class StoredCell {
        StoredCell() {}

        @Value.Parameter
        public abstract Cell cell();

        @Value.Parameter
        public abstract Optional<StoredValue> value();
    }

    @Value.Immutable
    public static abstract class Cell implements Comparable<Cell> {
        Cell() {}

        @Value.Parameter
        abstract Row row();

        @Value.Parameter
        abstract Column column();

        @Override
        public final int compareTo(Cell other) {
            int rowComparison = row().compareTo(other.row());
            if (rowComparison != 0) {
                return rowComparison;
            }
            return column().compareTo(other.column());
        }
    }

    public static final class TransactionWrite extends BytesWrapper<TransactionWrite> {
        private TransactionWrite(byte[] bytes) {
            super(bytes);
        }
    }

    public static final class StoredValue extends BytesWrapper<StoredValue> {
       private StoredValue(byte[] bytes) {
           super(bytes);
       }
    }

    public static final class Row extends BytesWrapper<Row> {
        private Row(byte[] bytes) {
            super(bytes);
        }
    }

    public static final class Column extends BytesWrapper<Column> {
        private Column(byte[] bytes) {
            super(bytes);
        }
    }

    private static abstract class BytesWrapper<T extends BytesWrapper<T>> implements Comparable<T> {
        private final byte[] bytes;

        private BytesWrapper(byte[] bytes) {
            this.bytes = checkNotNull(bytes).clone();
        }

        @Override
        public final boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            BytesWrapper<?> that = (BytesWrapper<?>) other;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public final int compareTo(T other) {
            BytesWrapper<T> id = other;
            return PtBytes.BYTES_COMPARATOR.compare(bytes, id.bytes);
        }
    }
}
