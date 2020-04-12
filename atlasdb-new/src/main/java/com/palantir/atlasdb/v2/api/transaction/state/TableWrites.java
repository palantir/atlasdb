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

package com.palantir.atlasdb.v2.api.transaction.state;

import static com.palantir.logsafe.Preconditions.checkNotNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.atlasdb.v2.api.api.ScanFilter.RowsFilter;

import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;

// This class presently uses Vavr for storing data.
// Unfortunately, Vavr's sorted maps are not navigable, and so the implementation is very, very inefficient.
// Should probably switch to using SnapTree.
public final class TableWrites {
    private final Table table;
    private final SortedMap<Cell, TransactionValue> writes;

    private TableWrites(Table table,
            SortedMap<Cell, TransactionValue> writes) {
        this.table = table;
        this.writes = writes;
    }

    public Table table() {
        return table;
    }

    public Map<Cell, TransactionValue> data() {
        return writes;
    }

    public boolean containsCell(Cell cell) {
        return writes.containsKey(cell);
    }

    public ScanDefinition toConflictCheckingScan() {
        return ScanDefinition.of(table(), ScanFilter.cells(data().keySet()), new ScanAttributes());
    }

    boolean isEmpty() {
        return writes.isEmpty();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public Iterator<TransactionValue> scan(ScanAttributes attributes, ScanFilter filter) {
        Comparator<Cell> cellComparator = filter.toComparator(attributes);
        Comparator<NewValue> valueComparator = Comparator.comparing(NewValue::cell, cellComparator);
        Seq<TransactionValue> asSeq = filter.accept(new ScanFilter.Visitor<Seq<TransactionValue>>() {
            @Override
            public Seq<TransactionValue> rowsAndColumns(RowsFilter rows,
                    ScanFilter.ColumnsFilter columns, int limit) {
                return rows.accept(new RowsFilter.Visitor<Seq<TransactionValue>>() {
                    private Seq<TransactionValue> applyColumnFilter(Seq<TransactionValue> filteredByRows) {
                        return filterBy(filteredByRows, columns).sorted(valueComparator).take(limit);
                    }

                    @Override
                    public Seq<TransactionValue> visitAllRows() {
                        return applyColumnFilter(writes.values());
                    }

                    @Override
                    public Seq<TransactionValue> visitExactRows(Set<NewIds.Row> rows) {
                        return applyColumnFilter(writes.filterKeys(cell -> rows.contains(cell.row())).values());
                    }

                    @Override
                    public Seq<TransactionValue> visitRowRange(Optional<NewIds.Row> fromInclusive,
                            Optional<NewIds.Row> toExclusive) {
                        Seq<TransactionValue> filtered = writes.values();
                        if (fromInclusive.isPresent()) {
                            filtered = filtered.filter(value -> value.cell().row().compareTo(fromInclusive.get()) >= 0);
                        }
                        if (toExclusive.isPresent()) {
                            filtered = filtered.filter(value -> value.cell().row().compareTo(toExclusive.get()) < 0);
                        }
                        return filtered;
                    }
                });
            }

            @Override
            public Seq<TransactionValue> cells(Set<Cell> cells) {
                return writes.filterKeys(cells::contains).values().sorted(valueComparator);
            }

            @Override
            public Seq<TransactionValue> withStoppingPoint(ScanFilter inner, Cell lastCellInclusive) {
                return inner.accept(this).takeWhile(
                        element -> cellComparator.compare(element.cell(), lastCellInclusive) <= 0);
            }
        });
        return asSeq.iterator();
    }

    private Seq<TransactionValue> filterBy(Seq<TransactionValue> rows, ScanFilter.ColumnsFilter columns) {
        return columns.accept(new ScanFilter.ColumnsFilter.Visitor<Seq<TransactionValue>>() {
            @Override
            public Seq<TransactionValue> visitAllColumns() {
                return rows;
            }

            @Override
            public Seq<TransactionValue> visitExactColumns(Set<NewIds.Column> columns) {
                return rows.filter(value -> columns.contains(value.cell().column()));
            }

            @Override
            public Seq<TransactionValue> visitColumnRange(
                    Optional<NewIds.Column> fromInclusive, Optional<NewIds.Column> toExclusive) {
                Seq<TransactionValue> filtered = rows;
                if (fromInclusive.isPresent()) {
                    filtered = filtered.filter(value -> value.cell().column().compareTo(fromInclusive.get()) >= 0);
                }
                if (toExclusive.isPresent()) {
                    filtered = filtered.filter(value -> value.cell().column().compareTo(toExclusive.get()) < 0);
                }
                return filtered;
            }
        });
    }

    public static final class Builder {
        private Table table;
        private SortedMap<Cell, TransactionValue> writes;

        public Builder() {
            writes = TreeMap.empty();
            table = null;
        }

        public Builder(TableWrites tableWrites) {
            this.writes = tableWrites.writes;
            this.table = tableWrites.table;
        }

        public Builder table(Table table) {
            this.table = table;
            return this;
        }

        public Builder put(TransactionValue value) {
            writes = writes.put(value.cell(), value);
            return this;
        }

        public Builder clear() {
            writes = TreeMap.empty();
            return this;
        }

        public Builder mergeFrom(TableWrites other) {
            if (writes.isEmpty()) {
                writes = other.writes;
                return this;
            }
            for (TransactionValue value : other.writes.values()) {
                writes = writes.put(value.cell(), value);
            }
            return this;
        }

        public TableWrites build() {
            return new TableWrites(checkNotNull(table), writes);
        }

        public TableWrites buildPartial() {
            return build();
        }
    }
}
