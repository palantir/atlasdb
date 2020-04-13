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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Column;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.atlasdb.v2.api.api.ScanFilter.RowsFilter;

import edu.stanford.ppl.concurrent.SnapTreeMap;

public final class TableWrites {
    private static final SnapTreeMap<Column, TransactionValue> EMPTY = new SnapTreeMap<>();
    private static final boolean INCLUSIVE = true;
    private static final boolean EXCLUSIVE = false;
    private final Table table;
    private final SnapTreeMap<Row, SnapTreeMap<Column, TransactionValue>> writes;

    private TableWrites(Table table, SnapTreeMap<Row, SnapTreeMap<Column, TransactionValue>> writes) {
        this.table = table;
        this.writes = writes;
    }

    public Table table() {
        return table;
    }

    public Stream<TransactionValue> stream() {
        return writes.values().stream().map(Map::values).flatMap(Collection::stream);
    }

    private Optional<TransactionValue> getCell(Cell cell) {
        return Optional.ofNullable(writes.getOrDefault(cell.row(), EMPTY).get(cell.column()));
    }

    public boolean containsCell(Cell cell) {
        return writes.getOrDefault(cell.row(), EMPTY).containsKey(cell.column());
    }

    public ScanDefinition toConflictCheckingScan() {
        return ScanDefinition.of(table(),
                ScanFilter.cells(stream().map(TransactionValue::cell).collect(ImmutableSet.toImmutableSet())));
    }

    boolean isEmpty() {
        return writes.isEmpty();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public Iterator<TransactionValue> scan(ScanFilter filter) {
        Comparator<Cell> cellComparator = filter.toCellComparator();
        return filter.accept(new ScanFilter.Visitor<Iterator<TransactionValue>>() {
            @Override
            public Iterator<TransactionValue> rowsAndColumns(
                    RowsFilter rows, ScanFilter.ColumnsFilter columns, int limit) {
                return Iterators.limit(
                        Iterators.concat(
                                Iterators.transform(filterRows(rows), row -> filterColumns(row, columns))),
                        limit);
            }

            @Override
            public Iterator<TransactionValue> cells(Set<Cell> cells) {
                ImmutableSortedSet<Cell> sortedCells = ImmutableSortedSet.copyOf(cellComparator, cells);
                return sortedCells.stream()
                        .flatMap(cell -> Streams.stream(getCell(cell)))
                        .iterator();
            }

            @Override
            public Iterator<TransactionValue> withStoppingPoint(ScanFilter inner, Cell lastCellInclusive) {
                Iterator<TransactionValue> innerIterator = inner.accept(this);
                return new AbstractIterator<TransactionValue>() {
                    @Override
                    protected TransactionValue computeNext() {
                        if (!innerIterator.hasNext()) {
                            return endOfData();
                        }
                        TransactionValue next = innerIterator.next();
                        if (cellComparator.compare(next.cell(), lastCellInclusive) > 0) {
                            return endOfData();
                        }
                        return next;
                    }
                };
            }
        });
    }

    private Iterator<SnapTreeMap<Column, TransactionValue>> filterRows(ScanFilter.RowsFilter filter) {
        return filter.accept(new RowsFilter.Visitor<Iterator<SnapTreeMap<Column, TransactionValue>>>() {
            @Override
            public Iterator<SnapTreeMap<Column, TransactionValue>> visitAllRows() {
                return writes.values().iterator();
            }

            @Override
            public Iterator<SnapTreeMap<Column, TransactionValue>> visitExactRows(ImmutableSortedSet<Row> rows) {
                return rows.stream().map(writes::get).filter(Objects::nonNull).iterator();
            }

            @Override
            public Iterator<SnapTreeMap<Column, TransactionValue>> visitRowRange(
                    Optional<Row> fromInclusive, Optional<Row> toExclusive) {
                NavigableMap<Row, SnapTreeMap<Column, TransactionValue>> filtered = writes;
                if (fromInclusive.isPresent()) {
                    filtered = filtered.tailMap(fromInclusive.get(), INCLUSIVE);
                }
                if (toExclusive.isPresent()) {
                    filtered = filtered.headMap(toExclusive.get(), EXCLUSIVE);
                }
                return filtered.values().iterator();
            }
        });
    }

    private static Iterator<TransactionValue> filterColumns(
            SnapTreeMap<Column, TransactionValue> row, ScanFilter.ColumnsFilter filter) {
        return filter.accept(new ScanFilter.ColumnsFilter.Visitor<Iterator<TransactionValue>>() {
            @Override
            public Iterator<TransactionValue> visitAllColumns() {
                return row.values().iterator();
            }

            @Override
            public Iterator<TransactionValue> visitExactColumns(ImmutableSortedSet<Column> columns) {
                return columns.stream().map(row::get).filter(Objects::nonNull).iterator();
            }

            @Override
            public Iterator<TransactionValue> visitColumnRange(
                    Optional<Column> fromInclusive, Optional<Column> toExclusive) {
                NavigableMap<Column, TransactionValue> filtered = row;
                if (fromInclusive.isPresent()) {
                    filtered = filtered.tailMap(fromInclusive.get(), INCLUSIVE);
                }
                if (toExclusive.isPresent()) {
                    filtered = filtered.headMap(toExclusive.get(), EXCLUSIVE);
                }
                return filtered.values().iterator();
            }
        });
    }

    public static final class Builder {
        private Table table;
        // never mutate this directly - always clone first. SnapTreeMap is mutable but efficiently cloneable.
        private SnapTreeMap<Row, SnapTreeMap<Column, TransactionValue>> writes;

        public Builder() {
            clear();
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
            SnapTreeMap<Row, SnapTreeMap<Column, TransactionValue>> newWrites = writes.clone();
            SnapTreeMap<Column, TransactionValue> columns = newWrites.getOrDefault(value.cell().row(), EMPTY).clone();
            columns.put(value.cell().column(), value);
            newWrites.put(value.cell().row(), columns);
            writes = newWrites;
            return this;
        }

        public Builder clear() {
            writes = new SnapTreeMap<>();
            table = null;
            return this;
        }

        public Builder mergeFrom(TableWrites other) {
            if (writes.isEmpty()) {
                writes = other.writes;
                return this;
            }
            other.writes.values().stream().map(Map::values).flatMap(Collection::stream).forEach(this::put);
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
