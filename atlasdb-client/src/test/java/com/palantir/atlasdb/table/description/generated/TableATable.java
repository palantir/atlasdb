package com.palantir.atlasdb.table.description.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelections;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.Prefix;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedExpiringSet;
import com.palantir.atlasdb.table.api.AtlasDbNamedMutableTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedPersistentSet;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.atlasdb.table.api.TypedRowResult;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.table.generation.Descending;
import com.palantir.atlasdb.table.generation.NamedColumnValue;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableView;
import com.palantir.common.persist.Persistable;
import com.palantir.common.persist.Persistable.Hydrator;
import com.palantir.common.persist.Persistables;
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;

@Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
public final class TableATable implements
        AtlasDbMutablePersistentTable<TableATable.TableARow,
                                         TableATable.TableANamedColumnValue<?>,
                                         TableATable.TableARowResult>,
        AtlasDbNamedMutableTable<TableATable.TableARow,
                                    TableATable.TableANamedColumnValue<?>,
                                    TableATable.TableARowResult> {
    private final Transaction t;
    private final List<TableATrigger> triggers;
    private final static String rawTableName = "tableA";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TableANamedColumn.values());

    static TableATable of(Transaction t, Namespace namespace) {
        return new TableATable(t, namespace, ImmutableList.<TableATrigger>of());
    }

    static TableATable of(Transaction t, Namespace namespace, TableATrigger trigger, TableATrigger... triggers) {
        return new TableATable(t, namespace, ImmutableList.<TableATrigger>builder().add(trigger).add(triggers).build());
    }

    static TableATable of(Transaction t, Namespace namespace, List<TableATrigger> triggers) {
        return new TableATable(t, namespace, triggers);
    }

    private TableATable(Transaction t, Namespace namespace, List<TableATrigger> triggers) {
        this.t = t;
        this.tableRef = TableReference.create(namespace, rawTableName);
        this.triggers = triggers;
    }

    public static String getRawTableName() {
        return rawTableName;
    }

    public TableReference getTableRef() {
        return tableRef;
    }

    public String getTableName() {
        return tableRef.getQualifiedName();
    }

    public Namespace getNamespace() {
        return tableRef.getNamespace();
    }

    /**
     * <pre>
     * TableARow {
     *   {@literal String component1};
     * }
     * </pre>
     */
    public static final class TableARow implements Persistable, Comparable<TableARow> {
        private final String component1;

        public static TableARow of(String component1) {
            return new TableARow(component1);
        }

        private TableARow(String component1) {
            this.component1 = component1;
        }

        public String getComponent1() {
            return component1;
        }

        public static Function<TableARow, String> getComponent1Fun() {
            return new Function<TableARow, String>() {
                @Override
                public String apply(TableARow row) {
                    return row.component1;
                }
            };
        }

        public static Function<String, TableARow> fromComponent1Fun() {
            return new Function<String, TableARow>() {
                @Override
                public TableARow apply(String row) {
                    return TableARow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component1Bytes = PtBytes.toBytes(component1);
            return EncodingUtils.add(component1Bytes);
        }

        public static final Hydrator<TableARow> BYTES_HYDRATOR = new Hydrator<TableARow>() {
            @Override
            public TableARow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String component1 = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new TableARow(component1);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("component1", component1)
                .toString();
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
            TableARow other = (TableARow) obj;
            return Objects.equal(component1, other.component1);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component1);
        }

        @Override
        public int compareTo(TableARow o) {
            return ComparisonChain.start()
                .compare(this.component1, o.component1)
                .result();
        }
    }

    public interface TableANamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column1 implements TableANamedColumnValue<Long> {
        private final Long value;

        public static Column1 of(Long value) {
            return new Column1(value);
        }

        private Column1(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column1";
        }

        @Override
        public String getShortColumnName() {
            return "c";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<Column1> BYTES_HYDRATOR = new Hydrator<Column1>() {
            @Override
            public Column1 hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeUnsignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface TableATrigger {
        public void putTableA(Multimap<TableARow, ? extends TableANamedColumnValue<?>> newRows);
    }

    public static final class TableARowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TableARowResult of(RowResult<byte[]> row) {
            return new TableARowResult(row);
        }

        private TableARowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TableARow getRowName() {
            return TableARow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TableARowResult, TableARow> getRowNameFun() {
            return new Function<TableARowResult, TableARow>() {
                @Override
                public TableARow apply(TableARowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TableARowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TableARowResult>() {
                @Override
                public TableARowResult apply(RowResult<byte[]> rowResult) {
                    return new TableARowResult(rowResult);
                }
            };
        }

        public boolean hasColumn1() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public Long getColumn1() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            Column1 value = Column1.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TableARowResult, Long> getColumn1Fun() {
            return new Function<TableARowResult, Long>() {
                @Override
                public Long apply(TableARowResult rowResult) {
                    return rowResult.getColumn1();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Column1", getColumn1())
                .toString();
        }
    }

    public enum TableANamedColumn {
        COLUMN1 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TableANamedColumn, byte[]> toShortName() {
            return new Function<TableANamedColumn, byte[]>() {
                @Override
                public byte[] apply(TableANamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TableANamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TableANamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TableANamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TableANamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TableANamedColumnValue<?>>>builder()
                .put("c", Column1.BYTES_HYDRATOR)
                .build();

    public Map<TableARow, Long> getColumn1s(Collection<TableARow> rows) {
        Map<Cell, TableARow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TableARow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TableARow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column1.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn1(TableARow row, Long value) {
        put(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1(Map<TableARow, Long> map) {
        Map<TableARow, TableANamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TableARow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn1UnlessExists(TableARow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1UnlessExists(Map<TableARow, Long> map) {
        Map<TableARow, TableANamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TableARow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TableARow, ? extends TableANamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TableATrigger trigger : triggers) {
            trigger.putTableA(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<TableARow, ? extends TableANamedColumnValue<?>> rows) {
        Multimap<TableARow, TableANamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<TableARow, TableANamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<TableARow, ? extends TableANamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteColumn1(TableARow row) {
        deleteColumn1(ImmutableSet.of(row));
    }

    public void deleteColumn1(Iterable<TableARow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TableARow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<? extends TableARow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        t.delete(tableRef, cells);
    }

    public Optional<TableARowResult> getRow(TableARow row) {
        return getRow(row, allColumns);
    }

    public Optional<TableARowResult> getRow(TableARow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(TableARowResult.of(rowResult));
        }
    }

    @Override
    public List<TableARowResult> getRows(Iterable<TableARow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TableARowResult> getRows(Iterable<TableARow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TableARowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TableARowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TableARowResult> getAsyncRows(Iterable<TableARow> rows, ExecutorService exec) {
        return getAsyncRows(rows, allColumns, exec);
    }

    @Override
    public List<TableARowResult> getAsyncRows(final Iterable<TableARow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<TableARowResult>> c =
                new Callable<List<TableARowResult>>() {
            @Override
            public List<TableARowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<TableANamedColumnValue<?>> getRowColumns(TableARow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TableANamedColumnValue<?>> getRowColumns(TableARow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TableANamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TableARow, TableANamedColumnValue<?>> getRowsMultimap(Iterable<? extends TableARow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TableARow, TableANamedColumnValue<?>> getRowsMultimap(Iterable<TableARow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<TableARow, TableANamedColumnValue<?>> getAsyncRowsMultimap(Iterable<TableARow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, allColumns, exec);
    }

    @Override
    public Multimap<TableARow, TableANamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<TableARow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<TableARow, TableANamedColumnValue<?>>> c =
                new Callable<Multimap<TableARow, TableANamedColumnValue<?>>>() {
            @Override
            public Multimap<TableARow, TableANamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<TableARow, TableANamedColumnValue<?>> getRowsMultimapInternal(Iterable<? extends TableARow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TableARow, TableANamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TableARow, TableANamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TableARow row = TableARow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TableARow, BatchingVisitable<TableANamedColumnValue<?>>> getRowsColumnRange(Iterable<TableARow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TableARow, BatchingVisitable<TableANamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TableARow row = TableARow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TableANamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TableARow, TableANamedColumnValue<?>>> getRowsColumnRange(Iterable<TableARow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TableARow row = TableARow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TableANamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<TableARowResult> getRange(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            range = range.getBuilder().retainColumns(allColumns).build();
        }
        return BatchingVisitables.transform(t.getRange(tableRef, range), new Function<RowResult<byte[]>, TableARowResult>() {
            @Override
            public TableARowResult apply(RowResult<byte[]> input) {
                return TableARowResult.of(input);
            }
        });
    }

    public IterableView<BatchingVisitable<TableARowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, ranges);
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<TableARowResult>>() {
            @Override
            public BatchingVisitable<TableARowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, TableARowResult>() {
                    @Override
                    public TableARowResult apply(RowResult<byte[]> row) {
                        return TableARowResult.of(row);
                    }
                });
            }
        });
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(TableARowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<? extends TableARow>, RuntimeException>() {
            @Override
            public boolean visit(List<? extends TableARow> rows) {
                delete(rows);
                return true;
            }
        });
    }

    @Override
    public List<String> findConstraintFailures(Map<Cell, byte[]> writes,
                                               ConstraintCheckingTransaction transaction,
                                               AtlasDbConstraintCheckingMode constraintCheckingMode) {
        return ImmutableList.of();
    }

    @Override
    public List<String> findConstraintFailuresNoRead(Map<Cell, byte[]> writes,
                                                     AtlasDbConstraintCheckingMode constraintCheckingMode) {
        return ImmutableList.of();
    }

    /**
     * This exists to avoid unused import warnings
     * {@link AbortingVisitor}
     * {@link AbortingVisitors}
     * {@link ArrayListMultimap}
     * {@link Arrays}
     * {@link AssertUtils}
     * {@link AsyncProxy}
     * {@link AtlasDbConstraintCheckingMode}
     * {@link AtlasDbDynamicMutableExpiringTable}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutableExpiringTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedExpiringSet}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
     * {@link BatchColumnRangeSelection}
     * {@link BatchingVisitable}
     * {@link BatchingVisitableView}
     * {@link BatchingVisitables}
     * {@link Bytes}
     * {@link Callable}
     * {@link Cell}
     * {@link Cells}
     * {@link Collection}
     * {@link Collections2}
     * {@link ColumnRangeSelection}
     * {@link ColumnRangeSelections}
     * {@link ColumnSelection}
     * {@link ColumnValue}
     * {@link ColumnValues}
     * {@link ComparisonChain}
     * {@link Compression}
     * {@link CompressionUtils}
     * {@link ConstraintCheckingTransaction}
     * {@link Descending}
     * {@link EncodingUtils}
     * {@link Entry}
     * {@link EnumSet}
     * {@link ExecutorService}
     * {@link Function}
     * {@link Generated}
     * {@link HashMultimap}
     * {@link HashSet}
     * {@link Hashing}
     * {@link Hydrator}
     * {@link ImmutableList}
     * {@link ImmutableMap}
     * {@link ImmutableMultimap}
     * {@link ImmutableSet}
     * {@link InvalidProtocolBufferException}
     * {@link IterableView}
     * {@link Iterables}
     * {@link Iterator}
     * {@link Iterators}
     * {@link Joiner}
     * {@link List}
     * {@link Lists}
     * {@link Map}
     * {@link Maps}
     * {@link MoreObjects}
     * {@link Multimap}
     * {@link Multimaps}
     * {@link NamedColumnValue}
     * {@link Namespace}
     * {@link Objects}
     * {@link Optional}
     * {@link Persistable}
     * {@link Persistables}
     * {@link Prefix}
     * {@link PtBytes}
     * {@link RangeRequest}
     * {@link RowResult}
     * {@link Set}
     * {@link Sets}
     * {@link Sha256Hash}
     * {@link SortedMap}
     * {@link Supplier}
     * {@link TableReference}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "wG86qhmr/JfjLsM4x9piyg==";
}
