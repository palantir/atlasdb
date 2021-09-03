package com.palantir.atlasdb.todo.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
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
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
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
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
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
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;

@Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
@SuppressWarnings({"all", "deprecation"})
public final class TodoTable implements
        AtlasDbMutablePersistentTable<TodoTable.TodoRow,
                                         TodoTable.TodoNamedColumnValue<?>,
                                         TodoTable.TodoRowResult>,
        AtlasDbNamedMutableTable<TodoTable.TodoRow,
                                    TodoTable.TodoNamedColumnValue<?>,
                                    TodoTable.TodoRowResult> {
    private final Transaction t;
    private final List<TodoTrigger> triggers;
    private final static String rawTableName = "todo";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TodoNamedColumn.values());

    static TodoTable of(Transaction t, Namespace namespace) {
        return new TodoTable(t, namespace, ImmutableList.<TodoTrigger>of());
    }

    static TodoTable of(Transaction t, Namespace namespace, TodoTrigger trigger, TodoTrigger... triggers) {
        return new TodoTable(t, namespace, ImmutableList.<TodoTrigger>builder().add(trigger).add(triggers).build());
    }

    static TodoTable of(Transaction t, Namespace namespace, List<TodoTrigger> triggers) {
        return new TodoTable(t, namespace, triggers);
    }

    private TodoTable(Transaction t, Namespace namespace, List<TodoTrigger> triggers) {
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
     * TodoRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class TodoRow implements Persistable, Comparable<TodoRow> {
        private final long id;

        public static TodoRow of(long id) {
            return new TodoRow(id);
        }

        private TodoRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<TodoRow, Long> getIdFun() {
            return new Function<TodoRow, Long>() {
                @Override
                public Long apply(TodoRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, TodoRow> fromIdFun() {
            return new Function<Long, TodoRow>() {
                @Override
                public TodoRow apply(Long row) {
                    return TodoRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<TodoRow> BYTES_HYDRATOR = new Hydrator<TodoRow>() {
            @Override
            public TodoRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new TodoRow(id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("id", id)
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
            TodoRow other = (TodoRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(TodoRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface TodoNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class Text implements TodoNamedColumnValue<String> {
        private final String value;

        public static Text of(String value) {
            return new Text(value);
        }

        private Text(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "text";
        }

        @Override
        public String getShortColumnName() {
            return "t";
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("t");
        }

        public static final Hydrator<Text> BYTES_HYDRATOR = new Hydrator<Text>() {
            @Override
            public Text hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(PtBytes.toString(bytes, 0, bytes.length-0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface TodoTrigger {
        public void putTodo(Multimap<TodoRow, ? extends TodoNamedColumnValue<?>> newRows);
    }

    public static final class TodoRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TodoRowResult of(RowResult<byte[]> row) {
            return new TodoRowResult(row);
        }

        private TodoRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TodoRow getRowName() {
            return TodoRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TodoRowResult, TodoRow> getRowNameFun() {
            return new Function<TodoRowResult, TodoRow>() {
                @Override
                public TodoRow apply(TodoRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TodoRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TodoRowResult>() {
                @Override
                public TodoRowResult apply(RowResult<byte[]> rowResult) {
                    return new TodoRowResult(rowResult);
                }
            };
        }

        public boolean hasText() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("t"));
        }

        public String getText() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("t"));
            if (bytes == null) {
                return null;
            }
            Text value = Text.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TodoRowResult, String> getTextFun() {
            return new Function<TodoRowResult, String>() {
                @Override
                public String apply(TodoRowResult rowResult) {
                    return rowResult.getText();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Text", getText())
                .toString();
        }
    }

    public enum TodoNamedColumn {
        TEXT {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("t");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TodoNamedColumn, byte[]> toShortName() {
            return new Function<TodoNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TodoNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TodoNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TodoNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TodoNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TodoNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TodoNamedColumnValue<?>>>builder()
                .put("t", Text.BYTES_HYDRATOR)
                .build();

    public Map<TodoRow, String> getTexts(Collection<TodoRow> rows) {
        Map<Cell, TodoRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TodoRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("t")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TodoRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = Text.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putText(TodoRow row, String value) {
        put(ImmutableMultimap.of(row, Text.of(value)));
    }

    public void putText(Map<TodoRow, String> map) {
        Map<TodoRow, TodoNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TodoRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Text.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TodoRow, ? extends TodoNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TodoTrigger trigger : triggers) {
            trigger.putTodo(rows);
        }
    }

    public void deleteText(TodoRow row) {
        deleteText(ImmutableSet.of(row));
    }

    public void deleteText(Iterable<TodoRow> rows) {
        byte[] col = PtBytes.toCachedBytes("t");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TodoRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TodoRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("t")));
        t.delete(tableRef, cells);
    }

    public Optional<TodoRowResult> getRow(TodoRow row) {
        return getRow(row, allColumns);
    }

    public Optional<TodoRowResult> getRow(TodoRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(TodoRowResult.of(rowResult));
        }
    }

    @Override
    public List<TodoRowResult> getRows(Iterable<TodoRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TodoRowResult> getRows(Iterable<TodoRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TodoRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TodoRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TodoNamedColumnValue<?>> getRowColumns(TodoRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TodoNamedColumnValue<?>> getRowColumns(TodoRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TodoNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TodoRow, TodoNamedColumnValue<?>> getRowsMultimap(Iterable<TodoRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TodoRow, TodoNamedColumnValue<?>> getRowsMultimap(Iterable<TodoRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TodoRow, TodoNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TodoRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TodoRow, TodoNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TodoRow, TodoNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TodoRow row = TodoRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TodoRow, BatchingVisitable<TodoNamedColumnValue<?>>> getRowsColumnRange(Iterable<TodoRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TodoRow, BatchingVisitable<TodoNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TodoRow row = TodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TodoNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TodoRow, TodoNamedColumnValue<?>>> getRowsColumnRange(Iterable<TodoRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TodoRow row = TodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TodoNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TodoRow, Iterator<TodoNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<TodoRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TodoRow, Iterator<TodoNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TodoRow row = TodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TodoNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private ColumnSelection optimizeColumnSelection(ColumnSelection columns) {
        if (columns.allColumnsSelected()) {
            return allColumns;
        }
        return columns;
    }

    public BatchingVisitableView<TodoRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TodoRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TodoRowResult>() {
            @Override
            public TodoRowResult apply(RowResult<byte[]> input) {
                return TodoRowResult.of(input);
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
     * {@link AtlasDbConstraintCheckingMode}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
     * {@link BatchColumnRangeSelection}
     * {@link BatchingVisitable}
     * {@link BatchingVisitableView}
     * {@link BatchingVisitables}
     * {@link BiFunction}
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
     * {@link Function}
     * {@link Generated}
     * {@link HashMultimap}
     * {@link HashSet}
     * {@link Hashing}
     * {@link Hydrator}
     * {@link ImmutableGetRangesQuery}
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
     * {@link Stream}
     * {@link Supplier}
     * {@link TableReference}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UUID}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "2TUlQO74MDuPAp1x+H3UCw==";
}
