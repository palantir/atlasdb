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
public final class NamespacedTodoTable implements
        AtlasDbDynamicMutablePersistentTable<NamespacedTodoTable.NamespacedTodoRow,
                                                NamespacedTodoTable.NamespacedTodoColumn,
                                                NamespacedTodoTable.NamespacedTodoColumnValue,
                                                NamespacedTodoTable.NamespacedTodoRowResult> {
    private final Transaction t;
    private final List<NamespacedTodoTrigger> triggers;
    private final static String rawTableName = "namespacedTodo";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static NamespacedTodoTable of(Transaction t, Namespace namespace) {
        return new NamespacedTodoTable(t, namespace, ImmutableList.<NamespacedTodoTrigger>of());
    }

    static NamespacedTodoTable of(Transaction t, Namespace namespace, NamespacedTodoTrigger trigger, NamespacedTodoTrigger... triggers) {
        return new NamespacedTodoTable(t, namespace, ImmutableList.<NamespacedTodoTrigger>builder().add(trigger).add(triggers).build());
    }

    static NamespacedTodoTable of(Transaction t, Namespace namespace, List<NamespacedTodoTrigger> triggers) {
        return new NamespacedTodoTable(t, namespace, triggers);
    }

    private NamespacedTodoTable(Transaction t, Namespace namespace, List<NamespacedTodoTrigger> triggers) {
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
     * NamespacedTodoRow {
     *   {@literal String namespace};
     * }
     * </pre>
     */
    public static final class NamespacedTodoRow implements Persistable, Comparable<NamespacedTodoRow> {
        private final String namespace;

        public static NamespacedTodoRow of(String namespace) {
            return new NamespacedTodoRow(namespace);
        }

        private NamespacedTodoRow(String namespace) {
            this.namespace = namespace;
        }

        public String getNamespace() {
            return namespace;
        }

        public static Function<NamespacedTodoRow, String> getNamespaceFun() {
            return new Function<NamespacedTodoRow, String>() {
                @Override
                public String apply(NamespacedTodoRow row) {
                    return row.namespace;
                }
            };
        }

        public static Function<String, NamespacedTodoRow> fromNamespaceFun() {
            return new Function<String, NamespacedTodoRow>() {
                @Override
                public NamespacedTodoRow apply(String row) {
                    return NamespacedTodoRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] namespaceBytes = PtBytes.toBytes(namespace);
            return EncodingUtils.add(namespaceBytes);
        }

        public static final Hydrator<NamespacedTodoRow> BYTES_HYDRATOR = new Hydrator<NamespacedTodoRow>() {
            @Override
            public NamespacedTodoRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String namespace = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new NamespacedTodoRow(namespace);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("namespace", namespace)
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
            NamespacedTodoRow other = (NamespacedTodoRow) obj;
            return Objects.equals(namespace, other.namespace);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(namespace);
        }

        @Override
        public int compareTo(NamespacedTodoRow o) {
            return ComparisonChain.start()
                .compare(this.namespace, o.namespace)
                .result();
        }
    }

    /**
     * <pre>
     * NamespacedTodoColumn {
     *   {@literal Long todoId};
     * }
     * </pre>
     */
    public static final class NamespacedTodoColumn implements Persistable, Comparable<NamespacedTodoColumn> {
        private final long todoId;

        public static NamespacedTodoColumn of(long todoId) {
            return new NamespacedTodoColumn(todoId);
        }

        private NamespacedTodoColumn(long todoId) {
            this.todoId = todoId;
        }

        public long getTodoId() {
            return todoId;
        }

        public static Function<NamespacedTodoColumn, Long> getTodoIdFun() {
            return new Function<NamespacedTodoColumn, Long>() {
                @Override
                public Long apply(NamespacedTodoColumn row) {
                    return row.todoId;
                }
            };
        }

        public static Function<Long, NamespacedTodoColumn> fromTodoIdFun() {
            return new Function<Long, NamespacedTodoColumn>() {
                @Override
                public NamespacedTodoColumn apply(Long row) {
                    return NamespacedTodoColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] todoIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ todoId);
            return EncodingUtils.add(todoIdBytes);
        }

        public static final Hydrator<NamespacedTodoColumn> BYTES_HYDRATOR = new Hydrator<NamespacedTodoColumn>() {
            @Override
            public NamespacedTodoColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long todoId = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new NamespacedTodoColumn(todoId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("todoId", todoId)
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
            NamespacedTodoColumn other = (NamespacedTodoColumn) obj;
            return Objects.equals(todoId, other.todoId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(todoId);
        }

        @Override
        public int compareTo(NamespacedTodoColumn o) {
            return ComparisonChain.start()
                .compare(this.todoId, o.todoId)
                .result();
        }
    }

    public interface NamespacedTodoTrigger {
        public void putNamespacedTodo(Multimap<NamespacedTodoRow, ? extends NamespacedTodoColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long todoId};
     * }
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class NamespacedTodoColumnValue implements ColumnValue<String> {
        private final NamespacedTodoColumn columnName;
        private final String value;

        public static NamespacedTodoColumnValue of(NamespacedTodoColumn columnName, String value) {
            return new NamespacedTodoColumnValue(columnName, value);
        }

        private NamespacedTodoColumnValue(NamespacedTodoColumn columnName, String value) {
            this.columnName = columnName;
            this.value = value;
        }

        public NamespacedTodoColumn getColumnName() {
            return columnName;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static String hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return PtBytes.toString(bytes, 0, bytes.length-0);
        }

        public static Function<NamespacedTodoColumnValue, NamespacedTodoColumn> getColumnNameFun() {
            return new Function<NamespacedTodoColumnValue, NamespacedTodoColumn>() {
                @Override
                public NamespacedTodoColumn apply(NamespacedTodoColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<NamespacedTodoColumnValue, String> getValueFun() {
            return new Function<NamespacedTodoColumnValue, String>() {
                @Override
                public String apply(NamespacedTodoColumnValue columnValue) {
                    return columnValue.getValue();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("ColumnName", this.columnName)
                .add("Value", this.value)
                .toString();
        }
    }

    public static final class NamespacedTodoRowResult implements TypedRowResult {
        private final NamespacedTodoRow rowName;
        private final ImmutableSet<NamespacedTodoColumnValue> columnValues;

        public static NamespacedTodoRowResult of(RowResult<byte[]> rowResult) {
            NamespacedTodoRow rowName = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<NamespacedTodoColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String value = NamespacedTodoColumnValue.hydrateValue(e.getValue());
                columnValues.add(NamespacedTodoColumnValue.of(col, value));
            }
            return new NamespacedTodoRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private NamespacedTodoRowResult(NamespacedTodoRow rowName, ImmutableSet<NamespacedTodoColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public NamespacedTodoRow getRowName() {
            return rowName;
        }

        public Set<NamespacedTodoColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<NamespacedTodoRowResult, NamespacedTodoRow> getRowNameFun() {
            return new Function<NamespacedTodoRowResult, NamespacedTodoRow>() {
                @Override
                public NamespacedTodoRow apply(NamespacedTodoRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<NamespacedTodoRowResult, ImmutableSet<NamespacedTodoColumnValue>> getColumnValuesFun() {
            return new Function<NamespacedTodoRowResult, ImmutableSet<NamespacedTodoColumnValue>>() {
                @Override
                public ImmutableSet<NamespacedTodoColumnValue> apply(NamespacedTodoRowResult rowResult) {
                    return rowResult.columnValues;
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("ColumnValues", getColumnValues())
                .toString();
        }
    }

    @Override
    public void delete(NamespacedTodoRow row, NamespacedTodoColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<NamespacedTodoRow> rows) {
        Multimap<NamespacedTodoRow, NamespacedTodoColumn> toRemove = HashMultimap.create();
        Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> result = getRowsMultimap(rows);
        for (Entry<NamespacedTodoRow, NamespacedTodoColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<NamespacedTodoRow, NamespacedTodoColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(NamespacedTodoRow rowName, Iterable<NamespacedTodoColumnValue> values) {
        put(ImmutableMultimap.<NamespacedTodoRow, NamespacedTodoColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(NamespacedTodoRow rowName, NamespacedTodoColumnValue... values) {
        put(ImmutableMultimap.<NamespacedTodoRow, NamespacedTodoColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<NamespacedTodoRow, ? extends NamespacedTodoColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (NamespacedTodoTrigger trigger : triggers) {
            trigger.putNamespacedTodo(values);
        }
    }

    @Override
    public void touch(Multimap<NamespacedTodoRow, NamespacedTodoColumn> values) {
        Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<NamespacedTodoRow, NamespacedTodoColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<NamespacedTodoRow, NamespacedTodoColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<NamespacedTodoColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(NamespacedTodoColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> get(Multimap<NamespacedTodoRow, NamespacedTodoColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                NamespacedTodoRow row = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                String val = NamespacedTodoColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, NamespacedTodoColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<NamespacedTodoColumnValue> getRowColumns(NamespacedTodoRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<NamespacedTodoColumnValue> getRowColumns(NamespacedTodoRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<NamespacedTodoColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = NamespacedTodoColumnValue.hydrateValue(e.getValue());
                ret.add(NamespacedTodoColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> getRowsMultimap(Iterable<NamespacedTodoRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> getRowsMultimap(Iterable<NamespacedTodoRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> getRowsMultimapInternal(Iterable<NamespacedTodoRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<NamespacedTodoRow, NamespacedTodoColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            NamespacedTodoRow row = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = NamespacedTodoColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, NamespacedTodoColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<NamespacedTodoRow, BatchingVisitable<NamespacedTodoColumnValue>> getRowsColumnRange(Iterable<NamespacedTodoRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<NamespacedTodoRow, BatchingVisitable<NamespacedTodoColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            NamespacedTodoRow row = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<NamespacedTodoColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = NamespacedTodoColumnValue.hydrateValue(result.getValue());
                return NamespacedTodoColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<NamespacedTodoRow, NamespacedTodoColumnValue>> getRowsColumnRange(Iterable<NamespacedTodoRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            NamespacedTodoRow row = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            String val = NamespacedTodoColumnValue.hydrateValue(e.getValue());
            NamespacedTodoColumnValue colValue = NamespacedTodoColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<NamespacedTodoRow, Iterator<NamespacedTodoColumnValue>> getRowsColumnRangeIterator(Iterable<NamespacedTodoRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<NamespacedTodoRow, Iterator<NamespacedTodoColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            NamespacedTodoRow row = NamespacedTodoRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<NamespacedTodoColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                NamespacedTodoColumn col = NamespacedTodoColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = NamespacedTodoColumnValue.hydrateValue(result.getValue());
                return NamespacedTodoColumnValue.of(col, val);
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

    public BatchingVisitableView<NamespacedTodoRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<NamespacedTodoRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, NamespacedTodoRowResult>() {
            @Override
            public NamespacedTodoRowResult apply(RowResult<byte[]> input) {
                return NamespacedTodoRowResult.of(input);
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
    static String __CLASS_HASH = "3DJdGHmS1KFWFWaP05WZiQ==";
}
