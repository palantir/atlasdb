package com.palantir.atlasdb.performance.schema.generated;

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
public final class ValueStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<ValueStreamIdxTable.ValueStreamIdxRow,
                                                ValueStreamIdxTable.ValueStreamIdxColumn,
                                                ValueStreamIdxTable.ValueStreamIdxColumnValue,
                                                ValueStreamIdxTable.ValueStreamIdxRowResult> {
    private final Transaction t;
    private final List<ValueStreamIdxTrigger> triggers;
    private final static String rawTableName = "blob_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static ValueStreamIdxTable of(Transaction t, Namespace namespace) {
        return new ValueStreamIdxTable(t, namespace, ImmutableList.<ValueStreamIdxTrigger>of());
    }

    static ValueStreamIdxTable of(Transaction t, Namespace namespace, ValueStreamIdxTrigger trigger, ValueStreamIdxTrigger... triggers) {
        return new ValueStreamIdxTable(t, namespace, ImmutableList.<ValueStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static ValueStreamIdxTable of(Transaction t, Namespace namespace, List<ValueStreamIdxTrigger> triggers) {
        return new ValueStreamIdxTable(t, namespace, triggers);
    }

    private ValueStreamIdxTable(Transaction t, Namespace namespace, List<ValueStreamIdxTrigger> triggers) {
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
     * ValueStreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class ValueStreamIdxRow implements Persistable, Comparable<ValueStreamIdxRow> {
        private final long id;

        public static ValueStreamIdxRow of(long id) {
            return new ValueStreamIdxRow(id);
        }

        private ValueStreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<ValueStreamIdxRow, Long> getIdFun() {
            return new Function<ValueStreamIdxRow, Long>() {
                @Override
                public Long apply(ValueStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, ValueStreamIdxRow> fromIdFun() {
            return new Function<Long, ValueStreamIdxRow>() {
                @Override
                public ValueStreamIdxRow apply(Long row) {
                    return ValueStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<ValueStreamIdxRow> BYTES_HYDRATOR = new Hydrator<ValueStreamIdxRow>() {
            @Override
            public ValueStreamIdxRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new ValueStreamIdxRow(id);
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
            ValueStreamIdxRow other = (ValueStreamIdxRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(ValueStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * ValueStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class ValueStreamIdxColumn implements Persistable, Comparable<ValueStreamIdxColumn> {
        private final byte[] reference;

        public static ValueStreamIdxColumn of(byte[] reference) {
            return new ValueStreamIdxColumn(reference);
        }

        private ValueStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<ValueStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<ValueStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(ValueStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], ValueStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], ValueStreamIdxColumn>() {
                @Override
                public ValueStreamIdxColumn apply(byte[] row) {
                    return ValueStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<ValueStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<ValueStreamIdxColumn>() {
            @Override
            public ValueStreamIdxColumn hydrateFromBytes(byte[] _input) {
                int _index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(_input, _index);
                _index += EncodingUtils.sizeOfSizedBytes(reference);
                return new ValueStreamIdxColumn(reference);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("reference", reference)
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
            ValueStreamIdxColumn other = (ValueStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(ValueStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface ValueStreamIdxTrigger {
        public void putValueStreamIdx(Multimap<ValueStreamIdxRow, ? extends ValueStreamIdxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal byte[] reference};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class ValueStreamIdxColumnValue implements ColumnValue<Long> {
        private final ValueStreamIdxColumn columnName;
        private final Long value;

        public static ValueStreamIdxColumnValue of(ValueStreamIdxColumn columnName, Long value) {
            return new ValueStreamIdxColumnValue(columnName, value);
        }

        private ValueStreamIdxColumnValue(ValueStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public ValueStreamIdxColumn getColumnName() {
            return columnName;
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static Long hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return EncodingUtils.decodeUnsignedVarLong(bytes, 0);
        }

        public static Function<ValueStreamIdxColumnValue, ValueStreamIdxColumn> getColumnNameFun() {
            return new Function<ValueStreamIdxColumnValue, ValueStreamIdxColumn>() {
                @Override
                public ValueStreamIdxColumn apply(ValueStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<ValueStreamIdxColumnValue, Long> getValueFun() {
            return new Function<ValueStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(ValueStreamIdxColumnValue columnValue) {
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

    public static final class ValueStreamIdxRowResult implements TypedRowResult {
        private final ValueStreamIdxRow rowName;
        private final ImmutableSet<ValueStreamIdxColumnValue> columnValues;

        public static ValueStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            ValueStreamIdxRow rowName = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<ValueStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = ValueStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(ValueStreamIdxColumnValue.of(col, value));
            }
            return new ValueStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private ValueStreamIdxRowResult(ValueStreamIdxRow rowName, ImmutableSet<ValueStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public ValueStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<ValueStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<ValueStreamIdxRowResult, ValueStreamIdxRow> getRowNameFun() {
            return new Function<ValueStreamIdxRowResult, ValueStreamIdxRow>() {
                @Override
                public ValueStreamIdxRow apply(ValueStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<ValueStreamIdxRowResult, ImmutableSet<ValueStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<ValueStreamIdxRowResult, ImmutableSet<ValueStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<ValueStreamIdxColumnValue> apply(ValueStreamIdxRowResult rowResult) {
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
    public void delete(ValueStreamIdxRow row, ValueStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<ValueStreamIdxRow> rows) {
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<ValueStreamIdxRow, ValueStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<ValueStreamIdxRow, ValueStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(ValueStreamIdxRow rowName, Iterable<ValueStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<ValueStreamIdxRow, ValueStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(ValueStreamIdxRow rowName, ValueStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<ValueStreamIdxRow, ValueStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<ValueStreamIdxRow, ? extends ValueStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (ValueStreamIdxTrigger trigger : triggers) {
            trigger.putValueStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<ValueStreamIdxRow, ValueStreamIdxColumn> values) {
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<ValueStreamIdxRow, ValueStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<ValueStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(ValueStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> get(Multimap<ValueStreamIdxRow, ValueStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                ValueStreamIdxRow row = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = ValueStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, ValueStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<ValueStreamIdxColumnValue> getRowColumns(ValueStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<ValueStreamIdxColumnValue> getRowColumns(ValueStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<ValueStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = ValueStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(ValueStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> getRowsMultimap(Iterable<ValueStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> getRowsMultimap(Iterable<ValueStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> getRowsMultimapInternal(Iterable<ValueStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<ValueStreamIdxRow, ValueStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            ValueStreamIdxRow row = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = ValueStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, ValueStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxColumnValue>> getRowsColumnRange(Iterable<ValueStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            ValueStreamIdxRow row = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<ValueStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = ValueStreamIdxColumnValue.hydrateValue(result.getValue());
                return ValueStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<ValueStreamIdxRow, ValueStreamIdxColumnValue>> getRowsColumnRange(Iterable<ValueStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            ValueStreamIdxRow row = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = ValueStreamIdxColumnValue.hydrateValue(e.getValue());
            ValueStreamIdxColumnValue colValue = ValueStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<ValueStreamIdxRow, Iterator<ValueStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<ValueStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<ValueStreamIdxRow, Iterator<ValueStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            ValueStreamIdxRow row = ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<ValueStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                ValueStreamIdxColumn col = ValueStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = ValueStreamIdxColumnValue.hydrateValue(result.getValue());
                return ValueStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<ValueStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<ValueStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, ValueStreamIdxRowResult>() {
            @Override
            public ValueStreamIdxRowResult apply(RowResult<byte[]> input) {
                return ValueStreamIdxRowResult.of(input);
            }
        });
    }

    @Override
    public List<String> findConstraintFailures(Map<Cell, byte[]> _writes,
                                               ConstraintCheckingTransaction _transaction,
                                               AtlasDbConstraintCheckingMode _constraintCheckingMode) {
        return ImmutableList.of();
    }

    @Override
    public List<String> findConstraintFailuresNoRead(Map<Cell, byte[]> _writes,
                                                     AtlasDbConstraintCheckingMode _constraintCheckingMode) {
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
    static String __CLASS_HASH = "AaO2qXe3eP1wnoPhGy6XdQ==";
}
