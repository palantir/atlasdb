package com.palantir.atlasdb.blob.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import com.google.common.base.Optional;
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
public final class DataStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<DataStreamIdxTable.DataStreamIdxRow,
                                                DataStreamIdxTable.DataStreamIdxColumn,
                                                DataStreamIdxTable.DataStreamIdxColumnValue,
                                                DataStreamIdxTable.DataStreamIdxRowResult> {
    private final Transaction t;
    private final List<DataStreamIdxTrigger> triggers;
    private final static String rawTableName = "data_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static DataStreamIdxTable of(Transaction t, Namespace namespace) {
        return new DataStreamIdxTable(t, namespace, ImmutableList.<DataStreamIdxTrigger>of());
    }

    static DataStreamIdxTable of(Transaction t, Namespace namespace, DataStreamIdxTrigger trigger, DataStreamIdxTrigger... triggers) {
        return new DataStreamIdxTable(t, namespace, ImmutableList.<DataStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static DataStreamIdxTable of(Transaction t, Namespace namespace, List<DataStreamIdxTrigger> triggers) {
        return new DataStreamIdxTable(t, namespace, triggers);
    }

    private DataStreamIdxTable(Transaction t, Namespace namespace, List<DataStreamIdxTrigger> triggers) {
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
     * DataStreamIdxRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class DataStreamIdxRow implements Persistable, Comparable<DataStreamIdxRow> {
        private final long hashOfRowComponents;
        private final long id;

        public static DataStreamIdxRow of(long id) {
            long hashOfRowComponents = computeHashFirstComponents(id);
            return new DataStreamIdxRow(hashOfRowComponents, id);
        }

        private DataStreamIdxRow(long hashOfRowComponents, long id) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<DataStreamIdxRow, Long> getIdFun() {
            return new Function<DataStreamIdxRow, Long>() {
                @Override
                public Long apply(DataStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, DataStreamIdxRow> fromIdFun() {
            return new Function<Long, DataStreamIdxRow>() {
                @Override
                public DataStreamIdxRow apply(Long row) {
                    return DataStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(hashOfRowComponentsBytes, idBytes);
        }

        public static final Hydrator<DataStreamIdxRow> BYTES_HYDRATOR = new Hydrator<DataStreamIdxRow>() {
            @Override
            public DataStreamIdxRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new DataStreamIdxRow(hashOfRowComponents, id);
            }
        };

        public static long computeHashFirstComponents(long id) {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(idBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
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
            DataStreamIdxRow other = (DataStreamIdxRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, id });
        }

        @Override
        public int compareTo(DataStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * DataStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class DataStreamIdxColumn implements Persistable, Comparable<DataStreamIdxColumn> {
        private final byte[] reference;

        public static DataStreamIdxColumn of(byte[] reference) {
            return new DataStreamIdxColumn(reference);
        }

        private DataStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<DataStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<DataStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(DataStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], DataStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], DataStreamIdxColumn>() {
                @Override
                public DataStreamIdxColumn apply(byte[] row) {
                    return DataStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<DataStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<DataStreamIdxColumn>() {
            @Override
            public DataStreamIdxColumn hydrateFromBytes(byte[] _input) {
                int _index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(_input, _index);
                _index += EncodingUtils.sizeOfSizedBytes(reference);
                return new DataStreamIdxColumn(reference);
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
            DataStreamIdxColumn other = (DataStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(DataStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface DataStreamIdxTrigger {
        public void putDataStreamIdx(Multimap<DataStreamIdxRow, ? extends DataStreamIdxColumnValue> newRows);
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
    public static final class DataStreamIdxColumnValue implements ColumnValue<Long> {
        private final DataStreamIdxColumn columnName;
        private final Long value;

        public static DataStreamIdxColumnValue of(DataStreamIdxColumn columnName, Long value) {
            return new DataStreamIdxColumnValue(columnName, value);
        }

        private DataStreamIdxColumnValue(DataStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public DataStreamIdxColumn getColumnName() {
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

        public static Function<DataStreamIdxColumnValue, DataStreamIdxColumn> getColumnNameFun() {
            return new Function<DataStreamIdxColumnValue, DataStreamIdxColumn>() {
                @Override
                public DataStreamIdxColumn apply(DataStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<DataStreamIdxColumnValue, Long> getValueFun() {
            return new Function<DataStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(DataStreamIdxColumnValue columnValue) {
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

    public static final class DataStreamIdxRowResult implements TypedRowResult {
        private final DataStreamIdxRow rowName;
        private final ImmutableSet<DataStreamIdxColumnValue> columnValues;

        public static DataStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            DataStreamIdxRow rowName = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<DataStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = DataStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(DataStreamIdxColumnValue.of(col, value));
            }
            return new DataStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private DataStreamIdxRowResult(DataStreamIdxRow rowName, ImmutableSet<DataStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public DataStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<DataStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<DataStreamIdxRowResult, DataStreamIdxRow> getRowNameFun() {
            return new Function<DataStreamIdxRowResult, DataStreamIdxRow>() {
                @Override
                public DataStreamIdxRow apply(DataStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<DataStreamIdxRowResult, ImmutableSet<DataStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<DataStreamIdxRowResult, ImmutableSet<DataStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<DataStreamIdxColumnValue> apply(DataStreamIdxRowResult rowResult) {
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
    public void delete(DataStreamIdxRow row, DataStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<DataStreamIdxRow> rows) {
        Multimap<DataStreamIdxRow, DataStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<DataStreamIdxRow, DataStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<DataStreamIdxRow, DataStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(DataStreamIdxRow rowName, Iterable<DataStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<DataStreamIdxRow, DataStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(DataStreamIdxRow rowName, DataStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<DataStreamIdxRow, DataStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<DataStreamIdxRow, ? extends DataStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (DataStreamIdxTrigger trigger : triggers) {
            trigger.putDataStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<DataStreamIdxRow, DataStreamIdxColumn> values) {
        Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<DataStreamIdxRow, DataStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<DataStreamIdxRow, DataStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<DataStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(DataStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> get(Multimap<DataStreamIdxRow, DataStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                DataStreamIdxRow row = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = DataStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DataStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<DataStreamIdxColumnValue> getRowColumns(DataStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<DataStreamIdxColumnValue> getRowColumns(DataStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<DataStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = DataStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(DataStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> getRowsMultimap(Iterable<DataStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> getRowsMultimap(Iterable<DataStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> getRowsMultimapInternal(Iterable<DataStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<DataStreamIdxRow, DataStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            DataStreamIdxRow row = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = DataStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DataStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<DataStreamIdxRow, BatchingVisitable<DataStreamIdxColumnValue>> getRowsColumnRange(Iterable<DataStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataStreamIdxRow, BatchingVisitable<DataStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataStreamIdxRow row = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<DataStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = DataStreamIdxColumnValue.hydrateValue(result.getValue());
                return DataStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<DataStreamIdxRow, DataStreamIdxColumnValue>> getRowsColumnRange(Iterable<DataStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            DataStreamIdxRow row = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = DataStreamIdxColumnValue.hydrateValue(e.getValue());
            DataStreamIdxColumnValue colValue = DataStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<DataStreamIdxRow, Iterator<DataStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<DataStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataStreamIdxRow, Iterator<DataStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataStreamIdxRow row = DataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<DataStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                DataStreamIdxColumn col = DataStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = DataStreamIdxColumnValue.hydrateValue(result.getValue());
                return DataStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<DataStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<DataStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, DataStreamIdxRowResult>() {
            @Override
            public DataStreamIdxRowResult apply(RowResult<byte[]> input) {
                return DataStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "JelgYU1LGPHdRT6dwB6nNA==";
}
