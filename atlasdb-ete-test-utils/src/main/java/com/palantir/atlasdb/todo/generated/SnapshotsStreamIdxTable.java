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
public final class SnapshotsStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow,
                                                SnapshotsStreamIdxTable.SnapshotsStreamIdxColumn,
                                                SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue,
                                                SnapshotsStreamIdxTable.SnapshotsStreamIdxRowResult> {
    private final Transaction t;
    private final List<SnapshotsStreamIdxTrigger> triggers;
    private final static String rawTableName = "snapshots_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SnapshotsStreamIdxTable of(Transaction t, Namespace namespace) {
        return new SnapshotsStreamIdxTable(t, namespace, ImmutableList.<SnapshotsStreamIdxTrigger>of());
    }

    static SnapshotsStreamIdxTable of(Transaction t, Namespace namespace, SnapshotsStreamIdxTrigger trigger, SnapshotsStreamIdxTrigger... triggers) {
        return new SnapshotsStreamIdxTable(t, namespace, ImmutableList.<SnapshotsStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static SnapshotsStreamIdxTable of(Transaction t, Namespace namespace, List<SnapshotsStreamIdxTrigger> triggers) {
        return new SnapshotsStreamIdxTable(t, namespace, triggers);
    }

    private SnapshotsStreamIdxTable(Transaction t, Namespace namespace, List<SnapshotsStreamIdxTrigger> triggers) {
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
     * SnapshotsStreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class SnapshotsStreamIdxRow implements Persistable, Comparable<SnapshotsStreamIdxRow> {
        private final long id;

        public static SnapshotsStreamIdxRow of(long id) {
            return new SnapshotsStreamIdxRow(id);
        }

        private SnapshotsStreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<SnapshotsStreamIdxRow, Long> getIdFun() {
            return new Function<SnapshotsStreamIdxRow, Long>() {
                @Override
                public Long apply(SnapshotsStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, SnapshotsStreamIdxRow> fromIdFun() {
            return new Function<Long, SnapshotsStreamIdxRow>() {
                @Override
                public SnapshotsStreamIdxRow apply(Long row) {
                    return SnapshotsStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<SnapshotsStreamIdxRow> BYTES_HYDRATOR = new Hydrator<SnapshotsStreamIdxRow>() {
            @Override
            public SnapshotsStreamIdxRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                return new SnapshotsStreamIdxRow(id);
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
            SnapshotsStreamIdxRow other = (SnapshotsStreamIdxRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(SnapshotsStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * SnapshotsStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class SnapshotsStreamIdxColumn implements Persistable, Comparable<SnapshotsStreamIdxColumn> {
        private final byte[] reference;

        public static SnapshotsStreamIdxColumn of(byte[] reference) {
            return new SnapshotsStreamIdxColumn(reference);
        }

        private SnapshotsStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<SnapshotsStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<SnapshotsStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(SnapshotsStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], SnapshotsStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], SnapshotsStreamIdxColumn>() {
                @Override
                public SnapshotsStreamIdxColumn apply(byte[] row) {
                    return SnapshotsStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<SnapshotsStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<SnapshotsStreamIdxColumn>() {
            @Override
            public SnapshotsStreamIdxColumn hydrateFromBytes(byte[] _input) {
                int _index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(_input, _index);
                _index += EncodingUtils.sizeOfSizedBytes(reference);
                return new SnapshotsStreamIdxColumn(reference);
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
            SnapshotsStreamIdxColumn other = (SnapshotsStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(SnapshotsStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface SnapshotsStreamIdxTrigger {
        public void putSnapshotsStreamIdx(Multimap<SnapshotsStreamIdxRow, ? extends SnapshotsStreamIdxColumnValue> newRows);
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
    public static final class SnapshotsStreamIdxColumnValue implements ColumnValue<Long> {
        private final SnapshotsStreamIdxColumn columnName;
        private final Long value;

        public static SnapshotsStreamIdxColumnValue of(SnapshotsStreamIdxColumn columnName, Long value) {
            return new SnapshotsStreamIdxColumnValue(columnName, value);
        }

        private SnapshotsStreamIdxColumnValue(SnapshotsStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SnapshotsStreamIdxColumn getColumnName() {
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

        public static Function<SnapshotsStreamIdxColumnValue, SnapshotsStreamIdxColumn> getColumnNameFun() {
            return new Function<SnapshotsStreamIdxColumnValue, SnapshotsStreamIdxColumn>() {
                @Override
                public SnapshotsStreamIdxColumn apply(SnapshotsStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SnapshotsStreamIdxColumnValue, Long> getValueFun() {
            return new Function<SnapshotsStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(SnapshotsStreamIdxColumnValue columnValue) {
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

    public static final class SnapshotsStreamIdxRowResult implements TypedRowResult {
        private final SnapshotsStreamIdxRow rowName;
        private final ImmutableSet<SnapshotsStreamIdxColumnValue> columnValues;

        public static SnapshotsStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            SnapshotsStreamIdxRow rowName = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SnapshotsStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = SnapshotsStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(SnapshotsStreamIdxColumnValue.of(col, value));
            }
            return new SnapshotsStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SnapshotsStreamIdxRowResult(SnapshotsStreamIdxRow rowName, ImmutableSet<SnapshotsStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SnapshotsStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<SnapshotsStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SnapshotsStreamIdxRowResult, SnapshotsStreamIdxRow> getRowNameFun() {
            return new Function<SnapshotsStreamIdxRowResult, SnapshotsStreamIdxRow>() {
                @Override
                public SnapshotsStreamIdxRow apply(SnapshotsStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SnapshotsStreamIdxRowResult, ImmutableSet<SnapshotsStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<SnapshotsStreamIdxRowResult, ImmutableSet<SnapshotsStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<SnapshotsStreamIdxColumnValue> apply(SnapshotsStreamIdxRowResult rowResult) {
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
    public void delete(SnapshotsStreamIdxRow row, SnapshotsStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SnapshotsStreamIdxRow> rows) {
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SnapshotsStreamIdxRow rowName, Iterable<SnapshotsStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SnapshotsStreamIdxRow rowName, SnapshotsStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SnapshotsStreamIdxRow, ? extends SnapshotsStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SnapshotsStreamIdxTrigger trigger : triggers) {
            trigger.putSnapshotsStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumn> values) {
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SnapshotsStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SnapshotsStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> get(Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SnapshotsStreamIdxRow row = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = SnapshotsStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SnapshotsStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SnapshotsStreamIdxColumnValue> getRowColumns(SnapshotsStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SnapshotsStreamIdxColumnValue> getRowColumns(SnapshotsStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SnapshotsStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = SnapshotsStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(SnapshotsStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> getRowsMultimap(Iterable<SnapshotsStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> getRowsMultimap(Iterable<SnapshotsStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> getRowsMultimapInternal(Iterable<SnapshotsStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SnapshotsStreamIdxRow row = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = SnapshotsStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SnapshotsStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SnapshotsStreamIdxRow, BatchingVisitable<SnapshotsStreamIdxColumnValue>> getRowsColumnRange(Iterable<SnapshotsStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamIdxRow, BatchingVisitable<SnapshotsStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamIdxRow row = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SnapshotsStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = SnapshotsStreamIdxColumnValue.hydrateValue(result.getValue());
                return SnapshotsStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SnapshotsStreamIdxRow, SnapshotsStreamIdxColumnValue>> getRowsColumnRange(Iterable<SnapshotsStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SnapshotsStreamIdxRow row = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = SnapshotsStreamIdxColumnValue.hydrateValue(e.getValue());
            SnapshotsStreamIdxColumnValue colValue = SnapshotsStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SnapshotsStreamIdxRow, Iterator<SnapshotsStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<SnapshotsStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamIdxRow, Iterator<SnapshotsStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamIdxRow row = SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SnapshotsStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SnapshotsStreamIdxColumn col = SnapshotsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = SnapshotsStreamIdxColumnValue.hydrateValue(result.getValue());
                return SnapshotsStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<SnapshotsStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SnapshotsStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SnapshotsStreamIdxRowResult>() {
            @Override
            public SnapshotsStreamIdxRowResult apply(RowResult<byte[]> input) {
                return SnapshotsStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "NnVQADBOCWqv0ES1aEyg7Q==";
}
