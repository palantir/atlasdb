package com.palantir.atlasdb.schema.generated;

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
public final class SweepIdToNameTable implements
        AtlasDbDynamicMutablePersistentTable<SweepIdToNameTable.SweepIdToNameRow,
                                                SweepIdToNameTable.SweepIdToNameColumn,
                                                SweepIdToNameTable.SweepIdToNameColumnValue,
                                                SweepIdToNameTable.SweepIdToNameRowResult> {
    private final Transaction t;
    private final List<SweepIdToNameTrigger> triggers;
    private final static String rawTableName = "sweepIdToName";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SweepIdToNameTable of(Transaction t, Namespace namespace) {
        return new SweepIdToNameTable(t, namespace, ImmutableList.<SweepIdToNameTrigger>of());
    }

    static SweepIdToNameTable of(Transaction t, Namespace namespace, SweepIdToNameTrigger trigger, SweepIdToNameTrigger... triggers) {
        return new SweepIdToNameTable(t, namespace, ImmutableList.<SweepIdToNameTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepIdToNameTable of(Transaction t, Namespace namespace, List<SweepIdToNameTrigger> triggers) {
        return new SweepIdToNameTable(t, namespace, triggers);
    }

    private SweepIdToNameTable(Transaction t, Namespace namespace, List<SweepIdToNameTrigger> triggers) {
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
     * SweepIdToNameRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal String singleton};
     * }
     * </pre>
     */
    public static final class SweepIdToNameRow implements Persistable, Comparable<SweepIdToNameRow> {
        private final long hashOfRowComponents;
        private final String singleton;

        public static SweepIdToNameRow of(String singleton) {
            long hashOfRowComponents = computeHashFirstComponents(singleton);
            return new SweepIdToNameRow(hashOfRowComponents, singleton);
        }

        private SweepIdToNameRow(long hashOfRowComponents, String singleton) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.singleton = singleton;
        }

        public String getSingleton() {
            return singleton;
        }

        public static Function<SweepIdToNameRow, String> getSingletonFun() {
            return new Function<SweepIdToNameRow, String>() {
                @Override
                public String apply(SweepIdToNameRow row) {
                    return row.singleton;
                }
            };
        }

        public static Function<String, SweepIdToNameRow> fromSingletonFun() {
            return new Function<String, SweepIdToNameRow>() {
                @Override
                public SweepIdToNameRow apply(String row) {
                    return SweepIdToNameRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] singletonBytes = PtBytes.toBytes(singleton);
            return EncodingUtils.add(hashOfRowComponentsBytes, singletonBytes);
        }

        public static final Hydrator<SweepIdToNameRow> BYTES_HYDRATOR = new Hydrator<SweepIdToNameRow>() {
            @Override
            public SweepIdToNameRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String singleton = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SweepIdToNameRow(hashOfRowComponents, singleton);
            }
        };

        public static long computeHashFirstComponents(String singleton) {
            byte[] singletonBytes = PtBytes.toBytes(singleton);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(singletonBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("singleton", singleton)
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
            SweepIdToNameRow other = (SweepIdToNameRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(singleton, other.singleton);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, singleton });
        }

        @Override
        public int compareTo(SweepIdToNameRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.singleton, o.singleton)
                .result();
        }
    }

    /**
     * <pre>
     * SweepIdToNameColumn {
     *   {@literal @Descending Long tableId};
     * }
     * </pre>
     */
    public static final class SweepIdToNameColumn implements Persistable, Comparable<SweepIdToNameColumn> {
        private final long tableId;

        public static SweepIdToNameColumn of(long tableId) {
            return new SweepIdToNameColumn(tableId);
        }

        private SweepIdToNameColumn(long tableId) {
            this.tableId = tableId;
        }

        public long getTableId() {
            return tableId;
        }

        public static Function<SweepIdToNameColumn, Long> getTableIdFun() {
            return new Function<SweepIdToNameColumn, Long>() {
                @Override
                public Long apply(SweepIdToNameColumn row) {
                    return row.tableId;
                }
            };
        }

        public static Function<Long, SweepIdToNameColumn> fromTableIdFun() {
            return new Function<Long, SweepIdToNameColumn>() {
                @Override
                public SweepIdToNameColumn apply(Long row) {
                    return SweepIdToNameColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] tableIdBytes = EncodingUtils.encodeUnsignedVarLong(tableId);
            EncodingUtils.flipAllBitsInPlace(tableIdBytes);
            return EncodingUtils.add(tableIdBytes);
        }

        public static final Hydrator<SweepIdToNameColumn> BYTES_HYDRATOR = new Hydrator<SweepIdToNameColumn>() {
            @Override
            public SweepIdToNameColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long tableId = EncodingUtils.decodeFlippedUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(tableId);
                return new SweepIdToNameColumn(tableId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("tableId", tableId)
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
            SweepIdToNameColumn other = (SweepIdToNameColumn) obj;
            return Objects.equals(tableId, other.tableId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(tableId);
        }

        @Override
        public int compareTo(SweepIdToNameColumn o) {
            return ComparisonChain.start()
                .compare(this.tableId, o.tableId)
                .result();
        }
    }

    public interface SweepIdToNameTrigger {
        public void putSweepIdToName(Multimap<SweepIdToNameRow, ? extends SweepIdToNameColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal @Descending Long tableId};
     * }
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class SweepIdToNameColumnValue implements ColumnValue<String> {
        private final SweepIdToNameColumn columnName;
        private final String value;

        public static SweepIdToNameColumnValue of(SweepIdToNameColumn columnName, String value) {
            return new SweepIdToNameColumnValue(columnName, value);
        }

        private SweepIdToNameColumnValue(SweepIdToNameColumn columnName, String value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SweepIdToNameColumn getColumnName() {
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

        public static Function<SweepIdToNameColumnValue, SweepIdToNameColumn> getColumnNameFun() {
            return new Function<SweepIdToNameColumnValue, SweepIdToNameColumn>() {
                @Override
                public SweepIdToNameColumn apply(SweepIdToNameColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SweepIdToNameColumnValue, String> getValueFun() {
            return new Function<SweepIdToNameColumnValue, String>() {
                @Override
                public String apply(SweepIdToNameColumnValue columnValue) {
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

    public static final class SweepIdToNameRowResult implements TypedRowResult {
        private final SweepIdToNameRow rowName;
        private final ImmutableSet<SweepIdToNameColumnValue> columnValues;

        public static SweepIdToNameRowResult of(RowResult<byte[]> rowResult) {
            SweepIdToNameRow rowName = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SweepIdToNameColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String value = SweepIdToNameColumnValue.hydrateValue(e.getValue());
                columnValues.add(SweepIdToNameColumnValue.of(col, value));
            }
            return new SweepIdToNameRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SweepIdToNameRowResult(SweepIdToNameRow rowName, ImmutableSet<SweepIdToNameColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SweepIdToNameRow getRowName() {
            return rowName;
        }

        public Set<SweepIdToNameColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SweepIdToNameRowResult, SweepIdToNameRow> getRowNameFun() {
            return new Function<SweepIdToNameRowResult, SweepIdToNameRow>() {
                @Override
                public SweepIdToNameRow apply(SweepIdToNameRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SweepIdToNameRowResult, ImmutableSet<SweepIdToNameColumnValue>> getColumnValuesFun() {
            return new Function<SweepIdToNameRowResult, ImmutableSet<SweepIdToNameColumnValue>>() {
                @Override
                public ImmutableSet<SweepIdToNameColumnValue> apply(SweepIdToNameRowResult rowResult) {
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
    public void delete(SweepIdToNameRow row, SweepIdToNameColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SweepIdToNameRow> rows) {
        Multimap<SweepIdToNameRow, SweepIdToNameColumn> toRemove = HashMultimap.create();
        Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> result = getRowsMultimap(rows);
        for (Entry<SweepIdToNameRow, SweepIdToNameColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SweepIdToNameRow, SweepIdToNameColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SweepIdToNameRow rowName, Iterable<SweepIdToNameColumnValue> values) {
        put(ImmutableMultimap.<SweepIdToNameRow, SweepIdToNameColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SweepIdToNameRow rowName, SweepIdToNameColumnValue... values) {
        put(ImmutableMultimap.<SweepIdToNameRow, SweepIdToNameColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SweepIdToNameRow, ? extends SweepIdToNameColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SweepIdToNameTrigger trigger : triggers) {
            trigger.putSweepIdToName(values);
        }
    }

    @Override
    public void touch(Multimap<SweepIdToNameRow, SweepIdToNameColumn> values) {
        Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SweepIdToNameRow, SweepIdToNameColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SweepIdToNameRow, SweepIdToNameColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SweepIdToNameColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SweepIdToNameColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> get(Multimap<SweepIdToNameRow, SweepIdToNameColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SweepIdToNameRow row = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                String val = SweepIdToNameColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepIdToNameColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SweepIdToNameColumnValue> getRowColumns(SweepIdToNameRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepIdToNameColumnValue> getRowColumns(SweepIdToNameRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepIdToNameColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = SweepIdToNameColumnValue.hydrateValue(e.getValue());
                ret.add(SweepIdToNameColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> getRowsMultimap(Iterable<SweepIdToNameRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> getRowsMultimap(Iterable<SweepIdToNameRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> getRowsMultimapInternal(Iterable<SweepIdToNameRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepIdToNameRow, SweepIdToNameColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepIdToNameRow row = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = SweepIdToNameColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepIdToNameColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepIdToNameRow, BatchingVisitable<SweepIdToNameColumnValue>> getRowsColumnRange(Iterable<SweepIdToNameRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepIdToNameRow, BatchingVisitable<SweepIdToNameColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepIdToNameRow row = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepIdToNameColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = SweepIdToNameColumnValue.hydrateValue(result.getValue());
                return SweepIdToNameColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepIdToNameRow, SweepIdToNameColumnValue>> getRowsColumnRange(Iterable<SweepIdToNameRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepIdToNameRow row = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            String val = SweepIdToNameColumnValue.hydrateValue(e.getValue());
            SweepIdToNameColumnValue colValue = SweepIdToNameColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepIdToNameRow, Iterator<SweepIdToNameColumnValue>> getRowsColumnRangeIterator(Iterable<SweepIdToNameRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepIdToNameRow, Iterator<SweepIdToNameColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepIdToNameRow row = SweepIdToNameRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepIdToNameColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SweepIdToNameColumn col = SweepIdToNameColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = SweepIdToNameColumnValue.hydrateValue(result.getValue());
                return SweepIdToNameColumnValue.of(col, val);
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

    public BatchingVisitableView<SweepIdToNameRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepIdToNameRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepIdToNameRowResult>() {
            @Override
            public SweepIdToNameRowResult apply(RowResult<byte[]> input) {
                return SweepIdToNameRowResult.of(input);
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
    static String __CLASS_HASH = "EOZ88gJXh1tXBjNBFJIO0g==";
}
