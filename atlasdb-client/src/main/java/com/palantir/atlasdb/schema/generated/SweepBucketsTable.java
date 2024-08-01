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

import javax.annotation.Nullable;
import javax.annotation.processing.Generated;

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
@SuppressWarnings({"deprecation"})
public final class SweepBucketsTable implements
        AtlasDbDynamicMutablePersistentTable<SweepBucketsTable.SweepBucketsRow,
                                                SweepBucketsTable.SweepBucketsColumn,
                                                SweepBucketsTable.SweepBucketsColumnValue,
                                                SweepBucketsTable.SweepBucketsRowResult> {
    private final Transaction t;
    private final List<SweepBucketsTrigger> triggers;
    private final static String rawTableName = "sweepBuckets";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SweepBucketsTable of(Transaction t, Namespace namespace) {
        return new SweepBucketsTable(t, namespace, ImmutableList.<SweepBucketsTrigger>of());
    }

    static SweepBucketsTable of(Transaction t, Namespace namespace, SweepBucketsTrigger trigger, SweepBucketsTrigger... triggers) {
        return new SweepBucketsTable(t, namespace, ImmutableList.<SweepBucketsTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepBucketsTable of(Transaction t, Namespace namespace, List<SweepBucketsTrigger> triggers) {
        return new SweepBucketsTable(t, namespace, triggers);
    }

    private SweepBucketsTable(Transaction t, Namespace namespace, List<SweepBucketsTrigger> triggers) {
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
     * SweepBucketsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long majorBucketIdentifier};
     * }
     * </pre>
     */
    public static final class SweepBucketsRow implements Persistable, Comparable<SweepBucketsRow> {
        private final long hashOfRowComponents;
        private final long majorBucketIdentifier;

        public static SweepBucketsRow of(long majorBucketIdentifier) {
            long hashOfRowComponents = computeHashFirstComponents(majorBucketIdentifier);
            return new SweepBucketsRow(hashOfRowComponents, majorBucketIdentifier);
        }

        private SweepBucketsRow(long hashOfRowComponents, long majorBucketIdentifier) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.majorBucketIdentifier = majorBucketIdentifier;
        }

        public long getMajorBucketIdentifier() {
            return majorBucketIdentifier;
        }

        public static Function<SweepBucketsRow, Long> getMajorBucketIdentifierFun() {
            return new Function<SweepBucketsRow, Long>() {
                @Override
                public Long apply(SweepBucketsRow row) {
                    return row.majorBucketIdentifier;
                }
            };
        }

        public static Function<Long, SweepBucketsRow> fromMajorBucketIdentifierFun() {
            return new Function<Long, SweepBucketsRow>() {
                @Override
                public SweepBucketsRow apply(Long row) {
                    return SweepBucketsRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] majorBucketIdentifierBytes = EncodingUtils.encodeSignedVarLong(majorBucketIdentifier);
            return EncodingUtils.add(hashOfRowComponentsBytes, majorBucketIdentifierBytes);
        }

        public static final Hydrator<SweepBucketsRow> BYTES_HYDRATOR = new Hydrator<SweepBucketsRow>() {
            @Override
            public SweepBucketsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                long majorBucketIdentifier = EncodingUtils.decodeSignedVarLong(__input, __index);
                return new SweepBucketsRow(hashOfRowComponents, majorBucketIdentifier);
            }
        };

        public static long computeHashFirstComponents(long majorBucketIdentifier) {
            byte[] majorBucketIdentifierBytes = EncodingUtils.encodeSignedVarLong(majorBucketIdentifier);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(majorBucketIdentifierBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("majorBucketIdentifier", majorBucketIdentifier)
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
            SweepBucketsRow other = (SweepBucketsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(majorBucketIdentifier, other.majorBucketIdentifier);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, majorBucketIdentifier });
        }

        @Override
        public int compareTo(SweepBucketsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.majorBucketIdentifier, o.majorBucketIdentifier)
                .result();
        }
    }

    /**
     * <pre>
     * SweepBucketsColumn {
     *   {@literal Long minorBucketIdentifier};
     * }
     * </pre>
     */
    public static final class SweepBucketsColumn implements Persistable, Comparable<SweepBucketsColumn> {
        private final long minorBucketIdentifier;

        public static SweepBucketsColumn of(long minorBucketIdentifier) {
            return new SweepBucketsColumn(minorBucketIdentifier);
        }

        private SweepBucketsColumn(long minorBucketIdentifier) {
            this.minorBucketIdentifier = minorBucketIdentifier;
        }

        public long getMinorBucketIdentifier() {
            return minorBucketIdentifier;
        }

        public static Function<SweepBucketsColumn, Long> getMinorBucketIdentifierFun() {
            return new Function<SweepBucketsColumn, Long>() {
                @Override
                public Long apply(SweepBucketsColumn row) {
                    return row.minorBucketIdentifier;
                }
            };
        }

        public static Function<Long, SweepBucketsColumn> fromMinorBucketIdentifierFun() {
            return new Function<Long, SweepBucketsColumn>() {
                @Override
                public SweepBucketsColumn apply(Long row) {
                    return SweepBucketsColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] minorBucketIdentifierBytes = EncodingUtils.encodeUnsignedVarLong(minorBucketIdentifier);
            return EncodingUtils.add(minorBucketIdentifierBytes);
        }

        public static final Hydrator<SweepBucketsColumn> BYTES_HYDRATOR = new Hydrator<SweepBucketsColumn>() {
            @Override
            public SweepBucketsColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long minorBucketIdentifier = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                return new SweepBucketsColumn(minorBucketIdentifier);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("minorBucketIdentifier", minorBucketIdentifier)
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
            SweepBucketsColumn other = (SweepBucketsColumn) obj;
            return Objects.equals(minorBucketIdentifier, other.minorBucketIdentifier);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(minorBucketIdentifier);
        }

        @Override
        public int compareTo(SweepBucketsColumn o) {
            return ComparisonChain.start()
                .compare(this.minorBucketIdentifier, o.minorBucketIdentifier)
                .result();
        }
    }

    public interface SweepBucketsTrigger {
        public void putSweepBuckets(Multimap<SweepBucketsRow, ? extends SweepBucketsColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long minorBucketIdentifier};
     * }
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class SweepBucketsColumnValue implements ColumnValue<byte[]> {
        private final SweepBucketsColumn columnName;
        private final byte[] value;

        public static SweepBucketsColumnValue of(SweepBucketsColumn columnName, byte[] value) {
            return new SweepBucketsColumnValue(columnName, value);
        }

        private SweepBucketsColumnValue(SweepBucketsColumn columnName, byte[] value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SweepBucketsColumn getColumnName() {
            return columnName;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value;
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static byte[] hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return EncodingUtils.getBytesFromOffsetToEnd(bytes, 0);
        }

        public static Function<SweepBucketsColumnValue, SweepBucketsColumn> getColumnNameFun() {
            return new Function<SweepBucketsColumnValue, SweepBucketsColumn>() {
                @Override
                public SweepBucketsColumn apply(SweepBucketsColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SweepBucketsColumnValue, byte[]> getValueFun() {
            return new Function<SweepBucketsColumnValue, byte[]>() {
                @Override
                public byte[] apply(SweepBucketsColumnValue columnValue) {
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

    public static final class SweepBucketsRowResult implements TypedRowResult {
        private final SweepBucketsRow rowName;
        private final ImmutableSet<SweepBucketsColumnValue> columnValues;

        public static SweepBucketsRowResult of(RowResult<byte[]> rowResult) {
            SweepBucketsRow rowName = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SweepBucketsColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] value = SweepBucketsColumnValue.hydrateValue(e.getValue());
                columnValues.add(SweepBucketsColumnValue.of(col, value));
            }
            return new SweepBucketsRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SweepBucketsRowResult(SweepBucketsRow rowName, ImmutableSet<SweepBucketsColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SweepBucketsRow getRowName() {
            return rowName;
        }

        public Set<SweepBucketsColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SweepBucketsRowResult, SweepBucketsRow> getRowNameFun() {
            return new Function<SweepBucketsRowResult, SweepBucketsRow>() {
                @Override
                public SweepBucketsRow apply(SweepBucketsRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SweepBucketsRowResult, ImmutableSet<SweepBucketsColumnValue>> getColumnValuesFun() {
            return new Function<SweepBucketsRowResult, ImmutableSet<SweepBucketsColumnValue>>() {
                @Override
                public ImmutableSet<SweepBucketsColumnValue> apply(SweepBucketsRowResult rowResult) {
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
    public void delete(SweepBucketsRow row, SweepBucketsColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SweepBucketsRow> rows) {
        Multimap<SweepBucketsRow, SweepBucketsColumn> toRemove = HashMultimap.create();
        Multimap<SweepBucketsRow, SweepBucketsColumnValue> result = getRowsMultimap(rows);
        for (Entry<SweepBucketsRow, SweepBucketsColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SweepBucketsRow, SweepBucketsColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SweepBucketsRow rowName, Iterable<SweepBucketsColumnValue> values) {
        put(ImmutableMultimap.<SweepBucketsRow, SweepBucketsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SweepBucketsRow rowName, SweepBucketsColumnValue... values) {
        put(ImmutableMultimap.<SweepBucketsRow, SweepBucketsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SweepBucketsRow, ? extends SweepBucketsColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SweepBucketsTrigger trigger : triggers) {
            trigger.putSweepBuckets(values);
        }
    }

    @Override
    public void touch(Multimap<SweepBucketsRow, SweepBucketsColumn> values) {
        Multimap<SweepBucketsRow, SweepBucketsColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SweepBucketsRow, SweepBucketsColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SweepBucketsRow, SweepBucketsColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SweepBucketsColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SweepBucketsColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SweepBucketsRow, SweepBucketsColumnValue> get(Multimap<SweepBucketsRow, SweepBucketsColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SweepBucketsRow, SweepBucketsColumnValue> rowMap = ArrayListMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SweepBucketsRow row = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                byte[] val = SweepBucketsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepBucketsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SweepBucketsColumnValue> getRowColumns(SweepBucketsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepBucketsColumnValue> getRowColumns(SweepBucketsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepBucketsColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepBucketsColumnValue.hydrateValue(e.getValue());
                ret.add(SweepBucketsColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepBucketsRow, SweepBucketsColumnValue> getRowsMultimap(Iterable<SweepBucketsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepBucketsRow, SweepBucketsColumnValue> getRowsMultimap(Iterable<SweepBucketsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepBucketsRow, SweepBucketsColumnValue> getRowsMultimapInternal(Iterable<SweepBucketsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepBucketsRow, SweepBucketsColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepBucketsRow, SweepBucketsColumnValue> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepBucketsRow row = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepBucketsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepBucketsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepBucketsRow, BatchingVisitable<SweepBucketsColumnValue>> getRowsColumnRange(Iterable<SweepBucketsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepBucketsRow, BatchingVisitable<SweepBucketsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepBucketsRow row = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepBucketsColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepBucketsColumnValue.hydrateValue(result.getValue());
                return SweepBucketsColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepBucketsRow, SweepBucketsColumnValue>> getRowsColumnRange(Iterable<SweepBucketsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepBucketsRow row = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            byte[] val = SweepBucketsColumnValue.hydrateValue(e.getValue());
            SweepBucketsColumnValue colValue = SweepBucketsColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepBucketsRow, Iterator<SweepBucketsColumnValue>> getRowsColumnRangeIterator(Iterable<SweepBucketsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepBucketsRow, Iterator<SweepBucketsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepBucketsRow row = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepBucketsColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SweepBucketsColumn col = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepBucketsColumnValue.hydrateValue(result.getValue());
                return SweepBucketsColumnValue.of(col, val);
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

    public BatchingVisitableView<SweepBucketsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepBucketsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepBucketsRowResult>() {
            @Override
            public SweepBucketsRowResult apply(RowResult<byte[]> input) {
                return SweepBucketsRowResult.of(input);
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
     * {@link Nullable}
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
    static String __CLASS_HASH = "pJEldhHFTzBLT6Oj00LhTQ==";
}
