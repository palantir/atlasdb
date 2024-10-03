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
public final class SweepAssignedBucketsTable implements
        AtlasDbDynamicMutablePersistentTable<SweepAssignedBucketsTable.SweepAssignedBucketsRow,
                                                SweepAssignedBucketsTable.SweepAssignedBucketsColumn,
                                                SweepAssignedBucketsTable.SweepAssignedBucketsColumnValue,
                                                SweepAssignedBucketsTable.SweepAssignedBucketsRowResult> {
    private final Transaction t;
    private final List<SweepAssignedBucketsTrigger> triggers;
    private final static String rawTableName = "sweepAssignedBuckets";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SweepAssignedBucketsTable of(Transaction t, Namespace namespace) {
        return new SweepAssignedBucketsTable(t, namespace, ImmutableList.<SweepAssignedBucketsTrigger>of());
    }

    static SweepAssignedBucketsTable of(Transaction t, Namespace namespace, SweepAssignedBucketsTrigger trigger, SweepAssignedBucketsTrigger... triggers) {
        return new SweepAssignedBucketsTable(t, namespace, ImmutableList.<SweepAssignedBucketsTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepAssignedBucketsTable of(Transaction t, Namespace namespace, List<SweepAssignedBucketsTrigger> triggers) {
        return new SweepAssignedBucketsTable(t, namespace, triggers);
    }

    private SweepAssignedBucketsTable(Transaction t, Namespace namespace, List<SweepAssignedBucketsTrigger> triggers) {
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
     * SweepAssignedBucketsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long shard};
     *   {@literal Long majorBucketIdentifier};
     *   {@literal byte[] strategy};
     * }
     * </pre>
     */
    public static final class SweepAssignedBucketsRow implements Persistable, Comparable<SweepAssignedBucketsRow> {
        private final long hashOfRowComponents;
        private final long shard;
        private final long majorBucketIdentifier;
        private final byte[] strategy;

        public static SweepAssignedBucketsRow of(long shard, long majorBucketIdentifier, byte[] strategy) {
            long hashOfRowComponents = computeHashFirstComponents(shard, majorBucketIdentifier, strategy);
            return new SweepAssignedBucketsRow(hashOfRowComponents, shard, majorBucketIdentifier, strategy);
        }

        private SweepAssignedBucketsRow(long hashOfRowComponents, long shard, long majorBucketIdentifier, byte[] strategy) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.shard = shard;
            this.majorBucketIdentifier = majorBucketIdentifier;
            this.strategy = strategy;
        }

        public long getShard() {
            return shard;
        }

        public long getMajorBucketIdentifier() {
            return majorBucketIdentifier;
        }

        public byte[] getStrategy() {
            return strategy;
        }

        public static Function<SweepAssignedBucketsRow, Long> getShardFun() {
            return new Function<SweepAssignedBucketsRow, Long>() {
                @Override
                public Long apply(SweepAssignedBucketsRow row) {
                    return row.shard;
                }
            };
        }

        public static Function<SweepAssignedBucketsRow, Long> getMajorBucketIdentifierFun() {
            return new Function<SweepAssignedBucketsRow, Long>() {
                @Override
                public Long apply(SweepAssignedBucketsRow row) {
                    return row.majorBucketIdentifier;
                }
            };
        }

        public static Function<SweepAssignedBucketsRow, byte[]> getStrategyFun() {
            return new Function<SweepAssignedBucketsRow, byte[]>() {
                @Override
                public byte[] apply(SweepAssignedBucketsRow row) {
                    return row.strategy;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] shardBytes = EncodingUtils.encodeSignedVarLong(shard);
            byte[] majorBucketIdentifierBytes = EncodingUtils.encodeSignedVarLong(majorBucketIdentifier);
            byte[] strategyBytes = strategy;
            return EncodingUtils.add(hashOfRowComponentsBytes, shardBytes, majorBucketIdentifierBytes, strategyBytes);
        }

        public static final Hydrator<SweepAssignedBucketsRow> BYTES_HYDRATOR = new Hydrator<SweepAssignedBucketsRow>() {
            @Override
            public SweepAssignedBucketsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                long shard = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(shard);
                long majorBucketIdentifier = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(majorBucketIdentifier);
                byte[] strategy = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                return new SweepAssignedBucketsRow(hashOfRowComponents, shard, majorBucketIdentifier, strategy);
            }
        };

        public static long computeHashFirstComponents(long shard, long majorBucketIdentifier, byte[] strategy) {
            byte[] shardBytes = EncodingUtils.encodeSignedVarLong(shard);
            byte[] majorBucketIdentifierBytes = EncodingUtils.encodeSignedVarLong(majorBucketIdentifier);
            byte[] strategyBytes = strategy;
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(shardBytes, majorBucketIdentifierBytes, strategyBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("shard", shard)
                .add("majorBucketIdentifier", majorBucketIdentifier)
                .add("strategy", strategy)
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
            SweepAssignedBucketsRow other = (SweepAssignedBucketsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(shard, other.shard) && Objects.equals(majorBucketIdentifier, other.majorBucketIdentifier) && Arrays.equals(strategy, other.strategy);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, shard, majorBucketIdentifier, strategy });
        }

        @Override
        public int compareTo(SweepAssignedBucketsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.shard, o.shard)
                .compare(this.majorBucketIdentifier, o.majorBucketIdentifier)
                .compare(this.strategy, o.strategy, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    /**
     * <pre>
     * SweepAssignedBucketsColumn {
     *   {@literal Long minorBucketIdentifier};
     * }
     * </pre>
     */
    public static final class SweepAssignedBucketsColumn implements Persistable, Comparable<SweepAssignedBucketsColumn> {
        private final long minorBucketIdentifier;

        public static SweepAssignedBucketsColumn of(long minorBucketIdentifier) {
            return new SweepAssignedBucketsColumn(minorBucketIdentifier);
        }

        private SweepAssignedBucketsColumn(long minorBucketIdentifier) {
            this.minorBucketIdentifier = minorBucketIdentifier;
        }

        public long getMinorBucketIdentifier() {
            return minorBucketIdentifier;
        }

        public static Function<SweepAssignedBucketsColumn, Long> getMinorBucketIdentifierFun() {
            return new Function<SweepAssignedBucketsColumn, Long>() {
                @Override
                public Long apply(SweepAssignedBucketsColumn row) {
                    return row.minorBucketIdentifier;
                }
            };
        }

        public static Function<Long, SweepAssignedBucketsColumn> fromMinorBucketIdentifierFun() {
            return new Function<Long, SweepAssignedBucketsColumn>() {
                @Override
                public SweepAssignedBucketsColumn apply(Long row) {
                    return SweepAssignedBucketsColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] minorBucketIdentifierBytes = EncodingUtils.encodeSignedVarLong(minorBucketIdentifier);
            return EncodingUtils.add(minorBucketIdentifierBytes);
        }

        public static final Hydrator<SweepAssignedBucketsColumn> BYTES_HYDRATOR = new Hydrator<SweepAssignedBucketsColumn>() {
            @Override
            public SweepAssignedBucketsColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long minorBucketIdentifier = EncodingUtils.decodeSignedVarLong(__input, __index);
                return new SweepAssignedBucketsColumn(minorBucketIdentifier);
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
            SweepAssignedBucketsColumn other = (SweepAssignedBucketsColumn) obj;
            return Objects.equals(minorBucketIdentifier, other.minorBucketIdentifier);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(minorBucketIdentifier);
        }

        @Override
        public int compareTo(SweepAssignedBucketsColumn o) {
            return ComparisonChain.start()
                .compare(this.minorBucketIdentifier, o.minorBucketIdentifier)
                .result();
        }
    }

    public interface SweepAssignedBucketsTrigger {
        public void putSweepAssignedBuckets(Multimap<SweepAssignedBucketsRow, ? extends SweepAssignedBucketsColumnValue> newRows);
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
    public static final class SweepAssignedBucketsColumnValue implements ColumnValue<byte[]> {
        private final SweepAssignedBucketsColumn columnName;
        private final byte[] value;

        public static SweepAssignedBucketsColumnValue of(SweepAssignedBucketsColumn columnName, byte[] value) {
            return new SweepAssignedBucketsColumnValue(columnName, value);
        }

        private SweepAssignedBucketsColumnValue(SweepAssignedBucketsColumn columnName, byte[] value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SweepAssignedBucketsColumn getColumnName() {
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

        public static Function<SweepAssignedBucketsColumnValue, SweepAssignedBucketsColumn> getColumnNameFun() {
            return new Function<SweepAssignedBucketsColumnValue, SweepAssignedBucketsColumn>() {
                @Override
                public SweepAssignedBucketsColumn apply(SweepAssignedBucketsColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SweepAssignedBucketsColumnValue, byte[]> getValueFun() {
            return new Function<SweepAssignedBucketsColumnValue, byte[]>() {
                @Override
                public byte[] apply(SweepAssignedBucketsColumnValue columnValue) {
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

    public static final class SweepAssignedBucketsRowResult implements TypedRowResult {
        private final SweepAssignedBucketsRow rowName;
        private final ImmutableSet<SweepAssignedBucketsColumnValue> columnValues;

        public static SweepAssignedBucketsRowResult of(RowResult<byte[]> rowResult) {
            SweepAssignedBucketsRow rowName = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SweepAssignedBucketsColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] value = SweepAssignedBucketsColumnValue.hydrateValue(e.getValue());
                columnValues.add(SweepAssignedBucketsColumnValue.of(col, value));
            }
            return new SweepAssignedBucketsRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SweepAssignedBucketsRowResult(SweepAssignedBucketsRow rowName, ImmutableSet<SweepAssignedBucketsColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SweepAssignedBucketsRow getRowName() {
            return rowName;
        }

        public Set<SweepAssignedBucketsColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SweepAssignedBucketsRowResult, SweepAssignedBucketsRow> getRowNameFun() {
            return new Function<SweepAssignedBucketsRowResult, SweepAssignedBucketsRow>() {
                @Override
                public SweepAssignedBucketsRow apply(SweepAssignedBucketsRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SweepAssignedBucketsRowResult, ImmutableSet<SweepAssignedBucketsColumnValue>> getColumnValuesFun() {
            return new Function<SweepAssignedBucketsRowResult, ImmutableSet<SweepAssignedBucketsColumnValue>>() {
                @Override
                public ImmutableSet<SweepAssignedBucketsColumnValue> apply(SweepAssignedBucketsRowResult rowResult) {
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
    public void delete(SweepAssignedBucketsRow row, SweepAssignedBucketsColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SweepAssignedBucketsRow> rows) {
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumn> toRemove = HashMultimap.create();
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> result = getRowsMultimap(rows);
        for (Entry<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SweepAssignedBucketsRow rowName, Iterable<SweepAssignedBucketsColumnValue> values) {
        put(ImmutableMultimap.<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SweepAssignedBucketsRow rowName, SweepAssignedBucketsColumnValue... values) {
        put(ImmutableMultimap.<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SweepAssignedBucketsRow, ? extends SweepAssignedBucketsColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SweepAssignedBucketsTrigger trigger : triggers) {
            trigger.putSweepAssignedBuckets(values);
        }
    }

    @Override
    public void touch(Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumn> values) {
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SweepAssignedBucketsColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SweepAssignedBucketsColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> get(Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> rowMap = ArrayListMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepAssignedBucketsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SweepAssignedBucketsColumnValue> getRowColumns(SweepAssignedBucketsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepAssignedBucketsColumnValue> getRowColumns(SweepAssignedBucketsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepAssignedBucketsColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(e.getValue());
                ret.add(SweepAssignedBucketsColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> getRowsMultimap(Iterable<SweepAssignedBucketsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> getRowsMultimap(Iterable<SweepAssignedBucketsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> getRowsMultimapInternal(Iterable<SweepAssignedBucketsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepAssignedBucketsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepAssignedBucketsRow, BatchingVisitable<SweepAssignedBucketsColumnValue>> getRowsColumnRange(Iterable<SweepAssignedBucketsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepAssignedBucketsRow, BatchingVisitable<SweepAssignedBucketsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepAssignedBucketsColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(result.getValue());
                return SweepAssignedBucketsColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepAssignedBucketsRow, SweepAssignedBucketsColumnValue>> getRowsColumnRange(Iterable<SweepAssignedBucketsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(e.getValue());
            SweepAssignedBucketsColumnValue colValue = SweepAssignedBucketsColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepAssignedBucketsRow, Iterator<SweepAssignedBucketsColumnValue>> getRowsColumnRangeIterator(Iterable<SweepAssignedBucketsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepAssignedBucketsRow, Iterator<SweepAssignedBucketsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepAssignedBucketsColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SweepAssignedBucketsColumn col = SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepAssignedBucketsColumnValue.hydrateValue(result.getValue());
                return SweepAssignedBucketsColumnValue.of(col, val);
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

    public BatchingVisitableView<SweepAssignedBucketsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepAssignedBucketsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepAssignedBucketsRowResult>() {
            @Override
            public SweepAssignedBucketsRowResult apply(RowResult<byte[]> input) {
                return SweepAssignedBucketsRowResult.of(input);
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
    static String __CLASS_HASH = "mhdLD3BQoWbq/pnHl1sRSA==";
}
