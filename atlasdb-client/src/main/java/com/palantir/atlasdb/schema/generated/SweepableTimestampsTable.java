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
public final class SweepableTimestampsTable implements
        AtlasDbDynamicMutablePersistentTable<SweepableTimestampsTable.SweepableTimestampsRow,
                                                SweepableTimestampsTable.SweepableTimestampsColumn,
                                                SweepableTimestampsTable.SweepableTimestampsColumnValue,
                                                SweepableTimestampsTable.SweepableTimestampsRowResult> {
    private final Transaction t;
    private final List<SweepableTimestampsTrigger> triggers;
    private final static String rawTableName = "sweepableTimestamps";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SweepableTimestampsTable of(Transaction t, Namespace namespace) {
        return new SweepableTimestampsTable(t, namespace, ImmutableList.<SweepableTimestampsTrigger>of());
    }

    static SweepableTimestampsTable of(Transaction t, Namespace namespace, SweepableTimestampsTrigger trigger, SweepableTimestampsTrigger... triggers) {
        return new SweepableTimestampsTable(t, namespace, ImmutableList.<SweepableTimestampsTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepableTimestampsTable of(Transaction t, Namespace namespace, List<SweepableTimestampsTrigger> triggers) {
        return new SweepableTimestampsTable(t, namespace, triggers);
    }

    private SweepableTimestampsTable(Transaction t, Namespace namespace, List<SweepableTimestampsTrigger> triggers) {
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
     * SweepableTimestampsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long shard};
     *   {@literal Long timestampPartition};
     *   {@literal byte[] sweepConservative};
     * }
     * </pre>
     */
    public static final class SweepableTimestampsRow implements Persistable, Comparable<SweepableTimestampsRow> {
        private final long hashOfRowComponents;
        private final long shard;
        private final long timestampPartition;
        private final byte[] sweepConservative;

        public static SweepableTimestampsRow of(long shard, long timestampPartition, byte[] sweepConservative) {
            long hashOfRowComponents = computeHashFirstComponents(shard);
            return new SweepableTimestampsRow(hashOfRowComponents, shard, timestampPartition, sweepConservative);
        }

        private SweepableTimestampsRow(long hashOfRowComponents, long shard, long timestampPartition, byte[] sweepConservative) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.shard = shard;
            this.timestampPartition = timestampPartition;
            this.sweepConservative = sweepConservative;
        }

        public long getShard() {
            return shard;
        }

        public long getTimestampPartition() {
            return timestampPartition;
        }

        public byte[] getSweepConservative() {
            return sweepConservative;
        }

        public static Function<SweepableTimestampsRow, Long> getShardFun() {
            return new Function<SweepableTimestampsRow, Long>() {
                @Override
                public Long apply(SweepableTimestampsRow row) {
                    return row.shard;
                }
            };
        }

        public static Function<SweepableTimestampsRow, Long> getTimestampPartitionFun() {
            return new Function<SweepableTimestampsRow, Long>() {
                @Override
                public Long apply(SweepableTimestampsRow row) {
                    return row.timestampPartition;
                }
            };
        }

        public static Function<SweepableTimestampsRow, byte[]> getSweepConservativeFun() {
            return new Function<SweepableTimestampsRow, byte[]>() {
                @Override
                public byte[] apply(SweepableTimestampsRow row) {
                    return row.sweepConservative;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] shardBytes = EncodingUtils.encodeUnsignedVarLong(shard);
            byte[] timestampPartitionBytes = EncodingUtils.encodeUnsignedVarLong(timestampPartition);
            byte[] sweepConservativeBytes = sweepConservative;
            return EncodingUtils.add(hashOfRowComponentsBytes, shardBytes, timestampPartitionBytes, sweepConservativeBytes);
        }

        public static final Hydrator<SweepableTimestampsRow> BYTES_HYDRATOR = new Hydrator<SweepableTimestampsRow>() {
            @Override
            public SweepableTimestampsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long shard = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(shard);
                Long timestampPartition = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(timestampPartition);
                byte[] sweepConservative = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new SweepableTimestampsRow(hashOfRowComponents, shard, timestampPartition, sweepConservative);
            }
        };

        public static long computeHashFirstComponents(long shard) {
            byte[] shardBytes = EncodingUtils.encodeUnsignedVarLong(shard);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(shardBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("shard", shard)
                .add("timestampPartition", timestampPartition)
                .add("sweepConservative", sweepConservative)
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
            SweepableTimestampsRow other = (SweepableTimestampsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(shard, other.shard) && Objects.equals(timestampPartition, other.timestampPartition) && Arrays.equals(sweepConservative, other.sweepConservative);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, shard, timestampPartition, sweepConservative });
        }

        @Override
        public int compareTo(SweepableTimestampsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.shard, o.shard)
                .compare(this.timestampPartition, o.timestampPartition)
                .compare(this.sweepConservative, o.sweepConservative, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    /**
     * <pre>
     * SweepableTimestampsColumn {
     *   {@literal Long timestampModulus};
     * }
     * </pre>
     */
    public static final class SweepableTimestampsColumn implements Persistable, Comparable<SweepableTimestampsColumn> {
        private final long timestampModulus;

        public static SweepableTimestampsColumn of(long timestampModulus) {
            return new SweepableTimestampsColumn(timestampModulus);
        }

        private SweepableTimestampsColumn(long timestampModulus) {
            this.timestampModulus = timestampModulus;
        }

        public long getTimestampModulus() {
            return timestampModulus;
        }

        public static Function<SweepableTimestampsColumn, Long> getTimestampModulusFun() {
            return new Function<SweepableTimestampsColumn, Long>() {
                @Override
                public Long apply(SweepableTimestampsColumn row) {
                    return row.timestampModulus;
                }
            };
        }

        public static Function<Long, SweepableTimestampsColumn> fromTimestampModulusFun() {
            return new Function<Long, SweepableTimestampsColumn>() {
                @Override
                public SweepableTimestampsColumn apply(Long row) {
                    return SweepableTimestampsColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] timestampModulusBytes = EncodingUtils.encodeUnsignedVarLong(timestampModulus);
            return EncodingUtils.add(timestampModulusBytes);
        }

        public static final Hydrator<SweepableTimestampsColumn> BYTES_HYDRATOR = new Hydrator<SweepableTimestampsColumn>() {
            @Override
            public SweepableTimestampsColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long timestampModulus = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(timestampModulus);
                return new SweepableTimestampsColumn(timestampModulus);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("timestampModulus", timestampModulus)
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
            SweepableTimestampsColumn other = (SweepableTimestampsColumn) obj;
            return Objects.equals(timestampModulus, other.timestampModulus);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(timestampModulus);
        }

        @Override
        public int compareTo(SweepableTimestampsColumn o) {
            return ComparisonChain.start()
                .compare(this.timestampModulus, o.timestampModulus)
                .result();
        }
    }

    public interface SweepableTimestampsTrigger {
        public void putSweepableTimestamps(Multimap<SweepableTimestampsRow, ? extends SweepableTimestampsColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long timestampModulus};
     * }
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class SweepableTimestampsColumnValue implements ColumnValue<byte[]> {
        private final SweepableTimestampsColumn columnName;
        private final byte[] value;

        public static SweepableTimestampsColumnValue of(SweepableTimestampsColumn columnName, byte[] value) {
            return new SweepableTimestampsColumnValue(columnName, value);
        }

        private SweepableTimestampsColumnValue(SweepableTimestampsColumn columnName, byte[] value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SweepableTimestampsColumn getColumnName() {
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

        public static Function<SweepableTimestampsColumnValue, SweepableTimestampsColumn> getColumnNameFun() {
            return new Function<SweepableTimestampsColumnValue, SweepableTimestampsColumn>() {
                @Override
                public SweepableTimestampsColumn apply(SweepableTimestampsColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SweepableTimestampsColumnValue, byte[]> getValueFun() {
            return new Function<SweepableTimestampsColumnValue, byte[]>() {
                @Override
                public byte[] apply(SweepableTimestampsColumnValue columnValue) {
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

    public static final class SweepableTimestampsRowResult implements TypedRowResult {
        private final SweepableTimestampsRow rowName;
        private final ImmutableSet<SweepableTimestampsColumnValue> columnValues;

        public static SweepableTimestampsRowResult of(RowResult<byte[]> rowResult) {
            SweepableTimestampsRow rowName = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SweepableTimestampsColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] value = SweepableTimestampsColumnValue.hydrateValue(e.getValue());
                columnValues.add(SweepableTimestampsColumnValue.of(col, value));
            }
            return new SweepableTimestampsRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SweepableTimestampsRowResult(SweepableTimestampsRow rowName, ImmutableSet<SweepableTimestampsColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SweepableTimestampsRow getRowName() {
            return rowName;
        }

        public Set<SweepableTimestampsColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SweepableTimestampsRowResult, SweepableTimestampsRow> getRowNameFun() {
            return new Function<SweepableTimestampsRowResult, SweepableTimestampsRow>() {
                @Override
                public SweepableTimestampsRow apply(SweepableTimestampsRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SweepableTimestampsRowResult, ImmutableSet<SweepableTimestampsColumnValue>> getColumnValuesFun() {
            return new Function<SweepableTimestampsRowResult, ImmutableSet<SweepableTimestampsColumnValue>>() {
                @Override
                public ImmutableSet<SweepableTimestampsColumnValue> apply(SweepableTimestampsRowResult rowResult) {
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
    public void delete(SweepableTimestampsRow row, SweepableTimestampsColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SweepableTimestampsRow> rows) {
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumn> toRemove = HashMultimap.create();
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> result = getRowsMultimap(rows);
        for (Entry<SweepableTimestampsRow, SweepableTimestampsColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SweepableTimestampsRow, SweepableTimestampsColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SweepableTimestampsRow rowName, Iterable<SweepableTimestampsColumnValue> values) {
        put(ImmutableMultimap.<SweepableTimestampsRow, SweepableTimestampsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SweepableTimestampsRow rowName, SweepableTimestampsColumnValue... values) {
        put(ImmutableMultimap.<SweepableTimestampsRow, SweepableTimestampsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SweepableTimestampsRow, ? extends SweepableTimestampsColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SweepableTimestampsTrigger trigger : triggers) {
            trigger.putSweepableTimestamps(values);
        }
    }

    @Override
    public void touch(Multimap<SweepableTimestampsRow, SweepableTimestampsColumn> values) {
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SweepableTimestampsRow, SweepableTimestampsColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SweepableTimestampsColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SweepableTimestampsColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> get(Multimap<SweepableTimestampsRow, SweepableTimestampsColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                byte[] val = SweepableTimestampsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepableTimestampsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SweepableTimestampsColumnValue> getRowColumns(SweepableTimestampsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepableTimestampsColumnValue> getRowColumns(SweepableTimestampsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepableTimestampsColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepableTimestampsColumnValue.hydrateValue(e.getValue());
                ret.add(SweepableTimestampsColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> getRowsMultimap(Iterable<SweepableTimestampsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> getRowsMultimap(Iterable<SweepableTimestampsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> getRowsMultimapInternal(Iterable<SweepableTimestampsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepableTimestampsRow, SweepableTimestampsColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = SweepableTimestampsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepableTimestampsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepableTimestampsRow, BatchingVisitable<SweepableTimestampsColumnValue>> getRowsColumnRange(Iterable<SweepableTimestampsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepableTimestampsRow, BatchingVisitable<SweepableTimestampsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepableTimestampsColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepableTimestampsColumnValue.hydrateValue(result.getValue());
                return SweepableTimestampsColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepableTimestampsRow, SweepableTimestampsColumnValue>> getRowsColumnRange(Iterable<SweepableTimestampsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            byte[] val = SweepableTimestampsColumnValue.hydrateValue(e.getValue());
            SweepableTimestampsColumnValue colValue = SweepableTimestampsColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepableTimestampsRow, Iterator<SweepableTimestampsColumnValue>> getRowsColumnRangeIterator(Iterable<SweepableTimestampsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepableTimestampsRow, Iterator<SweepableTimestampsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepableTimestampsColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SweepableTimestampsColumn col = SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = SweepableTimestampsColumnValue.hydrateValue(result.getValue());
                return SweepableTimestampsColumnValue.of(col, val);
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

    public BatchingVisitableView<SweepableTimestampsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepableTimestampsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepableTimestampsRowResult>() {
            @Override
            public SweepableTimestampsRowResult apply(RowResult<byte[]> input) {
                return SweepableTimestampsRowResult.of(input);
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
    static String __CLASS_HASH = "ZldNHckg6Jn99/m/D/IS/w==";
}
