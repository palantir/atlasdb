package com.palantir.atlasdb.schema.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
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
@SuppressWarnings("all")
public final class SweepableTimestampsTable implements
        AtlasDbMutablePersistentTable<SweepableTimestampsTable.SweepableTimestampsRow,
                                         SweepableTimestampsTable.SweepableTimestampsNamedColumnValue<?>,
                                         SweepableTimestampsTable.SweepableTimestampsRowResult>,
        AtlasDbNamedMutableTable<SweepableTimestampsTable.SweepableTimestampsRow,
                                    SweepableTimestampsTable.SweepableTimestampsNamedColumnValue<?>,
                                    SweepableTimestampsTable.SweepableTimestampsRowResult> {
    private final Transaction t;
    private final List<SweepableTimestampsTrigger> triggers;
    private final static String rawTableName = "sweepableTimestamps";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SweepableTimestampsNamedColumn.values());

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
     *   {@literal String sweepMode};
     * }
     * </pre>
     */
    public static final class SweepableTimestampsRow implements Persistable, Comparable<SweepableTimestampsRow> {
        private final long hashOfRowComponents;
        private final long shard;
        private final long timestampPartition;
        private final String sweepMode;

        public static SweepableTimestampsRow of(long shard, long timestampPartition, String sweepMode) {
            long hashOfRowComponents = computeHashFirstComponents(shard);
            return new SweepableTimestampsRow(hashOfRowComponents, shard, timestampPartition, sweepMode);
        }

        private SweepableTimestampsRow(long hashOfRowComponents, long shard, long timestampPartition, String sweepMode) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.shard = shard;
            this.timestampPartition = timestampPartition;
            this.sweepMode = sweepMode;
        }

        public long getShard() {
            return shard;
        }

        public long getTimestampPartition() {
            return timestampPartition;
        }

        public String getSweepMode() {
            return sweepMode;
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

        public static Function<SweepableTimestampsRow, String> getSweepModeFun() {
            return new Function<SweepableTimestampsRow, String>() {
                @Override
                public String apply(SweepableTimestampsRow row) {
                    return row.sweepMode;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] shardBytes = EncodingUtils.encodeUnsignedVarLong(shard);
            byte[] timestampPartitionBytes = EncodingUtils.encodeUnsignedVarLong(timestampPartition);
            byte[] sweepModeBytes = PtBytes.toBytes(sweepMode);
            return EncodingUtils.add(hashOfRowComponentsBytes, shardBytes, timestampPartitionBytes, sweepModeBytes);
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
                String sweepMode = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SweepableTimestampsRow(hashOfRowComponents, shard, timestampPartition, sweepMode);
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
                .add("sweepMode", sweepMode)
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
            return Objects.equal(hashOfRowComponents, other.hashOfRowComponents) && Objects.equal(shard, other.shard) && Objects.equal(timestampPartition, other.timestampPartition) && Objects.equal(sweepMode, other.sweepMode);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, shard, timestampPartition, sweepMode });
        }

        @Override
        public int compareTo(SweepableTimestampsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.shard, o.shard)
                .compare(this.timestampPartition, o.timestampPartition)
                .compare(this.sweepMode, o.sweepMode)
                .result();
        }
    }

    public interface SweepableTimestampsNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class TimestampModulus implements SweepableTimestampsNamedColumnValue<Long> {
        private final Long value;

        public static TimestampModulus of(Long value) {
            return new TimestampModulus(value);
        }

        private TimestampModulus(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "timestamp_modulus";
        }

        @Override
        public String getShortColumnName() {
            return "ts_mod";
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
            return PtBytes.toCachedBytes("ts_mod");
        }

        public static final Hydrator<TimestampModulus> BYTES_HYDRATOR = new Hydrator<TimestampModulus>() {
            @Override
            public TimestampModulus hydrateFromBytes(byte[] bytes) {
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

    public interface SweepableTimestampsTrigger {
        public void putSweepableTimestamps(Multimap<SweepableTimestampsRow, ? extends SweepableTimestampsNamedColumnValue<?>> newRows);
    }

    public static final class SweepableTimestampsRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SweepableTimestampsRowResult of(RowResult<byte[]> row) {
            return new SweepableTimestampsRowResult(row);
        }

        private SweepableTimestampsRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SweepableTimestampsRow getRowName() {
            return SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SweepableTimestampsRowResult, SweepableTimestampsRow> getRowNameFun() {
            return new Function<SweepableTimestampsRowResult, SweepableTimestampsRow>() {
                @Override
                public SweepableTimestampsRow apply(SweepableTimestampsRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SweepableTimestampsRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SweepableTimestampsRowResult>() {
                @Override
                public SweepableTimestampsRowResult apply(RowResult<byte[]> rowResult) {
                    return new SweepableTimestampsRowResult(rowResult);
                }
            };
        }

        public boolean hasTimestampModulus() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("ts_mod"));
        }

        public Long getTimestampModulus() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("ts_mod"));
            if (bytes == null) {
                return null;
            }
            TimestampModulus value = TimestampModulus.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SweepableTimestampsRowResult, Long> getTimestampModulusFun() {
            return new Function<SweepableTimestampsRowResult, Long>() {
                @Override
                public Long apply(SweepableTimestampsRowResult rowResult) {
                    return rowResult.getTimestampModulus();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("TimestampModulus", getTimestampModulus())
                .toString();
        }
    }

    public enum SweepableTimestampsNamedColumn {
        TIMESTAMP_MODULUS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("ts_mod");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SweepableTimestampsNamedColumn, byte[]> toShortName() {
            return new Function<SweepableTimestampsNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SweepableTimestampsNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SweepableTimestampsNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SweepableTimestampsNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SweepableTimestampsNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SweepableTimestampsNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SweepableTimestampsNamedColumnValue<?>>>builder()
                .put("ts_mod", TimestampModulus.BYTES_HYDRATOR)
                .build();

    public Map<SweepableTimestampsRow, Long> getTimestampModuluss(Collection<SweepableTimestampsRow> rows) {
        Map<Cell, SweepableTimestampsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepableTimestampsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("ts_mod")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepableTimestampsRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = TimestampModulus.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putTimestampModulus(SweepableTimestampsRow row, Long value) {
        put(ImmutableMultimap.of(row, TimestampModulus.of(value)));
    }

    public void putTimestampModulus(Map<SweepableTimestampsRow, Long> map) {
        Map<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepableTimestampsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), TimestampModulus.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putTimestampModulusUnlessExists(SweepableTimestampsRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, TimestampModulus.of(value)));
    }

    public void putTimestampModulusUnlessExists(Map<SweepableTimestampsRow, Long> map) {
        Map<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepableTimestampsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), TimestampModulus.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SweepableTimestampsRow, ? extends SweepableTimestampsNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SweepableTimestampsTrigger trigger : triggers) {
            trigger.putSweepableTimestamps(rows);
        }
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(Multimap<SweepableTimestampsRow, ? extends SweepableTimestampsNamedColumnValue<?>> rows) {
        Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<SweepableTimestampsRow, ? extends SweepableTimestampsNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteTimestampModulus(SweepableTimestampsRow row) {
        deleteTimestampModulus(ImmutableSet.of(row));
    }

    public void deleteTimestampModulus(Iterable<SweepableTimestampsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("ts_mod");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SweepableTimestampsRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SweepableTimestampsRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("ts_mod")));
        t.delete(tableRef, cells);
    }

    public Optional<SweepableTimestampsRowResult> getRow(SweepableTimestampsRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SweepableTimestampsRowResult> getRow(SweepableTimestampsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SweepableTimestampsRowResult.of(rowResult));
        }
    }

    @Override
    public List<SweepableTimestampsRowResult> getRows(Iterable<SweepableTimestampsRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SweepableTimestampsRowResult> getRows(Iterable<SweepableTimestampsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SweepableTimestampsRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SweepableTimestampsRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SweepableTimestampsNamedColumnValue<?>> getRowColumns(SweepableTimestampsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepableTimestampsNamedColumnValue<?>> getRowColumns(SweepableTimestampsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepableTimestampsNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> getRowsMultimap(Iterable<SweepableTimestampsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> getRowsMultimap(Iterable<SweepableTimestampsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SweepableTimestampsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepableTimestampsRow, BatchingVisitable<SweepableTimestampsNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepableTimestampsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepableTimestampsRow, BatchingVisitable<SweepableTimestampsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepableTimestampsNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepableTimestampsRow, SweepableTimestampsNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepableTimestampsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepableTimestampsRow row = SweepableTimestampsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepableTimestampsNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<SweepableTimestampsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepableTimestampsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
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
    static String __CLASS_HASH = "WPP7x4rcweJUek1Tq+iWlg==";
}
