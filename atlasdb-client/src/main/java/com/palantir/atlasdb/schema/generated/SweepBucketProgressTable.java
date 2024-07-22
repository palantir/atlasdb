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
public final class SweepBucketProgressTable implements
        AtlasDbMutablePersistentTable<SweepBucketProgressTable.SweepBucketProgressRow,
                                         SweepBucketProgressTable.SweepBucketProgressNamedColumnValue<?>,
                                         SweepBucketProgressTable.SweepBucketProgressRowResult>,
        AtlasDbNamedMutableTable<SweepBucketProgressTable.SweepBucketProgressRow,
                                    SweepBucketProgressTable.SweepBucketProgressNamedColumnValue<?>,
                                    SweepBucketProgressTable.SweepBucketProgressRowResult> {
    private final Transaction t;
    private final List<SweepBucketProgressTrigger> triggers;
    private final static String rawTableName = "sweepProgressPerBucket";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SweepBucketProgressNamedColumn.values());

    static SweepBucketProgressTable of(Transaction t, Namespace namespace) {
        return new SweepBucketProgressTable(t, namespace, ImmutableList.<SweepBucketProgressTrigger>of());
    }

    static SweepBucketProgressTable of(Transaction t, Namespace namespace, SweepBucketProgressTrigger trigger, SweepBucketProgressTrigger... triggers) {
        return new SweepBucketProgressTable(t, namespace, ImmutableList.<SweepBucketProgressTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepBucketProgressTable of(Transaction t, Namespace namespace, List<SweepBucketProgressTrigger> triggers) {
        return new SweepBucketProgressTable(t, namespace, triggers);
    }

    private SweepBucketProgressTable(Transaction t, Namespace namespace, List<SweepBucketProgressTrigger> triggers) {
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
     * SweepBucketProgressRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long shard};
     *   {@literal Long bucketIdentifier};
     *   {@literal byte[] sweepConservative};
     * }
     * </pre>
     */
    public static final class SweepBucketProgressRow implements Persistable, Comparable<SweepBucketProgressRow> {
        private final long hashOfRowComponents;
        private final long shard;
        private final long bucketIdentifier;
        private final byte[] sweepConservative;

        public static SweepBucketProgressRow of(long shard, long bucketIdentifier, byte[] sweepConservative) {
            long hashOfRowComponents = computeHashFirstComponents(shard, bucketIdentifier, sweepConservative);
            return new SweepBucketProgressRow(hashOfRowComponents, shard, bucketIdentifier, sweepConservative);
        }

        private SweepBucketProgressRow(long hashOfRowComponents, long shard, long bucketIdentifier, byte[] sweepConservative) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.shard = shard;
            this.bucketIdentifier = bucketIdentifier;
            this.sweepConservative = sweepConservative;
        }

        public long getShard() {
            return shard;
        }

        public long getBucketIdentifier() {
            return bucketIdentifier;
        }

        public byte[] getSweepConservative() {
            return sweepConservative;
        }

        public static Function<SweepBucketProgressRow, Long> getShardFun() {
            return new Function<SweepBucketProgressRow, Long>() {
                @Override
                public Long apply(SweepBucketProgressRow row) {
                    return row.shard;
                }
            };
        }

        public static Function<SweepBucketProgressRow, Long> getBucketIdentifierFun() {
            return new Function<SweepBucketProgressRow, Long>() {
                @Override
                public Long apply(SweepBucketProgressRow row) {
                    return row.bucketIdentifier;
                }
            };
        }

        public static Function<SweepBucketProgressRow, byte[]> getSweepConservativeFun() {
            return new Function<SweepBucketProgressRow, byte[]>() {
                @Override
                public byte[] apply(SweepBucketProgressRow row) {
                    return row.sweepConservative;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] shardBytes = EncodingUtils.encodeSignedVarLong(shard);
            byte[] bucketIdentifierBytes = EncodingUtils.encodeUnsignedVarLong(bucketIdentifier);
            byte[] sweepConservativeBytes = sweepConservative;
            return EncodingUtils.add(hashOfRowComponentsBytes, shardBytes, bucketIdentifierBytes, sweepConservativeBytes);
        }

        public static final Hydrator<SweepBucketProgressRow> BYTES_HYDRATOR = new Hydrator<SweepBucketProgressRow>() {
            @Override
            public SweepBucketProgressRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                long shard = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(shard);
                long bucketIdentifier = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(bucketIdentifier);
                byte[] sweepConservative = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                return new SweepBucketProgressRow(hashOfRowComponents, shard, bucketIdentifier, sweepConservative);
            }
        };

        public static long computeHashFirstComponents(long shard, long bucketIdentifier, byte[] sweepConservative) {
            byte[] shardBytes = EncodingUtils.encodeSignedVarLong(shard);
            byte[] bucketIdentifierBytes = EncodingUtils.encodeUnsignedVarLong(bucketIdentifier);
            byte[] sweepConservativeBytes = sweepConservative;
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(shardBytes, bucketIdentifierBytes, sweepConservativeBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("shard", shard)
                .add("bucketIdentifier", bucketIdentifier)
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
            SweepBucketProgressRow other = (SweepBucketProgressRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(shard, other.shard) && Objects.equals(bucketIdentifier, other.bucketIdentifier) && Arrays.equals(sweepConservative, other.sweepConservative);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, shard, bucketIdentifier, sweepConservative });
        }

        @Override
        public int compareTo(SweepBucketProgressRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.shard, o.shard)
                .compare(this.bucketIdentifier, o.bucketIdentifier)
                .compare(this.sweepConservative, o.sweepConservative, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface SweepBucketProgressNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class BucketProgress implements SweepBucketProgressNamedColumnValue<byte[]> {
        private final byte[] value;

        public static BucketProgress of(byte[] value) {
            return new BucketProgress(value);
        }

        private BucketProgress(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "bucket_progress";
        }

        @Override
        public String getShortColumnName() {
            return "p";
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value;
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("p");
        }

        public static final Hydrator<BucketProgress> BYTES_HYDRATOR = new Hydrator<BucketProgress>() {
            @Override
            public BucketProgress hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.getBytesFromOffsetToEnd(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface SweepBucketProgressTrigger {
        public void putSweepBucketProgress(Multimap<SweepBucketProgressRow, ? extends SweepBucketProgressNamedColumnValue<?>> newRows);
    }

    public static final class SweepBucketProgressRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SweepBucketProgressRowResult of(RowResult<byte[]> row) {
            return new SweepBucketProgressRowResult(row);
        }

        private SweepBucketProgressRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SweepBucketProgressRow getRowName() {
            return SweepBucketProgressRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SweepBucketProgressRowResult, SweepBucketProgressRow> getRowNameFun() {
            return new Function<SweepBucketProgressRowResult, SweepBucketProgressRow>() {
                @Override
                public SweepBucketProgressRow apply(SweepBucketProgressRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SweepBucketProgressRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SweepBucketProgressRowResult>() {
                @Override
                public SweepBucketProgressRowResult apply(RowResult<byte[]> rowResult) {
                    return new SweepBucketProgressRowResult(rowResult);
                }
            };
        }

        public boolean hasBucketProgress() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("p"));
        }

        public byte[] getBucketProgress() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("p"));
            if (bytes == null) {
                return null;
            }
            BucketProgress value = BucketProgress.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SweepBucketProgressRowResult, byte[]> getBucketProgressFun() {
            return new Function<SweepBucketProgressRowResult, byte[]>() {
                @Override
                public byte[] apply(SweepBucketProgressRowResult rowResult) {
                    return rowResult.getBucketProgress();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("BucketProgress", getBucketProgress())
                .toString();
        }
    }

    public enum SweepBucketProgressNamedColumn {
        BUCKET_PROGRESS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("p");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SweepBucketProgressNamedColumn, byte[]> toShortName() {
            return new Function<SweepBucketProgressNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SweepBucketProgressNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SweepBucketProgressNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SweepBucketProgressNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SweepBucketProgressNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SweepBucketProgressNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SweepBucketProgressNamedColumnValue<?>>>builder()
                .put("p", BucketProgress.BYTES_HYDRATOR)
                .build();

    public Map<SweepBucketProgressRow, byte[]> getBucketProgresss(Collection<SweepBucketProgressRow> rows) {
        Map<Cell, SweepBucketProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepBucketProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("p")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepBucketProgressRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = BucketProgress.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putBucketProgress(SweepBucketProgressRow row, byte[] value) {
        put(ImmutableMultimap.of(row, BucketProgress.of(value)));
    }

    public void putBucketProgress(Map<SweepBucketProgressRow, byte[]> map) {
        Map<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepBucketProgressRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), BucketProgress.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SweepBucketProgressRow, ? extends SweepBucketProgressNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SweepBucketProgressTrigger trigger : triggers) {
            trigger.putSweepBucketProgress(rows);
        }
    }

    public void deleteBucketProgress(SweepBucketProgressRow row) {
        deleteBucketProgress(ImmutableSet.of(row));
    }

    public void deleteBucketProgress(Iterable<SweepBucketProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("p");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SweepBucketProgressRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SweepBucketProgressRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("p")));
        t.delete(tableRef, cells);
    }

    public Optional<SweepBucketProgressRowResult> getRow(SweepBucketProgressRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SweepBucketProgressRowResult> getRow(SweepBucketProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SweepBucketProgressRowResult.of(rowResult));
        }
    }

    @Override
    public List<SweepBucketProgressRowResult> getRows(Iterable<SweepBucketProgressRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SweepBucketProgressRowResult> getRows(Iterable<SweepBucketProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SweepBucketProgressRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SweepBucketProgressRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SweepBucketProgressNamedColumnValue<?>> getRowColumns(SweepBucketProgressRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepBucketProgressNamedColumnValue<?>> getRowColumns(SweepBucketProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepBucketProgressNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> getRowsMultimap(Iterable<SweepBucketProgressRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> getRowsMultimap(Iterable<SweepBucketProgressRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SweepBucketProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepBucketProgressRow row = SweepBucketProgressRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepBucketProgressRow, BatchingVisitable<SweepBucketProgressNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepBucketProgressRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepBucketProgressRow, BatchingVisitable<SweepBucketProgressNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepBucketProgressRow row = SweepBucketProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepBucketProgressNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepBucketProgressRow, SweepBucketProgressNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepBucketProgressRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepBucketProgressRow row = SweepBucketProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepBucketProgressNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepBucketProgressRow, Iterator<SweepBucketProgressNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SweepBucketProgressRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepBucketProgressRow, Iterator<SweepBucketProgressNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepBucketProgressRow row = SweepBucketProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepBucketProgressNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<SweepBucketProgressRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepBucketProgressRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepBucketProgressRowResult>() {
            @Override
            public SweepBucketProgressRowResult apply(RowResult<byte[]> input) {
                return SweepBucketProgressRowResult.of(input);
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
    static String __CLASS_HASH = "wV1YiPCATShf1vpIcrUlgA==";
}
