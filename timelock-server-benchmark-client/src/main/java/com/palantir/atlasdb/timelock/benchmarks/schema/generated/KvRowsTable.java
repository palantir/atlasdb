package com.palantir.atlasdb.timelock.benchmarks.schema.generated;

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
public final class KvRowsTable implements
        AtlasDbMutablePersistentTable<KvRowsTable.KvRowsRow,
                                         KvRowsTable.KvRowsNamedColumnValue<?>,
                                         KvRowsTable.KvRowsRowResult>,
        AtlasDbNamedMutableTable<KvRowsTable.KvRowsRow,
                                    KvRowsTable.KvRowsNamedColumnValue<?>,
                                    KvRowsTable.KvRowsRowResult> {
    private final Transaction t;
    private final List<KvRowsTrigger> triggers;
    private final static String rawTableName = "KvRows";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(KvRowsNamedColumn.values());

    static KvRowsTable of(Transaction t, Namespace namespace) {
        return new KvRowsTable(t, namespace, ImmutableList.<KvRowsTrigger>of());
    }

    static KvRowsTable of(Transaction t, Namespace namespace, KvRowsTrigger trigger, KvRowsTrigger... triggers) {
        return new KvRowsTable(t, namespace, ImmutableList.<KvRowsTrigger>builder().add(trigger).add(triggers).build());
    }

    static KvRowsTable of(Transaction t, Namespace namespace, List<KvRowsTrigger> triggers) {
        return new KvRowsTable(t, namespace, triggers);
    }

    private KvRowsTable(Transaction t, Namespace namespace, List<KvRowsTrigger> triggers) {
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
     * KvRowsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal String bucket};
     *   {@literal Long key};
     * }
     * </pre>
     */
    public static final class KvRowsRow implements Persistable, Comparable<KvRowsRow> {
        private final long hashOfRowComponents;
        private final String bucket;
        private final long key;

        public static KvRowsRow of(String bucket, long key) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            return new KvRowsRow(hashOfRowComponents, bucket, key);
        }

        private KvRowsRow(long hashOfRowComponents, String bucket, long key) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.bucket = bucket;
            this.key = key;
        }

        public String getBucket() {
            return bucket;
        }

        public long getKey() {
            return key;
        }

        public static Function<KvRowsRow, String> getBucketFun() {
            return new Function<KvRowsRow, String>() {
                @Override
                public String apply(KvRowsRow row) {
                    return row.bucket;
                }
            };
        }

        public static Function<KvRowsRow, Long> getKeyFun() {
            return new Function<KvRowsRow, Long>() {
                @Override
                public Long apply(KvRowsRow row) {
                    return row.key;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            byte[] keyBytes = PtBytes.toBytes(Long.MIN_VALUE ^ key);
            return EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes, keyBytes);
        }

        public static final Hydrator<KvRowsRow> BYTES_HYDRATOR = new Hydrator<KvRowsRow>() {
            @Override
            public KvRowsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String bucket = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(bucket);
                Long key = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new KvRowsRow(hashOfRowComponents, bucket, key);
            }
        };

        public static long computeHashFirstComponents(String bucket) {
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(bucketBytes)).asLong();
        }

        public static RangeRequest.Builder createPrefixRangeUnsorted(String bucket) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes));
        }

        public static Prefix prefixUnsorted(String bucket) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            return new Prefix(EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes));
        }

        public static RangeRequest.Builder createPrefixRange(String bucket, long key) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            byte[] keyBytes = PtBytes.toBytes(Long.MIN_VALUE ^ key);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes, keyBytes));
        }

        public static Prefix prefix(String bucket, long key) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            byte[] keyBytes = PtBytes.toBytes(Long.MIN_VALUE ^ key);
            return new Prefix(EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes, keyBytes));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("bucket", bucket)
                .add("key", key)
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
            KvRowsRow other = (KvRowsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(bucket, other.bucket) && Objects.equals(key, other.key);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, bucket, key });
        }

        @Override
        public int compareTo(KvRowsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.bucket, o.bucket)
                .compare(this.key, o.key)
                .result();
        }
    }

    public interface KvRowsNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Data implements KvRowsNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Data of(byte[] value) {
            return new Data(value);
        }

        private Data(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "data";
        }

        @Override
        public String getShortColumnName() {
            return "d";
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
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<Data> BYTES_HYDRATOR = new Hydrator<Data>() {
            @Override
            public Data hydrateFromBytes(byte[] bytes) {
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

    public interface KvRowsTrigger {
        public void putKvRows(Multimap<KvRowsRow, ? extends KvRowsNamedColumnValue<?>> newRows);
    }

    public static final class KvRowsRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static KvRowsRowResult of(RowResult<byte[]> row) {
            return new KvRowsRowResult(row);
        }

        private KvRowsRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public KvRowsRow getRowName() {
            return KvRowsRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<KvRowsRowResult, KvRowsRow> getRowNameFun() {
            return new Function<KvRowsRowResult, KvRowsRow>() {
                @Override
                public KvRowsRow apply(KvRowsRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, KvRowsRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, KvRowsRowResult>() {
                @Override
                public KvRowsRowResult apply(RowResult<byte[]> rowResult) {
                    return new KvRowsRowResult(rowResult);
                }
            };
        }

        public boolean hasData() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public byte[] getData() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            Data value = Data.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<KvRowsRowResult, byte[]> getDataFun() {
            return new Function<KvRowsRowResult, byte[]>() {
                @Override
                public byte[] apply(KvRowsRowResult rowResult) {
                    return rowResult.getData();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Data", getData())
                .toString();
        }
    }

    public enum KvRowsNamedColumn {
        DATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        };

        public abstract byte[] getShortName();

        public static Function<KvRowsNamedColumn, byte[]> toShortName() {
            return new Function<KvRowsNamedColumn, byte[]>() {
                @Override
                public byte[] apply(KvRowsNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<KvRowsNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, KvRowsNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(KvRowsNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends KvRowsNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends KvRowsNamedColumnValue<?>>>builder()
                .put("d", Data.BYTES_HYDRATOR)
                .build();

    public Map<KvRowsRow, byte[]> getDatas(Collection<KvRowsRow> rows) {
        Map<Cell, KvRowsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (KvRowsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<KvRowsRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Data.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putData(KvRowsRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Data.of(value)));
    }

    public void putData(Map<KvRowsRow, byte[]> map) {
        Map<KvRowsRow, KvRowsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<KvRowsRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Data.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<KvRowsRow, ? extends KvRowsNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (KvRowsTrigger trigger : triggers) {
            trigger.putKvRows(rows);
        }
    }

    public void deleteData(KvRowsRow row) {
        deleteData(ImmutableSet.of(row));
    }

    public void deleteData(Iterable<KvRowsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(KvRowsRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<KvRowsRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    public Optional<KvRowsRowResult> getRow(KvRowsRow row) {
        return getRow(row, allColumns);
    }

    public Optional<KvRowsRowResult> getRow(KvRowsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(KvRowsRowResult.of(rowResult));
        }
    }

    @Override
    public List<KvRowsRowResult> getRows(Iterable<KvRowsRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<KvRowsRowResult> getRows(Iterable<KvRowsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<KvRowsRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(KvRowsRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<KvRowsNamedColumnValue<?>> getRowColumns(KvRowsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<KvRowsNamedColumnValue<?>> getRowColumns(KvRowsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<KvRowsNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<KvRowsRow, KvRowsNamedColumnValue<?>> getRowsMultimap(Iterable<KvRowsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<KvRowsRow, KvRowsNamedColumnValue<?>> getRowsMultimap(Iterable<KvRowsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<KvRowsRow, KvRowsNamedColumnValue<?>> getRowsMultimapInternal(Iterable<KvRowsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<KvRowsRow, KvRowsNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<KvRowsRow, KvRowsNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            KvRowsRow row = KvRowsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<KvRowsRow, BatchingVisitable<KvRowsNamedColumnValue<?>>> getRowsColumnRange(Iterable<KvRowsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KvRowsRow, BatchingVisitable<KvRowsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KvRowsRow row = KvRowsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<KvRowsNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<KvRowsRow, KvRowsNamedColumnValue<?>>> getRowsColumnRange(Iterable<KvRowsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            KvRowsRow row = KvRowsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            KvRowsNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<KvRowsRow, Iterator<KvRowsNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<KvRowsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KvRowsRow, Iterator<KvRowsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KvRowsRow row = KvRowsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<KvRowsNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private RangeRequest optimizeRangeRequest(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            return range.getBuilder().retainColumns(allColumns).build();
        }
        return range;
    }

    private Iterable<RangeRequest> optimizeRangeRequests(Iterable<RangeRequest> ranges) {
        return Iterables.transform(ranges, this::optimizeRangeRequest);
    }

    public BatchingVisitableView<KvRowsRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, KvRowsRowResult>() {
            @Override
            public KvRowsRowResult apply(RowResult<byte[]> input) {
                return KvRowsRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<KvRowsRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<KvRowsRowResult>>() {
            @Override
            public BatchingVisitable<KvRowsRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, KvRowsRowResult>() {
                    @Override
                    public KvRowsRowResult apply(RowResult<byte[]> row) {
                        return KvRowsRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<KvRowsRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KvRowsRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<KvRowsRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KvRowsRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<KvRowsRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, KvRowsRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(KvRowsRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<KvRowsRow>, RuntimeException>() {
            @Override
            public boolean visit(List<KvRowsRow> rows) {
                delete(rows);
                return true;
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
    static String __CLASS_HASH = "qQ8yVU6mlFnS76qaOYTVtg==";
}
