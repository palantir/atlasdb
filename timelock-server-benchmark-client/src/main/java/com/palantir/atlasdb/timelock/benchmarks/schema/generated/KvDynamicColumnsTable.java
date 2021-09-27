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
public final class KvDynamicColumnsTable implements
        AtlasDbDynamicMutablePersistentTable<KvDynamicColumnsTable.KvDynamicColumnsRow,
                                                KvDynamicColumnsTable.KvDynamicColumnsColumn,
                                                KvDynamicColumnsTable.KvDynamicColumnsColumnValue,
                                                KvDynamicColumnsTable.KvDynamicColumnsRowResult> {
    private final Transaction t;
    private final List<KvDynamicColumnsTrigger> triggers;
    private final static String rawTableName = "KvDynamicColumns";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static KvDynamicColumnsTable of(Transaction t, Namespace namespace) {
        return new KvDynamicColumnsTable(t, namespace, ImmutableList.<KvDynamicColumnsTrigger>of());
    }

    static KvDynamicColumnsTable of(Transaction t, Namespace namespace, KvDynamicColumnsTrigger trigger, KvDynamicColumnsTrigger... triggers) {
        return new KvDynamicColumnsTable(t, namespace, ImmutableList.<KvDynamicColumnsTrigger>builder().add(trigger).add(triggers).build());
    }

    static KvDynamicColumnsTable of(Transaction t, Namespace namespace, List<KvDynamicColumnsTrigger> triggers) {
        return new KvDynamicColumnsTable(t, namespace, triggers);
    }

    private KvDynamicColumnsTable(Transaction t, Namespace namespace, List<KvDynamicColumnsTrigger> triggers) {
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
     * KvDynamicColumnsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal String bucket};
     * }
     * </pre>
     */
    public static final class KvDynamicColumnsRow implements Persistable, Comparable<KvDynamicColumnsRow> {
        private final long hashOfRowComponents;
        private final String bucket;

        public static KvDynamicColumnsRow of(String bucket) {
            long hashOfRowComponents = computeHashFirstComponents(bucket);
            return new KvDynamicColumnsRow(hashOfRowComponents, bucket);
        }

        private KvDynamicColumnsRow(long hashOfRowComponents, String bucket) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.bucket = bucket;
        }

        public String getBucket() {
            return bucket;
        }

        public static Function<KvDynamicColumnsRow, String> getBucketFun() {
            return new Function<KvDynamicColumnsRow, String>() {
                @Override
                public String apply(KvDynamicColumnsRow row) {
                    return row.bucket;
                }
            };
        }

        public static Function<String, KvDynamicColumnsRow> fromBucketFun() {
            return new Function<String, KvDynamicColumnsRow>() {
                @Override
                public KvDynamicColumnsRow apply(String row) {
                    return KvDynamicColumnsRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] bucketBytes = EncodingUtils.encodeVarString(bucket);
            return EncodingUtils.add(hashOfRowComponentsBytes, bucketBytes);
        }

        public static final Hydrator<KvDynamicColumnsRow> BYTES_HYDRATOR = new Hydrator<KvDynamicColumnsRow>() {
            @Override
            public KvDynamicColumnsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String bucket = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(bucket);
                return new KvDynamicColumnsRow(hashOfRowComponents, bucket);
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

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("bucket", bucket)
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
            KvDynamicColumnsRow other = (KvDynamicColumnsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(bucket, other.bucket);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, bucket });
        }

        @Override
        public int compareTo(KvDynamicColumnsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.bucket, o.bucket)
                .result();
        }
    }

    /**
     * <pre>
     * KvDynamicColumnsColumn {
     *   {@literal Long key};
     * }
     * </pre>
     */
    public static final class KvDynamicColumnsColumn implements Persistable, Comparable<KvDynamicColumnsColumn> {
        private final long key;

        public static KvDynamicColumnsColumn of(long key) {
            return new KvDynamicColumnsColumn(key);
        }

        private KvDynamicColumnsColumn(long key) {
            this.key = key;
        }

        public long getKey() {
            return key;
        }

        public static Function<KvDynamicColumnsColumn, Long> getKeyFun() {
            return new Function<KvDynamicColumnsColumn, Long>() {
                @Override
                public Long apply(KvDynamicColumnsColumn row) {
                    return row.key;
                }
            };
        }

        public static Function<Long, KvDynamicColumnsColumn> fromKeyFun() {
            return new Function<Long, KvDynamicColumnsColumn>() {
                @Override
                public KvDynamicColumnsColumn apply(Long row) {
                    return KvDynamicColumnsColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] keyBytes = PtBytes.toBytes(Long.MIN_VALUE ^ key);
            return EncodingUtils.add(keyBytes);
        }

        public static final Hydrator<KvDynamicColumnsColumn> BYTES_HYDRATOR = new Hydrator<KvDynamicColumnsColumn>() {
            @Override
            public KvDynamicColumnsColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long key = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new KvDynamicColumnsColumn(key);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
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
            KvDynamicColumnsColumn other = (KvDynamicColumnsColumn) obj;
            return Objects.equals(key, other.key);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        @Override
        public int compareTo(KvDynamicColumnsColumn o) {
            return ComparisonChain.start()
                .compare(this.key, o.key)
                .result();
        }
    }

    public interface KvDynamicColumnsTrigger {
        public void putKvDynamicColumns(Multimap<KvDynamicColumnsRow, ? extends KvDynamicColumnsColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long key};
     * }
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class KvDynamicColumnsColumnValue implements ColumnValue<byte[]> {
        private final KvDynamicColumnsColumn columnName;
        private final byte[] value;

        public static KvDynamicColumnsColumnValue of(KvDynamicColumnsColumn columnName, byte[] value) {
            return new KvDynamicColumnsColumnValue(columnName, value);
        }

        private KvDynamicColumnsColumnValue(KvDynamicColumnsColumn columnName, byte[] value) {
            this.columnName = columnName;
            this.value = value;
        }

        public KvDynamicColumnsColumn getColumnName() {
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

        public static Function<KvDynamicColumnsColumnValue, KvDynamicColumnsColumn> getColumnNameFun() {
            return new Function<KvDynamicColumnsColumnValue, KvDynamicColumnsColumn>() {
                @Override
                public KvDynamicColumnsColumn apply(KvDynamicColumnsColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<KvDynamicColumnsColumnValue, byte[]> getValueFun() {
            return new Function<KvDynamicColumnsColumnValue, byte[]>() {
                @Override
                public byte[] apply(KvDynamicColumnsColumnValue columnValue) {
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

    public static final class KvDynamicColumnsRowResult implements TypedRowResult {
        private final KvDynamicColumnsRow rowName;
        private final ImmutableSet<KvDynamicColumnsColumnValue> columnValues;

        public static KvDynamicColumnsRowResult of(RowResult<byte[]> rowResult) {
            KvDynamicColumnsRow rowName = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<KvDynamicColumnsColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] value = KvDynamicColumnsColumnValue.hydrateValue(e.getValue());
                columnValues.add(KvDynamicColumnsColumnValue.of(col, value));
            }
            return new KvDynamicColumnsRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private KvDynamicColumnsRowResult(KvDynamicColumnsRow rowName, ImmutableSet<KvDynamicColumnsColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public KvDynamicColumnsRow getRowName() {
            return rowName;
        }

        public Set<KvDynamicColumnsColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<KvDynamicColumnsRowResult, KvDynamicColumnsRow> getRowNameFun() {
            return new Function<KvDynamicColumnsRowResult, KvDynamicColumnsRow>() {
                @Override
                public KvDynamicColumnsRow apply(KvDynamicColumnsRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<KvDynamicColumnsRowResult, ImmutableSet<KvDynamicColumnsColumnValue>> getColumnValuesFun() {
            return new Function<KvDynamicColumnsRowResult, ImmutableSet<KvDynamicColumnsColumnValue>>() {
                @Override
                public ImmutableSet<KvDynamicColumnsColumnValue> apply(KvDynamicColumnsRowResult rowResult) {
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
    public void delete(KvDynamicColumnsRow row, KvDynamicColumnsColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<KvDynamicColumnsRow> rows) {
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> toRemove = HashMultimap.create();
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> result = getRowsMultimap(rows);
        for (Entry<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(KvDynamicColumnsRow rowName, Iterable<KvDynamicColumnsColumnValue> values) {
        put(ImmutableMultimap.<KvDynamicColumnsRow, KvDynamicColumnsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(KvDynamicColumnsRow rowName, KvDynamicColumnsColumnValue... values) {
        put(ImmutableMultimap.<KvDynamicColumnsRow, KvDynamicColumnsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<KvDynamicColumnsRow, ? extends KvDynamicColumnsColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (KvDynamicColumnsTrigger trigger : triggers) {
            trigger.putKvDynamicColumns(values);
        }
    }

    @Override
    public void touch(Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> values) {
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<KvDynamicColumnsColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(KvDynamicColumnsColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> get(Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                KvDynamicColumnsRow row = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                byte[] val = KvDynamicColumnsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, KvDynamicColumnsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<KvDynamicColumnsColumnValue> getRowColumns(KvDynamicColumnsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<KvDynamicColumnsColumnValue> getRowColumns(KvDynamicColumnsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<KvDynamicColumnsColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = KvDynamicColumnsColumnValue.hydrateValue(e.getValue());
                ret.add(KvDynamicColumnsColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> getRowsMultimap(Iterable<KvDynamicColumnsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> getRowsMultimap(Iterable<KvDynamicColumnsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> getRowsMultimapInternal(Iterable<KvDynamicColumnsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            KvDynamicColumnsRow row = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = KvDynamicColumnsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, KvDynamicColumnsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<KvDynamicColumnsRow, BatchingVisitable<KvDynamicColumnsColumnValue>> getRowsColumnRange(Iterable<KvDynamicColumnsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KvDynamicColumnsRow, BatchingVisitable<KvDynamicColumnsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KvDynamicColumnsRow row = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<KvDynamicColumnsColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = KvDynamicColumnsColumnValue.hydrateValue(result.getValue());
                return KvDynamicColumnsColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<KvDynamicColumnsRow, KvDynamicColumnsColumnValue>> getRowsColumnRange(Iterable<KvDynamicColumnsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            KvDynamicColumnsRow row = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            byte[] val = KvDynamicColumnsColumnValue.hydrateValue(e.getValue());
            KvDynamicColumnsColumnValue colValue = KvDynamicColumnsColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<KvDynamicColumnsRow, Iterator<KvDynamicColumnsColumnValue>> getRowsColumnRangeIterator(Iterable<KvDynamicColumnsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KvDynamicColumnsRow, Iterator<KvDynamicColumnsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KvDynamicColumnsRow row = KvDynamicColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<KvDynamicColumnsColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                KvDynamicColumnsColumn col = KvDynamicColumnsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = KvDynamicColumnsColumnValue.hydrateValue(result.getValue());
                return KvDynamicColumnsColumnValue.of(col, val);
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

    public BatchingVisitableView<KvDynamicColumnsRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, KvDynamicColumnsRowResult>() {
            @Override
            public KvDynamicColumnsRowResult apply(RowResult<byte[]> input) {
                return KvDynamicColumnsRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<KvDynamicColumnsRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<KvDynamicColumnsRowResult>>() {
            @Override
            public BatchingVisitable<KvDynamicColumnsRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, KvDynamicColumnsRowResult>() {
                    @Override
                    public KvDynamicColumnsRowResult apply(RowResult<byte[]> row) {
                        return KvDynamicColumnsRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<KvDynamicColumnsRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KvDynamicColumnsRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<KvDynamicColumnsRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KvDynamicColumnsRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<KvDynamicColumnsRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, KvDynamicColumnsRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<KvDynamicColumnsRowResult>, RuntimeException>() {
            @Override
            public boolean visit(List<KvDynamicColumnsRowResult> rowResults) {
                Multimap<KvDynamicColumnsRow, KvDynamicColumnsColumn> toRemove = HashMultimap.create();
                for (KvDynamicColumnsRowResult rowResult : rowResults) {
                    for (KvDynamicColumnsColumnValue columnValue : rowResult.getColumnValues()) {
                        toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                    }
                }
                delete(toRemove);
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
    static String __CLASS_HASH = "afiyTnltfesp99pfyrOGSg==";
}
