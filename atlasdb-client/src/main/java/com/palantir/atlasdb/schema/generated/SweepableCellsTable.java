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
public final class SweepableCellsTable implements
        AtlasDbDynamicMutablePersistentTable<SweepableCellsTable.SweepableCellsRow,
                                                SweepableCellsTable.SweepableCellsColumn,
                                                SweepableCellsTable.SweepableCellsColumnValue,
                                                SweepableCellsTable.SweepableCellsRowResult> {
    private final Transaction t;
    private final List<SweepableCellsTrigger> triggers;
    private final static String rawTableName = "sweepableCells";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SweepableCellsTable of(Transaction t, Namespace namespace) {
        return new SweepableCellsTable(t, namespace, ImmutableList.<SweepableCellsTrigger>of());
    }

    static SweepableCellsTable of(Transaction t, Namespace namespace, SweepableCellsTrigger trigger, SweepableCellsTrigger... triggers) {
        return new SweepableCellsTable(t, namespace, ImmutableList.<SweepableCellsTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepableCellsTable of(Transaction t, Namespace namespace, List<SweepableCellsTrigger> triggers) {
        return new SweepableCellsTable(t, namespace, triggers);
    }

    private SweepableCellsTable(Transaction t, Namespace namespace, List<SweepableCellsTrigger> triggers) {
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
     * SweepableCellsRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long timestampPartition};
     *   {@literal byte[] metadata};
     * }
     * </pre>
     */
    public static final class SweepableCellsRow implements Persistable, Comparable<SweepableCellsRow> {
        private final long hashOfRowComponents;
        private final long timestampPartition;
        private final byte[] metadata;

        public static SweepableCellsRow of(long timestampPartition, byte[] metadata) {
            long hashOfRowComponents = computeHashFirstComponents(timestampPartition, metadata);
            return new SweepableCellsRow(hashOfRowComponents, timestampPartition, metadata);
        }

        private SweepableCellsRow(long hashOfRowComponents, long timestampPartition, byte[] metadata) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.timestampPartition = timestampPartition;
            this.metadata = metadata;
        }

        public long getTimestampPartition() {
            return timestampPartition;
        }

        public byte[] getMetadata() {
            return metadata;
        }

        public static Function<SweepableCellsRow, Long> getTimestampPartitionFun() {
            return new Function<SweepableCellsRow, Long>() {
                @Override
                public Long apply(SweepableCellsRow row) {
                    return row.timestampPartition;
                }
            };
        }

        public static Function<SweepableCellsRow, byte[]> getMetadataFun() {
            return new Function<SweepableCellsRow, byte[]>() {
                @Override
                public byte[] apply(SweepableCellsRow row) {
                    return row.metadata;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] timestampPartitionBytes = EncodingUtils.encodeUnsignedVarLong(timestampPartition);
            byte[] metadataBytes = metadata;
            return EncodingUtils.add(hashOfRowComponentsBytes, timestampPartitionBytes, metadataBytes);
        }

        public static final Hydrator<SweepableCellsRow> BYTES_HYDRATOR = new Hydrator<SweepableCellsRow>() {
            @Override
            public SweepableCellsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long timestampPartition = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(timestampPartition);
                byte[] metadata = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new SweepableCellsRow(hashOfRowComponents, timestampPartition, metadata);
            }
        };

        public static long computeHashFirstComponents(long timestampPartition, byte[] metadata) {
            byte[] timestampPartitionBytes = EncodingUtils.encodeUnsignedVarLong(timestampPartition);
            byte[] metadataBytes = metadata;
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(timestampPartitionBytes, metadataBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("timestampPartition", timestampPartition)
                .add("metadata", metadata)
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
            SweepableCellsRow other = (SweepableCellsRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(timestampPartition, other.timestampPartition) && Arrays.equals(metadata, other.metadata);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, timestampPartition, metadata });
        }

        @Override
        public int compareTo(SweepableCellsRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.timestampPartition, o.timestampPartition)
                .compare(this.metadata, o.metadata, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    /**
     * <pre>
     * SweepableCellsColumn {
     *   {@literal Long timestampModulus};
     *   {@literal Long writeIndex};
     * }
     * </pre>
     */
    public static final class SweepableCellsColumn implements Persistable, Comparable<SweepableCellsColumn> {
        private final long timestampModulus;
        private final long writeIndex;

        public static SweepableCellsColumn of(long timestampModulus, long writeIndex) {
            return new SweepableCellsColumn(timestampModulus, writeIndex);
        }

        private SweepableCellsColumn(long timestampModulus, long writeIndex) {
            this.timestampModulus = timestampModulus;
            this.writeIndex = writeIndex;
        }

        public long getTimestampModulus() {
            return timestampModulus;
        }

        public long getWriteIndex() {
            return writeIndex;
        }

        public static Function<SweepableCellsColumn, Long> getTimestampModulusFun() {
            return new Function<SweepableCellsColumn, Long>() {
                @Override
                public Long apply(SweepableCellsColumn row) {
                    return row.timestampModulus;
                }
            };
        }

        public static Function<SweepableCellsColumn, Long> getWriteIndexFun() {
            return new Function<SweepableCellsColumn, Long>() {
                @Override
                public Long apply(SweepableCellsColumn row) {
                    return row.writeIndex;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] timestampModulusBytes = EncodingUtils.encodeUnsignedVarLong(timestampModulus);
            byte[] writeIndexBytes = EncodingUtils.encodeSignedVarLong(writeIndex);
            return EncodingUtils.add(timestampModulusBytes, writeIndexBytes);
        }

        public static final Hydrator<SweepableCellsColumn> BYTES_HYDRATOR = new Hydrator<SweepableCellsColumn>() {
            @Override
            public SweepableCellsColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long timestampModulus = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(timestampModulus);
                Long writeIndex = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(writeIndex);
                return new SweepableCellsColumn(timestampModulus, writeIndex);
            }
        };

        public static BatchColumnRangeSelection createPrefixRange(long timestampModulus, int batchSize) {
            byte[] timestampModulusBytes = EncodingUtils.encodeUnsignedVarLong(timestampModulus);
            return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(timestampModulusBytes), batchSize);
        }

        public static Prefix prefix(long timestampModulus) {
            byte[] timestampModulusBytes = EncodingUtils.encodeUnsignedVarLong(timestampModulus);
            return new Prefix(EncodingUtils.add(timestampModulusBytes));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("timestampModulus", timestampModulus)
                .add("writeIndex", writeIndex)
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
            SweepableCellsColumn other = (SweepableCellsColumn) obj;
            return Objects.equals(timestampModulus, other.timestampModulus) && Objects.equals(writeIndex, other.writeIndex);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ timestampModulus, writeIndex });
        }

        @Override
        public int compareTo(SweepableCellsColumn o) {
            return ComparisonChain.start()
                .compare(this.timestampModulus, o.timestampModulus)
                .compare(this.writeIndex, o.writeIndex)
                .result();
        }
    }

    public interface SweepableCellsTrigger {
        public void putSweepableCells(Multimap<SweepableCellsRow, ? extends SweepableCellsColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long timestampModulus};
     *   {@literal Long writeIndex};
     * }
     * Column value description {
     *   type: com.palantir.atlasdb.keyvalue.api.StoredWriteReference;
     * }
     * </pre>
     */
    public static final class SweepableCellsColumnValue implements ColumnValue<com.palantir.atlasdb.keyvalue.api.StoredWriteReference> {
        private final SweepableCellsColumn columnName;
        private final com.palantir.atlasdb.keyvalue.api.StoredWriteReference value;

        public static SweepableCellsColumnValue of(SweepableCellsColumn columnName, com.palantir.atlasdb.keyvalue.api.StoredWriteReference value) {
            return new SweepableCellsColumnValue(columnName, value);
        }

        private SweepableCellsColumnValue(SweepableCellsColumn columnName, com.palantir.atlasdb.keyvalue.api.StoredWriteReference value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SweepableCellsColumn getColumnName() {
            return columnName;
        }

        @Override
        public com.palantir.atlasdb.keyvalue.api.StoredWriteReference getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.persistToBytes();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static com.palantir.atlasdb.keyvalue.api.StoredWriteReference hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return com.palantir.atlasdb.keyvalue.api.StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(bytes);
        }

        public static Function<SweepableCellsColumnValue, SweepableCellsColumn> getColumnNameFun() {
            return new Function<SweepableCellsColumnValue, SweepableCellsColumn>() {
                @Override
                public SweepableCellsColumn apply(SweepableCellsColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SweepableCellsColumnValue, com.palantir.atlasdb.keyvalue.api.StoredWriteReference> getValueFun() {
            return new Function<SweepableCellsColumnValue, com.palantir.atlasdb.keyvalue.api.StoredWriteReference>() {
                @Override
                public com.palantir.atlasdb.keyvalue.api.StoredWriteReference apply(SweepableCellsColumnValue columnValue) {
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

    public static final class SweepableCellsRowResult implements TypedRowResult {
        private final SweepableCellsRow rowName;
        private final ImmutableSet<SweepableCellsColumnValue> columnValues;

        public static SweepableCellsRowResult of(RowResult<byte[]> rowResult) {
            SweepableCellsRow rowName = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SweepableCellsColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference value = SweepableCellsColumnValue.hydrateValue(e.getValue());
                columnValues.add(SweepableCellsColumnValue.of(col, value));
            }
            return new SweepableCellsRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SweepableCellsRowResult(SweepableCellsRow rowName, ImmutableSet<SweepableCellsColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SweepableCellsRow getRowName() {
            return rowName;
        }

        public Set<SweepableCellsColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SweepableCellsRowResult, SweepableCellsRow> getRowNameFun() {
            return new Function<SweepableCellsRowResult, SweepableCellsRow>() {
                @Override
                public SweepableCellsRow apply(SweepableCellsRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SweepableCellsRowResult, ImmutableSet<SweepableCellsColumnValue>> getColumnValuesFun() {
            return new Function<SweepableCellsRowResult, ImmutableSet<SweepableCellsColumnValue>>() {
                @Override
                public ImmutableSet<SweepableCellsColumnValue> apply(SweepableCellsRowResult rowResult) {
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
    public void delete(SweepableCellsRow row, SweepableCellsColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SweepableCellsRow> rows) {
        Multimap<SweepableCellsRow, SweepableCellsColumn> toRemove = HashMultimap.create();
        Multimap<SweepableCellsRow, SweepableCellsColumnValue> result = getRowsMultimap(rows);
        for (Entry<SweepableCellsRow, SweepableCellsColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SweepableCellsRow, SweepableCellsColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SweepableCellsRow rowName, Iterable<SweepableCellsColumnValue> values) {
        put(ImmutableMultimap.<SweepableCellsRow, SweepableCellsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SweepableCellsRow rowName, SweepableCellsColumnValue... values) {
        put(ImmutableMultimap.<SweepableCellsRow, SweepableCellsColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SweepableCellsRow, ? extends SweepableCellsColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SweepableCellsTrigger trigger : triggers) {
            trigger.putSweepableCells(values);
        }
    }

    @Override
    public void touch(Multimap<SweepableCellsRow, SweepableCellsColumn> values) {
        Multimap<SweepableCellsRow, SweepableCellsColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SweepableCellsRow, SweepableCellsColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SweepableCellsRow, SweepableCellsColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SweepableCellsColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SweepableCellsColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SweepableCellsRow, SweepableCellsColumnValue> get(Multimap<SweepableCellsRow, SweepableCellsColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SweepableCellsRow, SweepableCellsColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SweepableCellsRow row = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepableCellsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SweepableCellsColumnValue> getRowColumns(SweepableCellsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepableCellsColumnValue> getRowColumns(SweepableCellsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepableCellsColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(e.getValue());
                ret.add(SweepableCellsColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepableCellsRow, SweepableCellsColumnValue> getRowsMultimap(Iterable<SweepableCellsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepableCellsRow, SweepableCellsColumnValue> getRowsMultimap(Iterable<SweepableCellsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepableCellsRow, SweepableCellsColumnValue> getRowsMultimapInternal(Iterable<SweepableCellsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepableCellsRow, SweepableCellsColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepableCellsRow, SweepableCellsColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepableCellsRow row = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SweepableCellsColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepableCellsRow, BatchingVisitable<SweepableCellsColumnValue>> getRowsColumnRange(Iterable<SweepableCellsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepableCellsRow, BatchingVisitable<SweepableCellsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepableCellsRow row = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepableCellsColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(result.getValue());
                return SweepableCellsColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepableCellsRow, SweepableCellsColumnValue>> getRowsColumnRange(Iterable<SweepableCellsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepableCellsRow row = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(e.getValue());
            SweepableCellsColumnValue colValue = SweepableCellsColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepableCellsRow, Iterator<SweepableCellsColumnValue>> getRowsColumnRangeIterator(Iterable<SweepableCellsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepableCellsRow, Iterator<SweepableCellsColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepableCellsRow row = SweepableCellsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepableCellsColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SweepableCellsColumn col = SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                com.palantir.atlasdb.keyvalue.api.StoredWriteReference val = SweepableCellsColumnValue.hydrateValue(result.getValue());
                return SweepableCellsColumnValue.of(col, val);
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

    public BatchingVisitableView<SweepableCellsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepableCellsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepableCellsRowResult>() {
            @Override
            public SweepableCellsRowResult apply(RowResult<byte[]> input) {
                return SweepableCellsRowResult.of(input);
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
    static String __CLASS_HASH = "Txtgb8DFuOwj83JGx3rzXQ==";
}
