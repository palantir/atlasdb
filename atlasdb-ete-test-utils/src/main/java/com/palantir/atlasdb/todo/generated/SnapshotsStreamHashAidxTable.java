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
public final class SnapshotsStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxRow,
                                                SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxColumn,
                                                SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxColumnValue,
                                                SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<SnapshotsStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "snapshots_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static SnapshotsStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new SnapshotsStreamHashAidxTable(t, namespace, ImmutableList.<SnapshotsStreamHashAidxTrigger>of());
    }

    static SnapshotsStreamHashAidxTable of(Transaction t, Namespace namespace, SnapshotsStreamHashAidxTrigger trigger, SnapshotsStreamHashAidxTrigger... triggers) {
        return new SnapshotsStreamHashAidxTable(t, namespace, ImmutableList.<SnapshotsStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static SnapshotsStreamHashAidxTable of(Transaction t, Namespace namespace, List<SnapshotsStreamHashAidxTrigger> triggers) {
        return new SnapshotsStreamHashAidxTable(t, namespace, triggers);
    }

    private SnapshotsStreamHashAidxTable(Transaction t, Namespace namespace, List<SnapshotsStreamHashAidxTrigger> triggers) {
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
     * SnapshotsStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class SnapshotsStreamHashAidxRow implements Persistable, Comparable<SnapshotsStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static SnapshotsStreamHashAidxRow of(Sha256Hash hash) {
            return new SnapshotsStreamHashAidxRow(hash);
        }

        private SnapshotsStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<SnapshotsStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<SnapshotsStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(SnapshotsStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, SnapshotsStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, SnapshotsStreamHashAidxRow>() {
                @Override
                public SnapshotsStreamHashAidxRow apply(Sha256Hash row) {
                    return SnapshotsStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<SnapshotsStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<SnapshotsStreamHashAidxRow>() {
            @Override
            public SnapshotsStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new SnapshotsStreamHashAidxRow(hash);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hash", hash)
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
            SnapshotsStreamHashAidxRow other = (SnapshotsStreamHashAidxRow) obj;
            return Objects.equals(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(SnapshotsStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * SnapshotsStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class SnapshotsStreamHashAidxColumn implements Persistable, Comparable<SnapshotsStreamHashAidxColumn> {
        private final long streamId;

        public static SnapshotsStreamHashAidxColumn of(long streamId) {
            return new SnapshotsStreamHashAidxColumn(streamId);
        }

        private SnapshotsStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<SnapshotsStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<SnapshotsStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(SnapshotsStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, SnapshotsStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, SnapshotsStreamHashAidxColumn>() {
                @Override
                public SnapshotsStreamHashAidxColumn apply(Long row) {
                    return SnapshotsStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<SnapshotsStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<SnapshotsStreamHashAidxColumn>() {
            @Override
            public SnapshotsStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new SnapshotsStreamHashAidxColumn(streamId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("streamId", streamId)
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
            SnapshotsStreamHashAidxColumn other = (SnapshotsStreamHashAidxColumn) obj;
            return Objects.equals(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(SnapshotsStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface SnapshotsStreamHashAidxTrigger {
        public void putSnapshotsStreamHashAidx(Multimap<SnapshotsStreamHashAidxRow, ? extends SnapshotsStreamHashAidxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long streamId};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class SnapshotsStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final SnapshotsStreamHashAidxColumn columnName;
        private final Long value;

        public static SnapshotsStreamHashAidxColumnValue of(SnapshotsStreamHashAidxColumn columnName, Long value) {
            return new SnapshotsStreamHashAidxColumnValue(columnName, value);
        }

        private SnapshotsStreamHashAidxColumnValue(SnapshotsStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public SnapshotsStreamHashAidxColumn getColumnName() {
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

        public static Function<SnapshotsStreamHashAidxColumnValue, SnapshotsStreamHashAidxColumn> getColumnNameFun() {
            return new Function<SnapshotsStreamHashAidxColumnValue, SnapshotsStreamHashAidxColumn>() {
                @Override
                public SnapshotsStreamHashAidxColumn apply(SnapshotsStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<SnapshotsStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<SnapshotsStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(SnapshotsStreamHashAidxColumnValue columnValue) {
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

    public static final class SnapshotsStreamHashAidxRowResult implements TypedRowResult {
        private final SnapshotsStreamHashAidxRow rowName;
        private final ImmutableSet<SnapshotsStreamHashAidxColumnValue> columnValues;

        public static SnapshotsStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            SnapshotsStreamHashAidxRow rowName = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<SnapshotsStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = SnapshotsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(SnapshotsStreamHashAidxColumnValue.of(col, value));
            }
            return new SnapshotsStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private SnapshotsStreamHashAidxRowResult(SnapshotsStreamHashAidxRow rowName, ImmutableSet<SnapshotsStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public SnapshotsStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<SnapshotsStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<SnapshotsStreamHashAidxRowResult, SnapshotsStreamHashAidxRow> getRowNameFun() {
            return new Function<SnapshotsStreamHashAidxRowResult, SnapshotsStreamHashAidxRow>() {
                @Override
                public SnapshotsStreamHashAidxRow apply(SnapshotsStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<SnapshotsStreamHashAidxRowResult, ImmutableSet<SnapshotsStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<SnapshotsStreamHashAidxRowResult, ImmutableSet<SnapshotsStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<SnapshotsStreamHashAidxColumnValue> apply(SnapshotsStreamHashAidxRowResult rowResult) {
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
    public void delete(SnapshotsStreamHashAidxRow row, SnapshotsStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<SnapshotsStreamHashAidxRow> rows) {
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(SnapshotsStreamHashAidxRow rowName, Iterable<SnapshotsStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(SnapshotsStreamHashAidxRow rowName, SnapshotsStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<SnapshotsStreamHashAidxRow, ? extends SnapshotsStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (SnapshotsStreamHashAidxTrigger trigger : triggers) {
            trigger.putSnapshotsStreamHashAidx(values);
        }
    }

    @Override
    public void touch(Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumn> values) {
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<SnapshotsStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(SnapshotsStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> get(Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                SnapshotsStreamHashAidxRow row = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SnapshotsStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<SnapshotsStreamHashAidxColumnValue> getRowColumns(SnapshotsStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SnapshotsStreamHashAidxColumnValue> getRowColumns(SnapshotsStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SnapshotsStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(SnapshotsStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> getRowsMultimap(Iterable<SnapshotsStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> getRowsMultimap(Iterable<SnapshotsStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<SnapshotsStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SnapshotsStreamHashAidxRow row = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, SnapshotsStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SnapshotsStreamHashAidxRow, BatchingVisitable<SnapshotsStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<SnapshotsStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamHashAidxRow, BatchingVisitable<SnapshotsStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamHashAidxRow row = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SnapshotsStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return SnapshotsStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SnapshotsStreamHashAidxRow, SnapshotsStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<SnapshotsStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SnapshotsStreamHashAidxRow row = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(e.getValue());
            SnapshotsStreamHashAidxColumnValue colValue = SnapshotsStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SnapshotsStreamHashAidxRow, Iterator<SnapshotsStreamHashAidxColumnValue>> getRowsColumnRangeIterator(Iterable<SnapshotsStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamHashAidxRow, Iterator<SnapshotsStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamHashAidxRow row = SnapshotsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SnapshotsStreamHashAidxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                SnapshotsStreamHashAidxColumn col = SnapshotsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = SnapshotsStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return SnapshotsStreamHashAidxColumnValue.of(col, val);
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

    public BatchingVisitableView<SnapshotsStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SnapshotsStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SnapshotsStreamHashAidxRowResult>() {
            @Override
            public SnapshotsStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return SnapshotsStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "rdNWsjVEIQHBTTSMqCSU9Q==";
}
