package com.palantir.atlasdb.schema.stream.generated;

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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedExpiringSet;
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
public final class StreamTestMaxMemStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxRow,
                                                StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxColumn,
                                                StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxColumnValue,
                                                StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<StreamTestMaxMemStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "stream_test_max_mem_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestMaxMemStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new StreamTestMaxMemStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamHashAidxTrigger>of());
    }

    static StreamTestMaxMemStreamHashAidxTable of(Transaction t, Namespace namespace, StreamTestMaxMemStreamHashAidxTrigger trigger, StreamTestMaxMemStreamHashAidxTrigger... triggers) {
        return new StreamTestMaxMemStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestMaxMemStreamHashAidxTable of(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamHashAidxTrigger> triggers) {
        return new StreamTestMaxMemStreamHashAidxTable(t, namespace, triggers);
    }

    private StreamTestMaxMemStreamHashAidxTable(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamHashAidxTrigger> triggers) {
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
     * StreamTestMaxMemStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class StreamTestMaxMemStreamHashAidxRow implements Persistable, Comparable<StreamTestMaxMemStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static StreamTestMaxMemStreamHashAidxRow of(Sha256Hash hash) {
            return new StreamTestMaxMemStreamHashAidxRow(hash);
        }

        private StreamTestMaxMemStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<StreamTestMaxMemStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<StreamTestMaxMemStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(StreamTestMaxMemStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, StreamTestMaxMemStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, StreamTestMaxMemStreamHashAidxRow>() {
                @Override
                public StreamTestMaxMemStreamHashAidxRow apply(Sha256Hash row) {
                    return StreamTestMaxMemStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<StreamTestMaxMemStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<StreamTestMaxMemStreamHashAidxRow>() {
            @Override
            public StreamTestMaxMemStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new StreamTestMaxMemStreamHashAidxRow(hash);
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
            StreamTestMaxMemStreamHashAidxRow other = (StreamTestMaxMemStreamHashAidxRow) obj;
            return Objects.equal(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(StreamTestMaxMemStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestMaxMemStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class StreamTestMaxMemStreamHashAidxColumn implements Persistable, Comparable<StreamTestMaxMemStreamHashAidxColumn> {
        private final long streamId;

        public static StreamTestMaxMemStreamHashAidxColumn of(long streamId) {
            return new StreamTestMaxMemStreamHashAidxColumn(streamId);
        }

        private StreamTestMaxMemStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<StreamTestMaxMemStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<StreamTestMaxMemStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, StreamTestMaxMemStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, StreamTestMaxMemStreamHashAidxColumn>() {
                @Override
                public StreamTestMaxMemStreamHashAidxColumn apply(Long row) {
                    return StreamTestMaxMemStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<StreamTestMaxMemStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestMaxMemStreamHashAidxColumn>() {
            @Override
            public StreamTestMaxMemStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new StreamTestMaxMemStreamHashAidxColumn(streamId);
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
            StreamTestMaxMemStreamHashAidxColumn other = (StreamTestMaxMemStreamHashAidxColumn) obj;
            return Objects.equal(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(StreamTestMaxMemStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface StreamTestMaxMemStreamHashAidxTrigger {
        public void putStreamTestMaxMemStreamHashAidx(Multimap<StreamTestMaxMemStreamHashAidxRow, ? extends StreamTestMaxMemStreamHashAidxColumnValue> newRows);
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
    public static final class StreamTestMaxMemStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final StreamTestMaxMemStreamHashAidxColumn columnName;
        private final Long value;

        public static StreamTestMaxMemStreamHashAidxColumnValue of(StreamTestMaxMemStreamHashAidxColumn columnName, Long value) {
            return new StreamTestMaxMemStreamHashAidxColumnValue(columnName, value);
        }

        private StreamTestMaxMemStreamHashAidxColumnValue(StreamTestMaxMemStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestMaxMemStreamHashAidxColumn getColumnName() {
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

        public static Function<StreamTestMaxMemStreamHashAidxColumnValue, StreamTestMaxMemStreamHashAidxColumn> getColumnNameFun() {
            return new Function<StreamTestMaxMemStreamHashAidxColumnValue, StreamTestMaxMemStreamHashAidxColumn>() {
                @Override
                public StreamTestMaxMemStreamHashAidxColumn apply(StreamTestMaxMemStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestMaxMemStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<StreamTestMaxMemStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamHashAidxColumnValue columnValue) {
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

    public static final class StreamTestMaxMemStreamHashAidxRowResult implements TypedRowResult {
        private final StreamTestMaxMemStreamHashAidxRow rowName;
        private final ImmutableSet<StreamTestMaxMemStreamHashAidxColumnValue> columnValues;

        public static StreamTestMaxMemStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestMaxMemStreamHashAidxRow rowName = StreamTestMaxMemStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestMaxMemStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestMaxMemStreamHashAidxColumnValue.of(col, value));
            }
            return new StreamTestMaxMemStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestMaxMemStreamHashAidxRowResult(StreamTestMaxMemStreamHashAidxRow rowName, ImmutableSet<StreamTestMaxMemStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestMaxMemStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestMaxMemStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestMaxMemStreamHashAidxRowResult, StreamTestMaxMemStreamHashAidxRow> getRowNameFun() {
            return new Function<StreamTestMaxMemStreamHashAidxRowResult, StreamTestMaxMemStreamHashAidxRow>() {
                @Override
                public StreamTestMaxMemStreamHashAidxRow apply(StreamTestMaxMemStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestMaxMemStreamHashAidxRowResult, ImmutableSet<StreamTestMaxMemStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestMaxMemStreamHashAidxRowResult, ImmutableSet<StreamTestMaxMemStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestMaxMemStreamHashAidxColumnValue> apply(StreamTestMaxMemStreamHashAidxRowResult rowResult) {
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
    public void delete(StreamTestMaxMemStreamHashAidxRow row, StreamTestMaxMemStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestMaxMemStreamHashAidxRow> rows) {
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestMaxMemStreamHashAidxRow rowName, Iterable<StreamTestMaxMemStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestMaxMemStreamHashAidxRow rowName, StreamTestMaxMemStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestMaxMemStreamHashAidxRow, ? extends StreamTestMaxMemStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestMaxMemStreamHashAidxTrigger trigger : triggers) {
            trigger.putStreamTestMaxMemStreamHashAidx(values);
        }
    }

    @Override
    public void putUnlessExists(StreamTestMaxMemStreamHashAidxRow rowName, Iterable<StreamTestMaxMemStreamHashAidxColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(StreamTestMaxMemStreamHashAidxRow rowName, StreamTestMaxMemStreamHashAidxColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(Multimap<StreamTestMaxMemStreamHashAidxRow, ? extends StreamTestMaxMemStreamHashAidxColumnValue> rows) {
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> toGet = Multimaps.transformValues(rows, StreamTestMaxMemStreamHashAidxColumnValue.getColumnNameFun());
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> existing = get(toGet);
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> toPut = HashMultimap.create();
        for (Entry<StreamTestMaxMemStreamHashAidxRow, ? extends StreamTestMaxMemStreamHashAidxColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> values) {
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestMaxMemStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestMaxMemStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> get(Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestMaxMemStreamHashAidxRow row = StreamTestMaxMemStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestMaxMemStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestMaxMemStreamHashAidxColumnValue> getRowColumns(StreamTestMaxMemStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestMaxMemStreamHashAidxColumnValue> getRowColumns(StreamTestMaxMemStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestMaxMemStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestMaxMemStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestMaxMemStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestMaxMemStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<StreamTestMaxMemStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestMaxMemStreamHashAidxRow row = StreamTestMaxMemStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestMaxMemStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestMaxMemStreamHashAidxRow, BatchingVisitable<StreamTestMaxMemStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestMaxMemStreamHashAidxRow, BatchingVisitable<StreamTestMaxMemStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestMaxMemStreamHashAidxRow row = StreamTestMaxMemStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestMaxMemStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return StreamTestMaxMemStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestMaxMemStreamHashAidxRow, StreamTestMaxMemStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestMaxMemStreamHashAidxRow row = StreamTestMaxMemStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestMaxMemStreamHashAidxColumn col = StreamTestMaxMemStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestMaxMemStreamHashAidxColumnValue.hydrateValue(e.getValue());
            StreamTestMaxMemStreamHashAidxColumnValue colValue = StreamTestMaxMemStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<StreamTestMaxMemStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestMaxMemStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestMaxMemStreamHashAidxRowResult>() {
            @Override
            public StreamTestMaxMemStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return StreamTestMaxMemStreamHashAidxRowResult.of(input);
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
     * {@link AtlasDbDynamicMutableExpiringTable}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutableExpiringTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedExpiringSet}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
     * {@link BatchColumnRangeSelection}
     * {@link BatchingVisitable}
     * {@link BatchingVisitableView}
     * {@link BatchingVisitables}
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
     * {@link Supplier}
     * {@link TableReference}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "xA43as12b80ZIvV4x8/caQ==";
}
