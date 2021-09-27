package com.palantir.atlasdb.schema.stream.generated;

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
public final class StreamTestStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
                                                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn,
                                                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue,
                                                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<StreamTestStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "stream_test_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new StreamTestStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestStreamHashAidxTrigger>of());
    }

    static StreamTestStreamHashAidxTable of(Transaction t, Namespace namespace, StreamTestStreamHashAidxTrigger trigger, StreamTestStreamHashAidxTrigger... triggers) {
        return new StreamTestStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestStreamHashAidxTable of(Transaction t, Namespace namespace, List<StreamTestStreamHashAidxTrigger> triggers) {
        return new StreamTestStreamHashAidxTable(t, namespace, triggers);
    }

    private StreamTestStreamHashAidxTable(Transaction t, Namespace namespace, List<StreamTestStreamHashAidxTrigger> triggers) {
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
     * StreamTestStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class StreamTestStreamHashAidxRow implements Persistable, Comparable<StreamTestStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static StreamTestStreamHashAidxRow of(Sha256Hash hash) {
            return new StreamTestStreamHashAidxRow(hash);
        }

        private StreamTestStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<StreamTestStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<StreamTestStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(StreamTestStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, StreamTestStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, StreamTestStreamHashAidxRow>() {
                @Override
                public StreamTestStreamHashAidxRow apply(Sha256Hash row) {
                    return StreamTestStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<StreamTestStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<StreamTestStreamHashAidxRow>() {
            @Override
            public StreamTestStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new StreamTestStreamHashAidxRow(hash);
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
            StreamTestStreamHashAidxRow other = (StreamTestStreamHashAidxRow) obj;
            return Objects.equals(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(StreamTestStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class StreamTestStreamHashAidxColumn implements Persistable, Comparable<StreamTestStreamHashAidxColumn> {
        private final long streamId;

        public static StreamTestStreamHashAidxColumn of(long streamId) {
            return new StreamTestStreamHashAidxColumn(streamId);
        }

        private StreamTestStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<StreamTestStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<StreamTestStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(StreamTestStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, StreamTestStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, StreamTestStreamHashAidxColumn>() {
                @Override
                public StreamTestStreamHashAidxColumn apply(Long row) {
                    return StreamTestStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<StreamTestStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestStreamHashAidxColumn>() {
            @Override
            public StreamTestStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new StreamTestStreamHashAidxColumn(streamId);
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
            StreamTestStreamHashAidxColumn other = (StreamTestStreamHashAidxColumn) obj;
            return Objects.equals(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(StreamTestStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface StreamTestStreamHashAidxTrigger {
        public void putStreamTestStreamHashAidx(Multimap<StreamTestStreamHashAidxRow, ? extends StreamTestStreamHashAidxColumnValue> newRows);
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
    public static final class StreamTestStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final StreamTestStreamHashAidxColumn columnName;
        private final Long value;

        public static StreamTestStreamHashAidxColumnValue of(StreamTestStreamHashAidxColumn columnName, Long value) {
            return new StreamTestStreamHashAidxColumnValue(columnName, value);
        }

        private StreamTestStreamHashAidxColumnValue(StreamTestStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestStreamHashAidxColumn getColumnName() {
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

        public static Function<StreamTestStreamHashAidxColumnValue, StreamTestStreamHashAidxColumn> getColumnNameFun() {
            return new Function<StreamTestStreamHashAidxColumnValue, StreamTestStreamHashAidxColumn>() {
                @Override
                public StreamTestStreamHashAidxColumn apply(StreamTestStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<StreamTestStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestStreamHashAidxColumnValue columnValue) {
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

    public static final class StreamTestStreamHashAidxRowResult implements TypedRowResult {
        private final StreamTestStreamHashAidxRow rowName;
        private final ImmutableSet<StreamTestStreamHashAidxColumnValue> columnValues;

        public static StreamTestStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestStreamHashAidxRow rowName = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestStreamHashAidxColumnValue.of(col, value));
            }
            return new StreamTestStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestStreamHashAidxRowResult(StreamTestStreamHashAidxRow rowName, ImmutableSet<StreamTestStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestStreamHashAidxRowResult, StreamTestStreamHashAidxRow> getRowNameFun() {
            return new Function<StreamTestStreamHashAidxRowResult, StreamTestStreamHashAidxRow>() {
                @Override
                public StreamTestStreamHashAidxRow apply(StreamTestStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestStreamHashAidxRowResult, ImmutableSet<StreamTestStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestStreamHashAidxRowResult, ImmutableSet<StreamTestStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestStreamHashAidxColumnValue> apply(StreamTestStreamHashAidxRowResult rowResult) {
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
    public void delete(StreamTestStreamHashAidxRow row, StreamTestStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestStreamHashAidxRow> rows) {
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestStreamHashAidxRow rowName, Iterable<StreamTestStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestStreamHashAidxRow rowName, StreamTestStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestStreamHashAidxRow, ? extends StreamTestStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestStreamHashAidxTrigger trigger : triggers) {
            trigger.putStreamTestStreamHashAidx(values);
        }
    }

    @Override
    public void touch(Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumn> values) {
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> get(Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestStreamHashAidxRow row = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestStreamHashAidxColumnValue> getRowColumns(StreamTestStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestStreamHashAidxColumnValue> getRowColumns(StreamTestStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<StreamTestStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestStreamHashAidxRow row = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestStreamHashAidxRow, BatchingVisitable<StreamTestStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestStreamHashAidxRow, BatchingVisitable<StreamTestStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestStreamHashAidxRow row = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return StreamTestStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestStreamHashAidxRow, StreamTestStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestStreamHashAidxRow row = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(e.getValue());
            StreamTestStreamHashAidxColumnValue colValue = StreamTestStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<StreamTestStreamHashAidxRow, Iterator<StreamTestStreamHashAidxColumnValue>> getRowsColumnRangeIterator(Iterable<StreamTestStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestStreamHashAidxRow, Iterator<StreamTestStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestStreamHashAidxRow row = StreamTestStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<StreamTestStreamHashAidxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                StreamTestStreamHashAidxColumn col = StreamTestStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return StreamTestStreamHashAidxColumnValue.of(col, val);
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

    public BatchingVisitableView<StreamTestStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, StreamTestStreamHashAidxRowResult>() {
            @Override
            public StreamTestStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return StreamTestStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "udUmVCU5CjaqCML8TN5H0g==";
}
