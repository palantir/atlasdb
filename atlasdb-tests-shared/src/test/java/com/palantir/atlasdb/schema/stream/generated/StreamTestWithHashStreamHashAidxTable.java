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
public final class StreamTestWithHashStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow,
                                                StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumn,
                                                StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue,
                                                StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<StreamTestWithHashStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "stream_test_with_hash_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestWithHashStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new StreamTestWithHashStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestWithHashStreamHashAidxTrigger>of());
    }

    static StreamTestWithHashStreamHashAidxTable of(Transaction t, Namespace namespace, StreamTestWithHashStreamHashAidxTrigger trigger, StreamTestWithHashStreamHashAidxTrigger... triggers) {
        return new StreamTestWithHashStreamHashAidxTable(t, namespace, ImmutableList.<StreamTestWithHashStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestWithHashStreamHashAidxTable of(Transaction t, Namespace namespace, List<StreamTestWithHashStreamHashAidxTrigger> triggers) {
        return new StreamTestWithHashStreamHashAidxTable(t, namespace, triggers);
    }

    private StreamTestWithHashStreamHashAidxTable(Transaction t, Namespace namespace, List<StreamTestWithHashStreamHashAidxTrigger> triggers) {
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
     * StreamTestWithHashStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamHashAidxRow implements Persistable, Comparable<StreamTestWithHashStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static StreamTestWithHashStreamHashAidxRow of(Sha256Hash hash) {
            return new StreamTestWithHashStreamHashAidxRow(hash);
        }

        private StreamTestWithHashStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<StreamTestWithHashStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<StreamTestWithHashStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(StreamTestWithHashStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, StreamTestWithHashStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, StreamTestWithHashStreamHashAidxRow>() {
                @Override
                public StreamTestWithHashStreamHashAidxRow apply(Sha256Hash row) {
                    return StreamTestWithHashStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamHashAidxRow>() {
            @Override
            public StreamTestWithHashStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new StreamTestWithHashStreamHashAidxRow(hash);
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
            StreamTestWithHashStreamHashAidxRow other = (StreamTestWithHashStreamHashAidxRow) obj;
            return Objects.equals(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(StreamTestWithHashStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestWithHashStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamHashAidxColumn implements Persistable, Comparable<StreamTestWithHashStreamHashAidxColumn> {
        private final long streamId;

        public static StreamTestWithHashStreamHashAidxColumn of(long streamId) {
            return new StreamTestWithHashStreamHashAidxColumn(streamId);
        }

        private StreamTestWithHashStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<StreamTestWithHashStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<StreamTestWithHashStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, StreamTestWithHashStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, StreamTestWithHashStreamHashAidxColumn>() {
                @Override
                public StreamTestWithHashStreamHashAidxColumn apply(Long row) {
                    return StreamTestWithHashStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamHashAidxColumn>() {
            @Override
            public StreamTestWithHashStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new StreamTestWithHashStreamHashAidxColumn(streamId);
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
            StreamTestWithHashStreamHashAidxColumn other = (StreamTestWithHashStreamHashAidxColumn) obj;
            return Objects.equals(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(StreamTestWithHashStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface StreamTestWithHashStreamHashAidxTrigger {
        public void putStreamTestWithHashStreamHashAidx(Multimap<StreamTestWithHashStreamHashAidxRow, ? extends StreamTestWithHashStreamHashAidxColumnValue> newRows);
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
    public static final class StreamTestWithHashStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final StreamTestWithHashStreamHashAidxColumn columnName;
        private final Long value;

        public static StreamTestWithHashStreamHashAidxColumnValue of(StreamTestWithHashStreamHashAidxColumn columnName, Long value) {
            return new StreamTestWithHashStreamHashAidxColumnValue(columnName, value);
        }

        private StreamTestWithHashStreamHashAidxColumnValue(StreamTestWithHashStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestWithHashStreamHashAidxColumn getColumnName() {
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

        public static Function<StreamTestWithHashStreamHashAidxColumnValue, StreamTestWithHashStreamHashAidxColumn> getColumnNameFun() {
            return new Function<StreamTestWithHashStreamHashAidxColumnValue, StreamTestWithHashStreamHashAidxColumn>() {
                @Override
                public StreamTestWithHashStreamHashAidxColumn apply(StreamTestWithHashStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestWithHashStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<StreamTestWithHashStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamHashAidxColumnValue columnValue) {
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

    public static final class StreamTestWithHashStreamHashAidxRowResult implements TypedRowResult {
        private final StreamTestWithHashStreamHashAidxRow rowName;
        private final ImmutableSet<StreamTestWithHashStreamHashAidxColumnValue> columnValues;

        public static StreamTestWithHashStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestWithHashStreamHashAidxRow rowName = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestWithHashStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestWithHashStreamHashAidxColumnValue.of(col, value));
            }
            return new StreamTestWithHashStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestWithHashStreamHashAidxRowResult(StreamTestWithHashStreamHashAidxRow rowName, ImmutableSet<StreamTestWithHashStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestWithHashStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestWithHashStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestWithHashStreamHashAidxRowResult, StreamTestWithHashStreamHashAidxRow> getRowNameFun() {
            return new Function<StreamTestWithHashStreamHashAidxRowResult, StreamTestWithHashStreamHashAidxRow>() {
                @Override
                public StreamTestWithHashStreamHashAidxRow apply(StreamTestWithHashStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestWithHashStreamHashAidxRowResult, ImmutableSet<StreamTestWithHashStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestWithHashStreamHashAidxRowResult, ImmutableSet<StreamTestWithHashStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestWithHashStreamHashAidxColumnValue> apply(StreamTestWithHashStreamHashAidxRowResult rowResult) {
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
    public void delete(StreamTestWithHashStreamHashAidxRow row, StreamTestWithHashStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestWithHashStreamHashAidxRow> rows) {
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestWithHashStreamHashAidxRow rowName, Iterable<StreamTestWithHashStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestWithHashStreamHashAidxRow rowName, StreamTestWithHashStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestWithHashStreamHashAidxRow, ? extends StreamTestWithHashStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestWithHashStreamHashAidxTrigger trigger : triggers) {
            trigger.putStreamTestWithHashStreamHashAidx(values);
        }
    }

    @Override
    public void touch(Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumn> values) {
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestWithHashStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestWithHashStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> get(Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestWithHashStreamHashAidxRow row = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestWithHashStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestWithHashStreamHashAidxColumnValue> getRowColumns(StreamTestWithHashStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamHashAidxColumnValue> getRowColumns(StreamTestWithHashStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestWithHashStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestWithHashStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestWithHashStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTestWithHashStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<StreamTestWithHashStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestWithHashStreamHashAidxRow row = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestWithHashStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestWithHashStreamHashAidxRow, BatchingVisitable<StreamTestWithHashStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestWithHashStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestWithHashStreamHashAidxRow, BatchingVisitable<StreamTestWithHashStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestWithHashStreamHashAidxRow row = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestWithHashStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return StreamTestWithHashStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestWithHashStreamHashAidxRow, StreamTestWithHashStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<StreamTestWithHashStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestWithHashStreamHashAidxRow row = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(e.getValue());
            StreamTestWithHashStreamHashAidxColumnValue colValue = StreamTestWithHashStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<StreamTestWithHashStreamHashAidxRow, Iterator<StreamTestWithHashStreamHashAidxColumnValue>> getRowsColumnRangeIterator(Iterable<StreamTestWithHashStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestWithHashStreamHashAidxRow, Iterator<StreamTestWithHashStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestWithHashStreamHashAidxRow row = StreamTestWithHashStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<StreamTestWithHashStreamHashAidxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                StreamTestWithHashStreamHashAidxColumn col = StreamTestWithHashStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestWithHashStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return StreamTestWithHashStreamHashAidxColumnValue.of(col, val);
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

    public BatchingVisitableView<StreamTestWithHashStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestWithHashStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, StreamTestWithHashStreamHashAidxRowResult>() {
            @Override
            public StreamTestWithHashStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return StreamTestWithHashStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "N1ZVUKBp5e4EiyZN3lOukQ==";
}
