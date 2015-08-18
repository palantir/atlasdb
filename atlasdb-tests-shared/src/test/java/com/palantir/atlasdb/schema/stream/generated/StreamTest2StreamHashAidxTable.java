package com.palantir.atlasdb.schema.stream.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Prefix;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
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
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;


public final class StreamTest2StreamHashAidxTable implements
        AtlasDbDynamicMutableExpiringTable<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow,
                                              StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumn,
                                              StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue,
                                              StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRowResult> {
    private final Transaction t;
    private final List<StreamTest2StreamHashAidxTrigger> triggers;
    private final static String tableName = "default.stream_test_2_stream_hash_idx";

    static StreamTest2StreamHashAidxTable of(Transaction t) {
        return new StreamTest2StreamHashAidxTable(t, ImmutableList.<StreamTest2StreamHashAidxTrigger>of());
    }

    static StreamTest2StreamHashAidxTable of(Transaction t, StreamTest2StreamHashAidxTrigger trigger, StreamTest2StreamHashAidxTrigger... triggers) {
        return new StreamTest2StreamHashAidxTable(t, ImmutableList.<StreamTest2StreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTest2StreamHashAidxTable of(Transaction t, List<StreamTest2StreamHashAidxTrigger> triggers) {
        return new StreamTest2StreamHashAidxTable(t, triggers);
    }

    private StreamTest2StreamHashAidxTable(Transaction t, List<StreamTest2StreamHashAidxTrigger> triggers) {
        this.t = t;
        this.triggers = triggers;
    }

    public static String getTableName() {
        return tableName;
    }

    /**
     * <pre>
     * StreamTest2StreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamHashAidxRow implements Persistable, Comparable<StreamTest2StreamHashAidxRow> {
        private final Sha256Hash hash;

        public static StreamTest2StreamHashAidxRow of(Sha256Hash hash) {
            return new StreamTest2StreamHashAidxRow(hash);
        }

        private StreamTest2StreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<StreamTest2StreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<StreamTest2StreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(StreamTest2StreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, StreamTest2StreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, StreamTest2StreamHashAidxRow>() {
                @Override
                public StreamTest2StreamHashAidxRow apply(Sha256Hash row) {
                    return new StreamTest2StreamHashAidxRow(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<StreamTest2StreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamHashAidxRow>() {
            @Override
            public StreamTest2StreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return of(hash);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
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
            StreamTest2StreamHashAidxRow other = (StreamTest2StreamHashAidxRow) obj;
            return Objects.equal(hash, other.hash);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(StreamTest2StreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTest2StreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamHashAidxColumn implements Persistable, Comparable<StreamTest2StreamHashAidxColumn> {
        private final long streamId;

        public static StreamTest2StreamHashAidxColumn of(long streamId) {
            return new StreamTest2StreamHashAidxColumn(streamId);
        }

        private StreamTest2StreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<StreamTest2StreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<StreamTest2StreamHashAidxColumn, Long>() {
                @Override
                public Long apply(StreamTest2StreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, StreamTest2StreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, StreamTest2StreamHashAidxColumn>() {
                @Override
                public StreamTest2StreamHashAidxColumn apply(Long row) {
                    return new StreamTest2StreamHashAidxColumn(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<StreamTest2StreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamHashAidxColumn>() {
            @Override
            public StreamTest2StreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return of(streamId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
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
            StreamTest2StreamHashAidxColumn other = (StreamTest2StreamHashAidxColumn) obj;
            return Objects.equal(streamId, other.streamId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(StreamTest2StreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface StreamTest2StreamHashAidxTrigger {
        public void putStreamTest2StreamHashAidx(Multimap<StreamTest2StreamHashAidxRow, ? extends StreamTest2StreamHashAidxColumnValue> newRows);
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
    public static final class StreamTest2StreamHashAidxColumnValue implements ColumnValue<Long> {
        private final StreamTest2StreamHashAidxColumn columnName;
        private final Long value;

        public static StreamTest2StreamHashAidxColumnValue of(StreamTest2StreamHashAidxColumn columnName, Long value) {
            return new StreamTest2StreamHashAidxColumnValue(columnName, value);
        }

        private StreamTest2StreamHashAidxColumnValue(StreamTest2StreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTest2StreamHashAidxColumn getColumnName() {
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

        public static Function<StreamTest2StreamHashAidxColumnValue, StreamTest2StreamHashAidxColumn> getColumnNameFun() {
            return new Function<StreamTest2StreamHashAidxColumnValue, StreamTest2StreamHashAidxColumn>() {
                @Override
                public StreamTest2StreamHashAidxColumn apply(StreamTest2StreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTest2StreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<StreamTest2StreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(StreamTest2StreamHashAidxColumnValue columnValue) {
                    return columnValue.getValue();
                }
            };
        }
    }

    public static final class StreamTest2StreamHashAidxRowResult implements TypedRowResult {
        private final StreamTest2StreamHashAidxRow rowName;
        private final ImmutableSet<StreamTest2StreamHashAidxColumnValue> columnValues;

        public static StreamTest2StreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            StreamTest2StreamHashAidxRow rowName = StreamTest2StreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTest2StreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamHashAidxColumn col = StreamTest2StreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTest2StreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTest2StreamHashAidxColumnValue.of(col, value));
            }
            return new StreamTest2StreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTest2StreamHashAidxRowResult(StreamTest2StreamHashAidxRow rowName, ImmutableSet<StreamTest2StreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTest2StreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<StreamTest2StreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTest2StreamHashAidxRowResult, StreamTest2StreamHashAidxRow> getRowNameFun() {
            return new Function<StreamTest2StreamHashAidxRowResult, StreamTest2StreamHashAidxRow>() {
                @Override
                public StreamTest2StreamHashAidxRow apply(StreamTest2StreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTest2StreamHashAidxRowResult, ImmutableSet<StreamTest2StreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTest2StreamHashAidxRowResult, ImmutableSet<StreamTest2StreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTest2StreamHashAidxColumnValue> apply(StreamTest2StreamHashAidxRowResult rowResult) {
                    return rowResult.columnValues;
                }
            };
        }
    }

    @Override
    public void delete(StreamTest2StreamHashAidxRow row, StreamTest2StreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTest2StreamHashAidxRow> rows) {
        Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumn> values) {
        t.delete(tableName, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTest2StreamHashAidxRow rowName, Iterable<StreamTest2StreamHashAidxColumnValue> values, long duration, TimeUnit unit) {
        put(ImmutableMultimap.<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(long duration, TimeUnit unit, StreamTest2StreamHashAidxRow rowName, StreamTest2StreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(Multimap<StreamTest2StreamHashAidxRow, ? extends StreamTest2StreamHashAidxColumnValue> values, long duration, TimeUnit unit) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(values, duration, unit));
        for (StreamTest2StreamHashAidxTrigger trigger : triggers) {
            trigger.putStreamTest2StreamHashAidx(values);
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTest2StreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTest2StreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> get(Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableName, rawCells);
        Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTest2StreamHashAidxRow row = StreamTest2StreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTest2StreamHashAidxColumn col = StreamTest2StreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTest2StreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getAsync(final Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumn> cells, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> call() {
                return get(cells);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    @Override
    public List<StreamTest2StreamHashAidxColumnValue> getRowColumns(StreamTest2StreamHashAidxRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamHashAidxColumnValue> getRowColumns(StreamTest2StreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTest2StreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamHashAidxColumn col = StreamTest2StreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTest2StreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getAsyncRowsMultimap(Iterable<StreamTest2StreamHashAidxRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getAsyncRowsMultimap(final Iterable<StreamTest2StreamHashAidxRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<StreamTest2StreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTest2StreamHashAidxRow, StreamTest2StreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTest2StreamHashAidxRow row = StreamTest2StreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTest2StreamHashAidxColumn col = StreamTest2StreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTest2StreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTest2StreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTest2StreamHashAidxRowResult>() {
            @Override
            public StreamTest2StreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return StreamTest2StreamHashAidxRowResult.of(input);
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
     * {@link AsyncProxy}
     * {@link AtlasDbConstraintCheckingMode}
     * {@link AtlasDbDynamicMutableExpiringTable}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutableExpiringTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedExpiringSet}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
     * {@link BatchingVisitable}
     * {@link BatchingVisitableView}
     * {@link BatchingVisitables}
     * {@link Bytes}
     * {@link Callable}
     * {@link Cell}
     * {@link Cells}
     * {@link Collection}
     * {@link Collections2}
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
     * {@link ExecutorService}
     * {@link Function}
     * {@link HashMultimap}
     * {@link HashSet}
     * {@link Hydrator}
     * {@link ImmutableList}
     * {@link ImmutableMap}
     * {@link ImmutableMultimap}
     * {@link ImmutableSet}
     * {@link InvalidProtocolBufferException}
     * {@link IterableView}
     * {@link Iterables}
     * {@link Iterator}
     * {@link Joiner}
     * {@link List}
     * {@link Lists}
     * {@link Map}
     * {@link Maps}
     * {@link MoreObjects}
     * {@link Multimap}
     * {@link Multimaps}
     * {@link NamedColumnValue}
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
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     */
    static String __CLASS_HASH = "hjznQy3MV1bYbLuNJ6QsTQ==";
}
