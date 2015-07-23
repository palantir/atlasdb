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


public final class StreamTest2StreamValueTable implements
        AtlasDbMutableExpiringTable<StreamTest2StreamValueTable.StreamTest2StreamValueRow,
                                       StreamTest2StreamValueTable.StreamTest2StreamValueNamedColumnValue<?>,
                                       StreamTest2StreamValueTable.StreamTest2StreamValueRowResult>,
        AtlasDbNamedMutableTable<StreamTest2StreamValueTable.StreamTest2StreamValueRow,
                                    StreamTest2StreamValueTable.StreamTest2StreamValueNamedColumnValue<?>,
                                    StreamTest2StreamValueTable.StreamTest2StreamValueRowResult> {
    private final Transaction t;
    private final List<StreamTest2StreamValueTrigger> triggers;
    private final static String tableName = "stream_test_2_stream_value";

    static StreamTest2StreamValueTable of(Transaction t) {
        return new StreamTest2StreamValueTable(t, ImmutableList.<StreamTest2StreamValueTrigger>of());
    }

    static StreamTest2StreamValueTable of(Transaction t, StreamTest2StreamValueTrigger trigger, StreamTest2StreamValueTrigger... triggers) {
        return new StreamTest2StreamValueTable(t, ImmutableList.<StreamTest2StreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTest2StreamValueTable of(Transaction t, List<StreamTest2StreamValueTrigger> triggers) {
        return new StreamTest2StreamValueTable(t, triggers);
    }

    private StreamTest2StreamValueTable(Transaction t, List<StreamTest2StreamValueTrigger> triggers) {
        this.t = t;
        this.triggers = triggers;
    }

    public static String getTableName() {
        return tableName;
    }

    /**
     * <pre>
     * StreamTest2StreamValueRow {
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamValueRow implements Persistable, Comparable<StreamTest2StreamValueRow> {
        private final long id;
        private final long blockId;

        public static StreamTest2StreamValueRow of(long id, long blockId) {
            return new StreamTest2StreamValueRow(id, blockId);
        }

        private StreamTest2StreamValueRow(long id, long blockId) {
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<StreamTest2StreamValueRow, Long> getIdFun() {
            return new Function<StreamTest2StreamValueRow, Long>() {
                @Override
                public Long apply(StreamTest2StreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<StreamTest2StreamValueRow, Long> getBlockIdFun() {
            return new Function<StreamTest2StreamValueRow, Long>() {
                @Override
                public Long apply(StreamTest2StreamValueRow row) {
                    return row.blockId;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
            return EncodingUtils.add(idBytes, blockIdBytes);
        }

        public static final Hydrator<StreamTest2StreamValueRow> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamValueRow>() {
            @Override
            public StreamTest2StreamValueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return of(id, blockId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("blockId", blockId)
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
            StreamTest2StreamValueRow other = (StreamTest2StreamValueRow) obj;
            return Objects.equal(id, other.id) && Objects.equal(blockId, other.blockId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, blockId);
        }

        @Override
        public int compareTo(StreamTest2StreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface StreamTest2StreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements StreamTest2StreamValueNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Value of(byte[] value) {
            return new Value(value);
        }

        private Value(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "value";
        }

        @Override
        public String getShortColumnName() {
            return "v";
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
            return PtBytes.toCachedBytes("v");
        }

        public static final Hydrator<Value> BYTES_HYDRATOR = new Hydrator<Value>() {
            @Override
            public Value hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.getBytesFromOffsetToEnd(bytes, 0));
            }
        };
    }

    public interface StreamTest2StreamValueTrigger {
        public void putStreamTest2StreamValue(Multimap<StreamTest2StreamValueRow, ? extends StreamTest2StreamValueNamedColumnValue<?>> newRows);
    }

    public static final class StreamTest2StreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTest2StreamValueRowResult of(RowResult<byte[]> row) {
            return new StreamTest2StreamValueRowResult(row);
        }

        private StreamTest2StreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTest2StreamValueRow getRowName() {
            return StreamTest2StreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTest2StreamValueRowResult, StreamTest2StreamValueRow> getRowNameFun() {
            return new Function<StreamTest2StreamValueRowResult, StreamTest2StreamValueRow>() {
                @Override
                public StreamTest2StreamValueRow apply(StreamTest2StreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTest2StreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTest2StreamValueRowResult>() {
                @Override
                public StreamTest2StreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTest2StreamValueRowResult(rowResult);
                }
            };
        }

        public boolean hasValue() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("v"));
        }

        public byte[] getValue() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("v"));
            if (bytes == null) {
                return null;
            }
            Value value = Value.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<StreamTest2StreamValueRowResult, byte[]> getValueFun() {
            return new Function<StreamTest2StreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(StreamTest2StreamValueRowResult rowResult) {
                    return rowResult.getValue();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("RowName", getRowName())
                    .add("Value", getValue())
                .toString();
        }
    }

    public enum StreamTest2StreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTest2StreamValueNamedColumn, byte[]> toShortName() {
            return new Function<StreamTest2StreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTest2StreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTest2StreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTest2StreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTest2StreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTest2StreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTest2StreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<StreamTest2StreamValueRow, byte[]> getValues(Collection<StreamTest2StreamValueRow> rows) {
        Map<Cell, StreamTest2StreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTest2StreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<StreamTest2StreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(StreamTest2StreamValueRow row, byte[] value, long duration, TimeUnit unit) {
        put(ImmutableMultimap.of(row, Value.of(value)), duration, unit);
    }

    public void putValue(Map<StreamTest2StreamValueRow, byte[]> map, long duration, TimeUnit unit) {
        Map<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTest2StreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut), duration, unit);
    }

    @Override
    public void put(Multimap<StreamTest2StreamValueRow, ? extends StreamTest2StreamValueNamedColumnValue<?>> rows, long duration, TimeUnit unit) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows, duration, unit));
        for (StreamTest2StreamValueTrigger trigger : triggers) {
            trigger.putStreamTest2StreamValue(rows);
        }
    }

    public void deleteValue(StreamTest2StreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<StreamTest2StreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(StreamTest2StreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTest2StreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        ImmutableSet.Builder<Cell> cells = ImmutableSet.builder();
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableName, cells.build());
    }

    @Override
    public Optional<StreamTest2StreamValueRowResult> getRow(StreamTest2StreamValueRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<StreamTest2StreamValueRowResult> getRow(StreamTest2StreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(StreamTest2StreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTest2StreamValueRowResult> getRows(Iterable<StreamTest2StreamValueRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamValueRowResult> getRows(Iterable<StreamTest2StreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<StreamTest2StreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTest2StreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTest2StreamValueRowResult> getAsyncRows(Iterable<StreamTest2StreamValueRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<StreamTest2StreamValueRowResult> getAsyncRows(final Iterable<StreamTest2StreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<StreamTest2StreamValueRowResult>> c =
                new Callable<List<StreamTest2StreamValueRowResult>>() {
            @Override
            public List<StreamTest2StreamValueRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<StreamTest2StreamValueNamedColumnValue<?>> getRowColumns(StreamTest2StreamValueRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamValueNamedColumnValue<?>> getRowColumns(StreamTest2StreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTest2StreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTest2StreamValueRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTest2StreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<StreamTest2StreamValueRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<StreamTest2StreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>>> c =
                new Callable<Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>>>() {
            @Override
            public Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTest2StreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTest2StreamValueRow, StreamTest2StreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTest2StreamValueRow row = StreamTest2StreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTest2StreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTest2StreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTest2StreamValueRowResult>() {
            @Override
            public StreamTest2StreamValueRowResult apply(RowResult<byte[]> input) {
                return StreamTest2StreamValueRowResult.of(input);
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
    static String __CLASS_HASH = "ULoKB/K+oKCRY1ZU9K42aA==";
}
