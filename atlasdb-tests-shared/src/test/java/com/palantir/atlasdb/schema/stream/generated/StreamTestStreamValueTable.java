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
import com.palantir.atlasdb.schema.Namespace;
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


public final class StreamTestStreamValueTable implements
        AtlasDbMutablePersistentTable<StreamTestStreamValueTable.StreamTestStreamValueRow,
                                         StreamTestStreamValueTable.StreamTestStreamValueNamedColumnValue<?>,
                                         StreamTestStreamValueTable.StreamTestStreamValueRowResult>,
        AtlasDbNamedMutableTable<StreamTestStreamValueTable.StreamTestStreamValueRow,
                                    StreamTestStreamValueTable.StreamTestStreamValueNamedColumnValue<?>,
                                    StreamTestStreamValueTable.StreamTestStreamValueRowResult> {
    private final Transaction t;
    private final List<StreamTestStreamValueTrigger> triggers;
    private final static String rawTableName = "stream_test_stream_value";
    private final String tableName;
    private final Namespace namespace;

    static StreamTestStreamValueTable of(Transaction t, Namespace namespace) {
        return new StreamTestStreamValueTable(t, namespace, ImmutableList.<StreamTestStreamValueTrigger>of());
    }

    static StreamTestStreamValueTable of(Transaction t, Namespace namespace, StreamTestStreamValueTrigger trigger, StreamTestStreamValueTrigger... triggers) {
        return new StreamTestStreamValueTable(t, namespace, ImmutableList.<StreamTestStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestStreamValueTable of(Transaction t, Namespace namespace, List<StreamTestStreamValueTrigger> triggers) {
        return new StreamTestStreamValueTable(t, namespace, triggers);
    }

    private StreamTestStreamValueTable(Transaction t, Namespace namespace, List<StreamTestStreamValueTrigger> triggers) {
        this.t = t;
        this.tableName = namespace.getName() + "." + rawTableName;
        this.triggers = triggers;
        this.namespace = namespace;
    }

    public String getTableName() {
        return tableName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    /**
     * <pre>
     * StreamTestStreamValueRow {
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class StreamTestStreamValueRow implements Persistable, Comparable<StreamTestStreamValueRow> {
        private final long id;
        private final long blockId;

        public static StreamTestStreamValueRow of(long id, long blockId) {
            return new StreamTestStreamValueRow(id, blockId);
        }

        private StreamTestStreamValueRow(long id, long blockId) {
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<StreamTestStreamValueRow, Long> getIdFun() {
            return new Function<StreamTestStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<StreamTestStreamValueRow, Long> getBlockIdFun() {
            return new Function<StreamTestStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestStreamValueRow row) {
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

        public static final Hydrator<StreamTestStreamValueRow> BYTES_HYDRATOR = new Hydrator<StreamTestStreamValueRow>() {
            @Override
            public StreamTestStreamValueRow hydrateFromBytes(byte[] __input) {
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
            StreamTestStreamValueRow other = (StreamTestStreamValueRow) obj;
            return Objects.equal(id, other.id) && Objects.equal(blockId, other.blockId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, blockId);
        }

        @Override
        public int compareTo(StreamTestStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface StreamTestStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements StreamTestStreamValueNamedColumnValue<byte[]> {
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

    public interface StreamTestStreamValueTrigger {
        public void putStreamTestStreamValue(Multimap<StreamTestStreamValueRow, ? extends StreamTestStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class StreamTestStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTestStreamValueRowResult of(RowResult<byte[]> row) {
            return new StreamTestStreamValueRowResult(row);
        }

        private StreamTestStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTestStreamValueRow getRowName() {
            return StreamTestStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTestStreamValueRowResult, StreamTestStreamValueRow> getRowNameFun() {
            return new Function<StreamTestStreamValueRowResult, StreamTestStreamValueRow>() {
                @Override
                public StreamTestStreamValueRow apply(StreamTestStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTestStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTestStreamValueRowResult>() {
                @Override
                public StreamTestStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTestStreamValueRowResult(rowResult);
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

        public static Function<StreamTestStreamValueRowResult, byte[]> getValueFun() {
            return new Function<StreamTestStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(StreamTestStreamValueRowResult rowResult) {
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

    public enum StreamTestStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTestStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<StreamTestStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTestStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTestStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTestStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTestStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<StreamTestStreamValueRow, byte[]> getValues(Collection<StreamTestStreamValueRow> rows) {
        Map<Cell, StreamTestStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTestStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<StreamTestStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(StreamTestStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<StreamTestStreamValueRow, byte[]> map) {
        Map<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<StreamTestStreamValueRow, ? extends StreamTestStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows));
        for (StreamTestStreamValueTrigger trigger : triggers) {
            trigger.putStreamTestStreamValue(rows);
        }
    }

    public void deleteValue(StreamTestStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<StreamTestStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(StreamTestStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTestStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        ImmutableSet.Builder<Cell> cells = ImmutableSet.builder();
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableName, cells.build());
    }

    @Override
    public Optional<StreamTestStreamValueRowResult> getRow(StreamTestStreamValueRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<StreamTestStreamValueRowResult> getRow(StreamTestStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(StreamTestStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTestStreamValueRowResult> getRows(Iterable<StreamTestStreamValueRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<StreamTestStreamValueRowResult> getRows(Iterable<StreamTestStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<StreamTestStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTestStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTestStreamValueRowResult> getAsyncRows(Iterable<StreamTestStreamValueRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<StreamTestStreamValueRowResult> getAsyncRows(final Iterable<StreamTestStreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<StreamTestStreamValueRowResult>> c =
                new Callable<List<StreamTestStreamValueRowResult>>() {
            @Override
            public List<StreamTestStreamValueRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<StreamTestStreamValueNamedColumnValue<?>> getRowColumns(StreamTestStreamValueRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTestStreamValueNamedColumnValue<?>> getRowColumns(StreamTestStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<StreamTestStreamValueRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<StreamTestStreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>>> c =
                new Callable<Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>>>() {
            @Override
            public Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTestStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestStreamValueRow, StreamTestStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestStreamValueRow row = StreamTestStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTestStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTestStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestStreamValueRowResult>() {
            @Override
            public StreamTestStreamValueRowResult apply(RowResult<byte[]> input) {
                return StreamTestStreamValueRowResult.of(input);
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

    static String __CLASS_HASH = "UMFwV3hNEjXmdXjW2aFqxw==";
}
