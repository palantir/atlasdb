package com.palantir.example.profile.schema.generated;

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
import com.google.common.hash.Hashing;
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
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;


public final class UserPhotosStreamValueTable implements
        AtlasDbMutablePersistentTable<UserPhotosStreamValueTable.UserPhotosStreamValueRow,
                                         UserPhotosStreamValueTable.UserPhotosStreamValueNamedColumnValue<?>,
                                         UserPhotosStreamValueTable.UserPhotosStreamValueRowResult>,
        AtlasDbNamedMutableTable<UserPhotosStreamValueTable.UserPhotosStreamValueRow,
                                    UserPhotosStreamValueTable.UserPhotosStreamValueNamedColumnValue<?>,
                                    UserPhotosStreamValueTable.UserPhotosStreamValueRowResult> {
    private final Transaction t;
    private final List<UserPhotosStreamValueTrigger> triggers;
    private final static String rawTableName = "user_photos_stream_value";
    private final String tableName;
    private final Namespace namespace;

    static UserPhotosStreamValueTable of(Transaction t, Namespace namespace) {
        return new UserPhotosStreamValueTable(t, namespace, ImmutableList.<UserPhotosStreamValueTrigger>of());
    }

    static UserPhotosStreamValueTable of(Transaction t, Namespace namespace, UserPhotosStreamValueTrigger trigger, UserPhotosStreamValueTrigger... triggers) {
        return new UserPhotosStreamValueTable(t, namespace, ImmutableList.<UserPhotosStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static UserPhotosStreamValueTable of(Transaction t, Namespace namespace, List<UserPhotosStreamValueTrigger> triggers) {
        return new UserPhotosStreamValueTable(t, namespace, triggers);
    }

    private UserPhotosStreamValueTable(Transaction t, Namespace namespace, List<UserPhotosStreamValueTrigger> triggers) {
        this.t = t;
        this.tableName = namespace.getName() + "." + rawTableName;
        this.triggers = triggers;
        this.namespace = namespace;
    }

    public static String getRawTableName() {
        return rawTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    /**
     * <pre>
     * UserPhotosStreamValueRow {
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class UserPhotosStreamValueRow implements Persistable, Comparable<UserPhotosStreamValueRow> {
        private final long id;
        private final long blockId;

        public static UserPhotosStreamValueRow of(long id, long blockId) {
            return new UserPhotosStreamValueRow(id, blockId);
        }

        private UserPhotosStreamValueRow(long id, long blockId) {
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<UserPhotosStreamValueRow, Long> getIdFun() {
            return new Function<UserPhotosStreamValueRow, Long>() {
                @Override
                public Long apply(UserPhotosStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<UserPhotosStreamValueRow, Long> getBlockIdFun() {
            return new Function<UserPhotosStreamValueRow, Long>() {
                @Override
                public Long apply(UserPhotosStreamValueRow row) {
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

        public static final Hydrator<UserPhotosStreamValueRow> BYTES_HYDRATOR = new Hydrator<UserPhotosStreamValueRow>() {
            @Override
            public UserPhotosStreamValueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return new UserPhotosStreamValueRow(id, blockId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
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
            UserPhotosStreamValueRow other = (UserPhotosStreamValueRow) obj;
            return Objects.equal(id, other.id) && Objects.equal(blockId, other.blockId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, blockId);
        }

        @Override
        public int compareTo(UserPhotosStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface UserPhotosStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements UserPhotosStreamValueNamedColumnValue<byte[]> {
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

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface UserPhotosStreamValueTrigger {
        public void putUserPhotosStreamValue(Multimap<UserPhotosStreamValueRow, ? extends UserPhotosStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class UserPhotosStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static UserPhotosStreamValueRowResult of(RowResult<byte[]> row) {
            return new UserPhotosStreamValueRowResult(row);
        }

        private UserPhotosStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public UserPhotosStreamValueRow getRowName() {
            return UserPhotosStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<UserPhotosStreamValueRowResult, UserPhotosStreamValueRow> getRowNameFun() {
            return new Function<UserPhotosStreamValueRowResult, UserPhotosStreamValueRow>() {
                @Override
                public UserPhotosStreamValueRow apply(UserPhotosStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, UserPhotosStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, UserPhotosStreamValueRowResult>() {
                @Override
                public UserPhotosStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new UserPhotosStreamValueRowResult(rowResult);
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

        public static Function<UserPhotosStreamValueRowResult, byte[]> getValueFun() {
            return new Function<UserPhotosStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(UserPhotosStreamValueRowResult rowResult) {
                    return rowResult.getValue();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Value", getValue())
                .toString();
        }
    }

    public enum UserPhotosStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<UserPhotosStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<UserPhotosStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(UserPhotosStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<UserPhotosStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, UserPhotosStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(UserPhotosStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends UserPhotosStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends UserPhotosStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<UserPhotosStreamValueRow, byte[]> getValues(Collection<UserPhotosStreamValueRow> rows) {
        Map<Cell, UserPhotosStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserPhotosStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UserPhotosStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(UserPhotosStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<UserPhotosStreamValueRow, byte[]> map) {
        Map<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserPhotosStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putValueUnlessExists(UserPhotosStreamValueRow row, byte[] value) {
        putUnlessExists(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValueUnlessExists(Map<UserPhotosStreamValueRow, byte[]> map) {
        Map<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserPhotosStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<UserPhotosStreamValueRow, ? extends UserPhotosStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows));
        for (UserPhotosStreamValueTrigger trigger : triggers) {
            trigger.putUserPhotosStreamValue(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<UserPhotosStreamValueRow, ? extends UserPhotosStreamValueNamedColumnValue<?>> rows) {
        Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<UserPhotosStreamValueRow, ? extends UserPhotosStreamValueNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteValue(UserPhotosStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<UserPhotosStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(UserPhotosStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<UserPhotosStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableName, cells);
    }

    @Override
    public Optional<UserPhotosStreamValueRowResult> getRow(UserPhotosStreamValueRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<UserPhotosStreamValueRowResult> getRow(UserPhotosStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(UserPhotosStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<UserPhotosStreamValueRowResult> getRows(Iterable<UserPhotosStreamValueRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<UserPhotosStreamValueRowResult> getRows(Iterable<UserPhotosStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<UserPhotosStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(UserPhotosStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<UserPhotosStreamValueRowResult> getAsyncRows(Iterable<UserPhotosStreamValueRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<UserPhotosStreamValueRowResult> getAsyncRows(final Iterable<UserPhotosStreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<UserPhotosStreamValueRowResult>> c =
                new Callable<List<UserPhotosStreamValueRowResult>>() {
            @Override
            public List<UserPhotosStreamValueRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<UserPhotosStreamValueNamedColumnValue<?>> getRowColumns(UserPhotosStreamValueRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<UserPhotosStreamValueNamedColumnValue<?>> getRowColumns(UserPhotosStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<UserPhotosStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<UserPhotosStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<UserPhotosStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<UserPhotosStreamValueRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<UserPhotosStreamValueRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>>> c =
                new Callable<Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>>>() {
            @Override
            public Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<UserPhotosStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<UserPhotosStreamValueRow, UserPhotosStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            UserPhotosStreamValueRow row = UserPhotosStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<UserPhotosStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<UserPhotosStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, UserPhotosStreamValueRowResult>() {
            @Override
            public UserPhotosStreamValueRowResult apply(RowResult<byte[]> input) {
                return UserPhotosStreamValueRowResult.of(input);
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

    static String __CLASS_HASH = "4DIQB23Rz3Mq9f+d8uuD8g==";
}
