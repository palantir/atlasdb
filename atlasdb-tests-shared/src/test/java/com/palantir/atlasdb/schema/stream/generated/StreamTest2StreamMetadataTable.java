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


public final class StreamTest2StreamMetadataTable implements
        AtlasDbMutableExpiringTable<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow,
                                       StreamTest2StreamMetadataTable.StreamTest2StreamMetadataNamedColumnValue<?>,
                                       StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRowResult>,
        AtlasDbNamedMutableTable<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow,
                                    StreamTest2StreamMetadataTable.StreamTest2StreamMetadataNamedColumnValue<?>,
                                    StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRowResult> {
    private final Transaction t;
    private final List<StreamTest2StreamMetadataTrigger> triggers;
    private final static String rawTableName = "stream_test_2_stream_metadata";
    private final String tableName;
    private final Namespace namespace;

    static StreamTest2StreamMetadataTable of(Transaction t, Namespace namespace) {
        return new StreamTest2StreamMetadataTable(t, namespace, ImmutableList.<StreamTest2StreamMetadataTrigger>of());
    }

    static StreamTest2StreamMetadataTable of(Transaction t, Namespace namespace, StreamTest2StreamMetadataTrigger trigger, StreamTest2StreamMetadataTrigger... triggers) {
        return new StreamTest2StreamMetadataTable(t, namespace, ImmutableList.<StreamTest2StreamMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTest2StreamMetadataTable of(Transaction t, Namespace namespace, List<StreamTest2StreamMetadataTrigger> triggers) {
        return new StreamTest2StreamMetadataTable(t, namespace, triggers);
    }

    private StreamTest2StreamMetadataTable(Transaction t, Namespace namespace, List<StreamTest2StreamMetadataTrigger> triggers) {
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
     * StreamTest2StreamMetadataRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamMetadataRow implements Persistable, Comparable<StreamTest2StreamMetadataRow> {
        private final long id;

        public static StreamTest2StreamMetadataRow of(long id) {
            return new StreamTest2StreamMetadataRow(id);
        }

        private StreamTest2StreamMetadataRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTest2StreamMetadataRow, Long> getIdFun() {
            return new Function<StreamTest2StreamMetadataRow, Long>() {
                @Override
                public Long apply(StreamTest2StreamMetadataRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTest2StreamMetadataRow> fromIdFun() {
            return new Function<Long, StreamTest2StreamMetadataRow>() {
                @Override
                public StreamTest2StreamMetadataRow apply(Long row) {
                    return StreamTest2StreamMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<StreamTest2StreamMetadataRow> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamMetadataRow>() {
            @Override
            public StreamTest2StreamMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTest2StreamMetadataRow(id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("id", id)
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
            StreamTest2StreamMetadataRow other = (StreamTest2StreamMetadataRow) obj;
            return Objects.equal(id, other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(StreamTest2StreamMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface StreamTest2StreamMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
     *   name: "StreamMetadata"
     *   field {
     *     name: "status"
     *     number: 1
     *     label: LABEL_REQUIRED
     *     type: TYPE_ENUM
     *     type_name: ".com.palantir.atlasdb.protos.generated.Status"
     *   }
     *   field {
     *     name: "length"
     *     number: 2
     *     label: LABEL_REQUIRED
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "hash"
     *     number: 3
     *     label: LABEL_REQUIRED
     *     type: TYPE_BYTES
     *   }
     * }
     * </pre>
     */
    public static final class Metadata implements StreamTest2StreamMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> {
        private final com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value;

        public static Metadata of(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
            return new Metadata(value);
        }

        private Metadata(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "metadata";
        }

        @Override
        public String getShortColumnName() {
            return "md";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("md");
        }

        public static final Hydrator<Metadata> BYTES_HYDRATOR = new Hydrator<Metadata>() {
            @Override
            public Metadata hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface StreamTest2StreamMetadataTrigger {
        public void putStreamTest2StreamMetadata(Multimap<StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataNamedColumnValue<?>> newRows);
    }

    public static final class StreamTest2StreamMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTest2StreamMetadataRowResult of(RowResult<byte[]> row) {
            return new StreamTest2StreamMetadataRowResult(row);
        }

        private StreamTest2StreamMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTest2StreamMetadataRow getRowName() {
            return StreamTest2StreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTest2StreamMetadataRowResult, StreamTest2StreamMetadataRow> getRowNameFun() {
            return new Function<StreamTest2StreamMetadataRowResult, StreamTest2StreamMetadataRow>() {
                @Override
                public StreamTest2StreamMetadataRow apply(StreamTest2StreamMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTest2StreamMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTest2StreamMetadataRowResult>() {
                @Override
                public StreamTest2StreamMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTest2StreamMetadataRowResult(rowResult);
                }
            };
        }

        public boolean hasMetadata() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("md"));
        }

        public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata getMetadata() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("md"));
            if (bytes == null) {
                return null;
            }
            Metadata value = Metadata.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<StreamTest2StreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadataFun() {
            return new Function<StreamTest2StreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata>() {
                @Override
                public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata apply(StreamTest2StreamMetadataRowResult rowResult) {
                    return rowResult.getMetadata();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Metadata", getMetadata())
                .toString();
        }
    }

    public enum StreamTest2StreamMetadataNamedColumn {
        METADATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("md");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTest2StreamMetadataNamedColumn, byte[]> toShortName() {
            return new Function<StreamTest2StreamMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTest2StreamMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTest2StreamMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTest2StreamMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTest2StreamMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTest2StreamMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTest2StreamMetadataNamedColumnValue<?>>>builder()
                .put("md", Metadata.BYTES_HYDRATOR)
                .build();

    public Map<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadatas(Collection<StreamTest2StreamMetadataRow> rows) {
        Map<Cell, StreamTest2StreamMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTest2StreamMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("md")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putMetadata(StreamTest2StreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value, long duration, TimeUnit unit) {
        put(ImmutableMultimap.of(row, Metadata.of(value)), duration, unit);
    }

    public void putMetadata(Map<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map, long duration, TimeUnit unit) {
        Map<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut), duration, unit);
    }

    public void putMetadataUnlessExists(StreamTest2StreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value, long duration, TimeUnit unit) {
        putUnlessExists(ImmutableMultimap.of(row, Metadata.of(value)), duration, unit);
    }

    public void putMetadataUnlessExists(Map<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map, long duration, TimeUnit unit) {
        Map<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTest2StreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut), duration, unit);
    }

    @Override
    public void put(Multimap<StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataNamedColumnValue<?>> rows, long duration, TimeUnit unit) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows, duration, unit));
        for (StreamTest2StreamMetadataTrigger trigger : triggers) {
            trigger.putStreamTest2StreamMetadata(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataNamedColumnValue<?>> rows, long duration, TimeUnit unit) {
        Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut, duration, unit);
    }

    public void deleteMetadata(StreamTest2StreamMetadataRow row) {
        deleteMetadata(ImmutableSet.of(row));
    }

    public void deleteMetadata(Iterable<StreamTest2StreamMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("md");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(StreamTest2StreamMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTest2StreamMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("md")));
        t.delete(tableName, cells);
    }

    @Override
    public Optional<StreamTest2StreamMetadataRowResult> getRow(StreamTest2StreamMetadataRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<StreamTest2StreamMetadataRowResult> getRow(StreamTest2StreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(StreamTest2StreamMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTest2StreamMetadataRowResult> getRows(Iterable<StreamTest2StreamMetadataRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamMetadataRowResult> getRows(Iterable<StreamTest2StreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<StreamTest2StreamMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTest2StreamMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTest2StreamMetadataRowResult> getAsyncRows(Iterable<StreamTest2StreamMetadataRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<StreamTest2StreamMetadataRowResult> getAsyncRows(final Iterable<StreamTest2StreamMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<StreamTest2StreamMetadataRowResult>> c =
                new Callable<List<StreamTest2StreamMetadataRowResult>>() {
            @Override
            public List<StreamTest2StreamMetadataRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<StreamTest2StreamMetadataNamedColumnValue<?>> getRowColumns(StreamTest2StreamMetadataRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamMetadataNamedColumnValue<?>> getRowColumns(StreamTest2StreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTest2StreamMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTest2StreamMetadataRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTest2StreamMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<StreamTest2StreamMetadataRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<StreamTest2StreamMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>>> c =
                new Callable<Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>>>() {
            @Override
            public Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTest2StreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTest2StreamMetadataRow, StreamTest2StreamMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTest2StreamMetadataRow row = StreamTest2StreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTest2StreamMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTest2StreamMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTest2StreamMetadataRowResult>() {
            @Override
            public StreamTest2StreamMetadataRowResult apply(RowResult<byte[]> input) {
                return StreamTest2StreamMetadataRowResult.of(input);
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

    static String __CLASS_HASH = "zWsk40vONcLwh/6ZqVDaJw==";
}
