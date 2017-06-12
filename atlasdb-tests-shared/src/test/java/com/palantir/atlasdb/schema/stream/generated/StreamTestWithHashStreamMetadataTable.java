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
public final class StreamTestWithHashStreamMetadataTable implements
        AtlasDbMutablePersistentTable<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow,
                                         StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataNamedColumnValue<?>,
                                         StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRowResult>,
        AtlasDbNamedMutableTable<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow,
                                    StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataNamedColumnValue<?>,
                                    StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRowResult> {
    private final Transaction t;
    private final List<StreamTestWithHashStreamMetadataTrigger> triggers;
    private final static String rawTableName = "stream_test_with_hash_stream_metadata";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(StreamTestWithHashStreamMetadataNamedColumn.values());

    static StreamTestWithHashStreamMetadataTable of(Transaction t, Namespace namespace) {
        return new StreamTestWithHashStreamMetadataTable(t, namespace, ImmutableList.<StreamTestWithHashStreamMetadataTrigger>of());
    }

    static StreamTestWithHashStreamMetadataTable of(Transaction t, Namespace namespace, StreamTestWithHashStreamMetadataTrigger trigger, StreamTestWithHashStreamMetadataTrigger... triggers) {
        return new StreamTestWithHashStreamMetadataTable(t, namespace, ImmutableList.<StreamTestWithHashStreamMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestWithHashStreamMetadataTable of(Transaction t, Namespace namespace, List<StreamTestWithHashStreamMetadataTrigger> triggers) {
        return new StreamTestWithHashStreamMetadataTable(t, namespace, triggers);
    }

    private StreamTestWithHashStreamMetadataTable(Transaction t, Namespace namespace, List<StreamTestWithHashStreamMetadataTrigger> triggers) {
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
     * StreamTestWithHashStreamMetadataRow {
     *   {@literal Long firstComponentHash};
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamMetadataRow implements Persistable, Comparable<StreamTestWithHashStreamMetadataRow> {
        private final long firstComponentHash;
        private final long id;

        public static StreamTestWithHashStreamMetadataRow of(long id) {
            long firstComponentHash = Hashing.murmur3_128().hashBytes(ValueType.VAR_LONG.convertFromJava(id)).asLong();
            return new StreamTestWithHashStreamMetadataRow(firstComponentHash, id);
        }

        private StreamTestWithHashStreamMetadataRow(long firstComponentHash, long id) {
            this.firstComponentHash = firstComponentHash;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTestWithHashStreamMetadataRow, Long> getIdFun() {
            return new Function<StreamTestWithHashStreamMetadataRow, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamMetadataRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTestWithHashStreamMetadataRow> fromIdFun() {
            return new Function<Long, StreamTestWithHashStreamMetadataRow>() {
                @Override
                public StreamTestWithHashStreamMetadataRow apply(Long row) {
                    return StreamTestWithHashStreamMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] firstComponentHashBytes = PtBytes.toBytes(Long.MIN_VALUE ^ firstComponentHash);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(firstComponentHashBytes, idBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamMetadataRow> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamMetadataRow>() {
            @Override
            public StreamTestWithHashStreamMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long firstComponentHash = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTestWithHashStreamMetadataRow(firstComponentHash, id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("firstComponentHash", firstComponentHash)
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
            StreamTestWithHashStreamMetadataRow other = (StreamTestWithHashStreamMetadataRow) obj;
            return Objects.equal(firstComponentHash, other.firstComponentHash) && Objects.equal(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ firstComponentHash, id });
        }

        @Override
        public int compareTo(StreamTestWithHashStreamMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.firstComponentHash, o.firstComponentHash)
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface StreamTestWithHashStreamMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

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
    public static final class Metadata implements StreamTestWithHashStreamMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> {
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

    public interface StreamTestWithHashStreamMetadataTrigger {
        public void putStreamTestWithHashStreamMetadata(Multimap<StreamTestWithHashStreamMetadataRow, ? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>> newRows);
    }

    public static final class StreamTestWithHashStreamMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTestWithHashStreamMetadataRowResult of(RowResult<byte[]> row) {
            return new StreamTestWithHashStreamMetadataRowResult(row);
        }

        private StreamTestWithHashStreamMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTestWithHashStreamMetadataRow getRowName() {
            return StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTestWithHashStreamMetadataRowResult, StreamTestWithHashStreamMetadataRow> getRowNameFun() {
            return new Function<StreamTestWithHashStreamMetadataRowResult, StreamTestWithHashStreamMetadataRow>() {
                @Override
                public StreamTestWithHashStreamMetadataRow apply(StreamTestWithHashStreamMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTestWithHashStreamMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTestWithHashStreamMetadataRowResult>() {
                @Override
                public StreamTestWithHashStreamMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTestWithHashStreamMetadataRowResult(rowResult);
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

        public static Function<StreamTestWithHashStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadataFun() {
            return new Function<StreamTestWithHashStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata>() {
                @Override
                public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata apply(StreamTestWithHashStreamMetadataRowResult rowResult) {
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

    public enum StreamTestWithHashStreamMetadataNamedColumn {
        METADATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("md");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTestWithHashStreamMetadataNamedColumn, byte[]> toShortName() {
            return new Function<StreamTestWithHashStreamMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestWithHashStreamMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestWithHashStreamMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTestWithHashStreamMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTestWithHashStreamMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>>>builder()
                .put("md", Metadata.BYTES_HYDRATOR)
                .build();

    public Map<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadatas(Collection<StreamTestWithHashStreamMetadataRow> rows) {
        Map<Cell, StreamTestWithHashStreamMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTestWithHashStreamMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("md")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putMetadata(StreamTestWithHashStreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
        put(ImmutableMultimap.of(row, Metadata.of(value)));
    }

    public void putMetadata(Map<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map) {
        Map<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putMetadataUnlessExists(StreamTestWithHashStreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
        putUnlessExists(ImmutableMultimap.of(row, Metadata.of(value)));
    }

    public void putMetadataUnlessExists(Map<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map) {
        Map<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestWithHashStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<StreamTestWithHashStreamMetadataRow, ? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (StreamTestWithHashStreamMetadataTrigger trigger : triggers) {
            trigger.putStreamTestWithHashStreamMetadata(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<StreamTestWithHashStreamMetadataRow, ? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>> rows) {
        Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<StreamTestWithHashStreamMetadataRow, ? extends StreamTestWithHashStreamMetadataNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteMetadata(StreamTestWithHashStreamMetadataRow row) {
        deleteMetadata(ImmutableSet.of(row));
    }

    public void deleteMetadata(Iterable<StreamTestWithHashStreamMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("md");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(StreamTestWithHashStreamMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTestWithHashStreamMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("md")));
        t.delete(tableRef, cells);
    }

    public Optional<StreamTestWithHashStreamMetadataRowResult> getRow(StreamTestWithHashStreamMetadataRow row) {
        return getRow(row, allColumns);
    }

    public Optional<StreamTestWithHashStreamMetadataRowResult> getRow(StreamTestWithHashStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(StreamTestWithHashStreamMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTestWithHashStreamMetadataRowResult> getRows(Iterable<StreamTestWithHashStreamMetadataRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamMetadataRowResult> getRows(Iterable<StreamTestWithHashStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<StreamTestWithHashStreamMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTestWithHashStreamMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowColumns(StreamTestWithHashStreamMetadataRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowColumns(StreamTestWithHashStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestWithHashStreamMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestWithHashStreamMetadataRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestWithHashStreamMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTestWithHashStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestWithHashStreamMetadataRow, BatchingVisitable<StreamTestWithHashStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestWithHashStreamMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestWithHashStreamMetadataRow, BatchingVisitable<StreamTestWithHashStreamMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestWithHashStreamMetadataNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestWithHashStreamMetadataRow, StreamTestWithHashStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestWithHashStreamMetadataRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestWithHashStreamMetadataNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<StreamTestWithHashStreamMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestWithHashStreamMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestWithHashStreamMetadataRowResult>() {
            @Override
            public StreamTestWithHashStreamMetadataRowResult apply(RowResult<byte[]> input) {
                return StreamTestWithHashStreamMetadataRowResult.of(input);
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
    static String __CLASS_HASH = "Vn50INJJjQ96rUiLUid4wA==";
}
