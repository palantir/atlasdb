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

import javax.annotation.Nullable;
import javax.annotation.processing.Generated;

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
public final class SnapshotsStreamMetadataTable implements
        AtlasDbMutablePersistentTable<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow,
                                         SnapshotsStreamMetadataTable.SnapshotsStreamMetadataNamedColumnValue<?>,
                                         SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRowResult>,
        AtlasDbNamedMutableTable<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow,
                                    SnapshotsStreamMetadataTable.SnapshotsStreamMetadataNamedColumnValue<?>,
                                    SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRowResult> {
    private final Transaction t;
    private final List<SnapshotsStreamMetadataTrigger> triggers;
    private final static String rawTableName = "snapshots_stream_metadata";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SnapshotsStreamMetadataNamedColumn.values());

    static SnapshotsStreamMetadataTable of(Transaction t, Namespace namespace) {
        return new SnapshotsStreamMetadataTable(t, namespace, ImmutableList.<SnapshotsStreamMetadataTrigger>of());
    }

    static SnapshotsStreamMetadataTable of(Transaction t, Namespace namespace, SnapshotsStreamMetadataTrigger trigger, SnapshotsStreamMetadataTrigger... triggers) {
        return new SnapshotsStreamMetadataTable(t, namespace, ImmutableList.<SnapshotsStreamMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static SnapshotsStreamMetadataTable of(Transaction t, Namespace namespace, List<SnapshotsStreamMetadataTrigger> triggers) {
        return new SnapshotsStreamMetadataTable(t, namespace, triggers);
    }

    private SnapshotsStreamMetadataTable(Transaction t, Namespace namespace, List<SnapshotsStreamMetadataTrigger> triggers) {
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
     * SnapshotsStreamMetadataRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class SnapshotsStreamMetadataRow implements Persistable, Comparable<SnapshotsStreamMetadataRow> {
        private final long id;

        public static SnapshotsStreamMetadataRow of(long id) {
            return new SnapshotsStreamMetadataRow(id);
        }

        private SnapshotsStreamMetadataRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<SnapshotsStreamMetadataRow, Long> getIdFun() {
            return new Function<SnapshotsStreamMetadataRow, Long>() {
                @Override
                public Long apply(SnapshotsStreamMetadataRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, SnapshotsStreamMetadataRow> fromIdFun() {
            return new Function<Long, SnapshotsStreamMetadataRow>() {
                @Override
                public SnapshotsStreamMetadataRow apply(Long row) {
                    return SnapshotsStreamMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<SnapshotsStreamMetadataRow> BYTES_HYDRATOR = new Hydrator<SnapshotsStreamMetadataRow>() {
            @Override
            public SnapshotsStreamMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                return new SnapshotsStreamMetadataRow(id);
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
            SnapshotsStreamMetadataRow other = (SnapshotsStreamMetadataRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(SnapshotsStreamMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface SnapshotsStreamMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

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
     *   
     * }
     * </pre>
     */
    public static final class Metadata implements SnapshotsStreamMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> {
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

    public interface SnapshotsStreamMetadataTrigger {
        public void putSnapshotsStreamMetadata(Multimap<SnapshotsStreamMetadataRow, ? extends SnapshotsStreamMetadataNamedColumnValue<?>> newRows);
    }

    public static final class SnapshotsStreamMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SnapshotsStreamMetadataRowResult of(RowResult<byte[]> row) {
            return new SnapshotsStreamMetadataRowResult(row);
        }

        private SnapshotsStreamMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SnapshotsStreamMetadataRow getRowName() {
            return SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SnapshotsStreamMetadataRowResult, SnapshotsStreamMetadataRow> getRowNameFun() {
            return new Function<SnapshotsStreamMetadataRowResult, SnapshotsStreamMetadataRow>() {
                @Override
                public SnapshotsStreamMetadataRow apply(SnapshotsStreamMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SnapshotsStreamMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SnapshotsStreamMetadataRowResult>() {
                @Override
                public SnapshotsStreamMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new SnapshotsStreamMetadataRowResult(rowResult);
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

        public static Function<SnapshotsStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadataFun() {
            return new Function<SnapshotsStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata>() {
                @Override
                public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata apply(SnapshotsStreamMetadataRowResult rowResult) {
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

    public enum SnapshotsStreamMetadataNamedColumn {
        METADATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("md");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SnapshotsStreamMetadataNamedColumn, byte[]> toShortName() {
            return new Function<SnapshotsStreamMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SnapshotsStreamMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SnapshotsStreamMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SnapshotsStreamMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SnapshotsStreamMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SnapshotsStreamMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SnapshotsStreamMetadataNamedColumnValue<?>>>builder()
                .put("md", Metadata.BYTES_HYDRATOR)
                .build();

    public Map<SnapshotsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadatas(Collection<SnapshotsStreamMetadataRow> rows) {
        Map<Cell, SnapshotsStreamMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SnapshotsStreamMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("md")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SnapshotsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putMetadata(SnapshotsStreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
        put(ImmutableMultimap.of(row, Metadata.of(value)));
    }

    public void putMetadata(Map<SnapshotsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map) {
        Map<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SnapshotsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SnapshotsStreamMetadataRow, ? extends SnapshotsStreamMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SnapshotsStreamMetadataTrigger trigger : triggers) {
            trigger.putSnapshotsStreamMetadata(rows);
        }
    }

    public void deleteMetadata(SnapshotsStreamMetadataRow row) {
        deleteMetadata(ImmutableSet.of(row));
    }

    public void deleteMetadata(Iterable<SnapshotsStreamMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("md");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SnapshotsStreamMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SnapshotsStreamMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("md")));
        t.delete(tableRef, cells);
    }

    public Optional<SnapshotsStreamMetadataRowResult> getRow(SnapshotsStreamMetadataRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SnapshotsStreamMetadataRowResult> getRow(SnapshotsStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SnapshotsStreamMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<SnapshotsStreamMetadataRowResult> getRows(Iterable<SnapshotsStreamMetadataRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SnapshotsStreamMetadataRowResult> getRows(Iterable<SnapshotsStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SnapshotsStreamMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SnapshotsStreamMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SnapshotsStreamMetadataNamedColumnValue<?>> getRowColumns(SnapshotsStreamMetadataRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SnapshotsStreamMetadataNamedColumnValue<?>> getRowColumns(SnapshotsStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SnapshotsStreamMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<SnapshotsStreamMetadataRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<SnapshotsStreamMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SnapshotsStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SnapshotsStreamMetadataRow row = SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SnapshotsStreamMetadataRow, BatchingVisitable<SnapshotsStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<SnapshotsStreamMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamMetadataRow, BatchingVisitable<SnapshotsStreamMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamMetadataRow row = SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SnapshotsStreamMetadataNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SnapshotsStreamMetadataRow, SnapshotsStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<SnapshotsStreamMetadataRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SnapshotsStreamMetadataRow row = SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SnapshotsStreamMetadataNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SnapshotsStreamMetadataRow, Iterator<SnapshotsStreamMetadataNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SnapshotsStreamMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SnapshotsStreamMetadataRow, Iterator<SnapshotsStreamMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SnapshotsStreamMetadataRow row = SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SnapshotsStreamMetadataNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
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

    public BatchingVisitableView<SnapshotsStreamMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SnapshotsStreamMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SnapshotsStreamMetadataRowResult>() {
            @Override
            public SnapshotsStreamMetadataRowResult apply(RowResult<byte[]> input) {
                return SnapshotsStreamMetadataRowResult.of(input);
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
     * {@link Nullable}
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
    static String __CLASS_HASH = "X1A0s/Ak7gFq6exUuTBR2g==";
}
