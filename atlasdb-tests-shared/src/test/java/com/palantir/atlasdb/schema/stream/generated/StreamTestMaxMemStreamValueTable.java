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
public final class StreamTestMaxMemStreamValueTable implements
        AtlasDbMutablePersistentTable<StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueRow,
                                         StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueNamedColumnValue<?>,
                                         StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueRowResult>,
        AtlasDbNamedMutableTable<StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueRow,
                                    StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueNamedColumnValue<?>,
                                    StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueRowResult> {
    private final Transaction t;
    private final List<StreamTestMaxMemStreamValueTrigger> triggers;
    private final static String rawTableName = "stream_test_max_mem_stream_value";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(StreamTestMaxMemStreamValueNamedColumn.values());

    static StreamTestMaxMemStreamValueTable of(Transaction t, Namespace namespace) {
        return new StreamTestMaxMemStreamValueTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamValueTrigger>of());
    }

    static StreamTestMaxMemStreamValueTable of(Transaction t, Namespace namespace, StreamTestMaxMemStreamValueTrigger trigger, StreamTestMaxMemStreamValueTrigger... triggers) {
        return new StreamTestMaxMemStreamValueTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestMaxMemStreamValueTable of(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamValueTrigger> triggers) {
        return new StreamTestMaxMemStreamValueTable(t, namespace, triggers);
    }

    private StreamTestMaxMemStreamValueTable(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamValueTrigger> triggers) {
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
     * StreamTestMaxMemStreamValueRow {
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class StreamTestMaxMemStreamValueRow implements Persistable, Comparable<StreamTestMaxMemStreamValueRow> {
        private final long id;
        private final long blockId;

        public static StreamTestMaxMemStreamValueRow of(long id, long blockId) {
            return new StreamTestMaxMemStreamValueRow(id, blockId);
        }

        private StreamTestMaxMemStreamValueRow(long id, long blockId) {
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<StreamTestMaxMemStreamValueRow, Long> getIdFun() {
            return new Function<StreamTestMaxMemStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<StreamTestMaxMemStreamValueRow, Long> getBlockIdFun() {
            return new Function<StreamTestMaxMemStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamValueRow row) {
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

        public static final Hydrator<StreamTestMaxMemStreamValueRow> BYTES_HYDRATOR = new Hydrator<StreamTestMaxMemStreamValueRow>() {
            @Override
            public StreamTestMaxMemStreamValueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return new StreamTestMaxMemStreamValueRow(id, blockId);
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
            StreamTestMaxMemStreamValueRow other = (StreamTestMaxMemStreamValueRow) obj;
            return Objects.equals(id, other.id) && Objects.equals(blockId, other.blockId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ id, blockId });
        }

        @Override
        public int compareTo(StreamTestMaxMemStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface StreamTestMaxMemStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements StreamTestMaxMemStreamValueNamedColumnValue<byte[]> {
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

    public interface StreamTestMaxMemStreamValueTrigger {
        public void putStreamTestMaxMemStreamValue(Multimap<StreamTestMaxMemStreamValueRow, ? extends StreamTestMaxMemStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class StreamTestMaxMemStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTestMaxMemStreamValueRowResult of(RowResult<byte[]> row) {
            return new StreamTestMaxMemStreamValueRowResult(row);
        }

        private StreamTestMaxMemStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTestMaxMemStreamValueRow getRowName() {
            return StreamTestMaxMemStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTestMaxMemStreamValueRowResult, StreamTestMaxMemStreamValueRow> getRowNameFun() {
            return new Function<StreamTestMaxMemStreamValueRowResult, StreamTestMaxMemStreamValueRow>() {
                @Override
                public StreamTestMaxMemStreamValueRow apply(StreamTestMaxMemStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTestMaxMemStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTestMaxMemStreamValueRowResult>() {
                @Override
                public StreamTestMaxMemStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTestMaxMemStreamValueRowResult(rowResult);
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

        public static Function<StreamTestMaxMemStreamValueRowResult, byte[]> getValueFun() {
            return new Function<StreamTestMaxMemStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(StreamTestMaxMemStreamValueRowResult rowResult) {
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

    public enum StreamTestMaxMemStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTestMaxMemStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<StreamTestMaxMemStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestMaxMemStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestMaxMemStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTestMaxMemStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTestMaxMemStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTestMaxMemStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTestMaxMemStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<StreamTestMaxMemStreamValueRow, byte[]> getValues(Collection<StreamTestMaxMemStreamValueRow> rows) {
        Map<Cell, StreamTestMaxMemStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTestMaxMemStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<StreamTestMaxMemStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(StreamTestMaxMemStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<StreamTestMaxMemStreamValueRow, byte[]> map) {
        Map<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestMaxMemStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<StreamTestMaxMemStreamValueRow, ? extends StreamTestMaxMemStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (StreamTestMaxMemStreamValueTrigger trigger : triggers) {
            trigger.putStreamTestMaxMemStreamValue(rows);
        }
    }

    public void deleteValue(StreamTestMaxMemStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<StreamTestMaxMemStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(StreamTestMaxMemStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTestMaxMemStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<StreamTestMaxMemStreamValueRowResult> getRow(StreamTestMaxMemStreamValueRow row) {
        return getRow(row, allColumns);
    }

    public Optional<StreamTestMaxMemStreamValueRowResult> getRow(StreamTestMaxMemStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(StreamTestMaxMemStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTestMaxMemStreamValueRowResult> getRows(Iterable<StreamTestMaxMemStreamValueRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<StreamTestMaxMemStreamValueRowResult> getRows(Iterable<StreamTestMaxMemStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<StreamTestMaxMemStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTestMaxMemStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowColumns(StreamTestMaxMemStreamValueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowColumns(StreamTestMaxMemStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestMaxMemStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestMaxMemStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestMaxMemStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTestMaxMemStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestMaxMemStreamValueRow row = StreamTestMaxMemStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestMaxMemStreamValueRow, BatchingVisitable<StreamTestMaxMemStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestMaxMemStreamValueRow, BatchingVisitable<StreamTestMaxMemStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestMaxMemStreamValueRow row = StreamTestMaxMemStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestMaxMemStreamValueNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestMaxMemStreamValueRow, StreamTestMaxMemStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamValueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestMaxMemStreamValueRow row = StreamTestMaxMemStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestMaxMemStreamValueNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<StreamTestMaxMemStreamValueRow, Iterator<StreamTestMaxMemStreamValueNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<StreamTestMaxMemStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestMaxMemStreamValueRow, Iterator<StreamTestMaxMemStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestMaxMemStreamValueRow row = StreamTestMaxMemStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<StreamTestMaxMemStreamValueNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<StreamTestMaxMemStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestMaxMemStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, StreamTestMaxMemStreamValueRowResult>() {
            @Override
            public StreamTestMaxMemStreamValueRowResult apply(RowResult<byte[]> input) {
                return StreamTestMaxMemStreamValueRowResult.of(input);
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
    static String __CLASS_HASH = "OF33YFqOoa6rErfIE6Reew==";
}
