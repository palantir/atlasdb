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
public final class StreamTestWithHashStreamValueTable implements
        AtlasDbMutablePersistentTable<StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow,
                                         StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueNamedColumnValue<?>,
                                         StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRowResult>,
        AtlasDbNamedMutableTable<StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow,
                                    StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueNamedColumnValue<?>,
                                    StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRowResult> {
    private final Transaction t;
    private final List<StreamTestWithHashStreamValueTrigger> triggers;
    private final static String rawTableName = "stream_test_with_hash_stream_value";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(StreamTestWithHashStreamValueNamedColumn.values());

    static StreamTestWithHashStreamValueTable of(Transaction t, Namespace namespace) {
        return new StreamTestWithHashStreamValueTable(t, namespace, ImmutableList.<StreamTestWithHashStreamValueTrigger>of());
    }

    static StreamTestWithHashStreamValueTable of(Transaction t, Namespace namespace, StreamTestWithHashStreamValueTrigger trigger, StreamTestWithHashStreamValueTrigger... triggers) {
        return new StreamTestWithHashStreamValueTable(t, namespace, ImmutableList.<StreamTestWithHashStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestWithHashStreamValueTable of(Transaction t, Namespace namespace, List<StreamTestWithHashStreamValueTrigger> triggers) {
        return new StreamTestWithHashStreamValueTable(t, namespace, triggers);
    }

    private StreamTestWithHashStreamValueTable(Transaction t, Namespace namespace, List<StreamTestWithHashStreamValueTrigger> triggers) {
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
     * StreamTestWithHashStreamValueRow {
     *   {@literal Long firstComponentHash};
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamValueRow implements Persistable, Comparable<StreamTestWithHashStreamValueRow> {
        private final long firstComponentHash;
        private final long id;
        private final long blockId;

        public static StreamTestWithHashStreamValueRow of(long id, long blockId) {
            long firstComponentHash = Hashing.murmur3_128().hashBytes(ValueType.VAR_LONG.convertFromJava(id)).asLong();
            return new StreamTestWithHashStreamValueRow(firstComponentHash, id, blockId);
        }

        private StreamTestWithHashStreamValueRow(long firstComponentHash, long id, long blockId) {
            this.firstComponentHash = firstComponentHash;
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<StreamTestWithHashStreamValueRow, Long> getIdFun() {
            return new Function<StreamTestWithHashStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<StreamTestWithHashStreamValueRow, Long> getBlockIdFun() {
            return new Function<StreamTestWithHashStreamValueRow, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamValueRow row) {
                    return row.blockId;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] firstComponentHashBytes = PtBytes.toBytes(Long.MIN_VALUE ^ firstComponentHash);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
            return EncodingUtils.add(firstComponentHashBytes, idBytes, blockIdBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamValueRow> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamValueRow>() {
            @Override
            public StreamTestWithHashStreamValueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long firstComponentHash = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return new StreamTestWithHashStreamValueRow(firstComponentHash, id, blockId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("firstComponentHash", firstComponentHash)
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
            StreamTestWithHashStreamValueRow other = (StreamTestWithHashStreamValueRow) obj;
            return Objects.equal(firstComponentHash, other.firstComponentHash) && Objects.equal(id, other.id) && Objects.equal(blockId, other.blockId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ firstComponentHash, id, blockId });
        }

        @Override
        public int compareTo(StreamTestWithHashStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.firstComponentHash, o.firstComponentHash)
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface StreamTestWithHashStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements StreamTestWithHashStreamValueNamedColumnValue<byte[]> {
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

    public interface StreamTestWithHashStreamValueTrigger {
        public void putStreamTestWithHashStreamValue(Multimap<StreamTestWithHashStreamValueRow, ? extends StreamTestWithHashStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class StreamTestWithHashStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static StreamTestWithHashStreamValueRowResult of(RowResult<byte[]> row) {
            return new StreamTestWithHashStreamValueRowResult(row);
        }

        private StreamTestWithHashStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public StreamTestWithHashStreamValueRow getRowName() {
            return StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<StreamTestWithHashStreamValueRowResult, StreamTestWithHashStreamValueRow> getRowNameFun() {
            return new Function<StreamTestWithHashStreamValueRowResult, StreamTestWithHashStreamValueRow>() {
                @Override
                public StreamTestWithHashStreamValueRow apply(StreamTestWithHashStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, StreamTestWithHashStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, StreamTestWithHashStreamValueRowResult>() {
                @Override
                public StreamTestWithHashStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new StreamTestWithHashStreamValueRowResult(rowResult);
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

        public static Function<StreamTestWithHashStreamValueRowResult, byte[]> getValueFun() {
            return new Function<StreamTestWithHashStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(StreamTestWithHashStreamValueRowResult rowResult) {
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

    public enum StreamTestWithHashStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<StreamTestWithHashStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<StreamTestWithHashStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestWithHashStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestWithHashStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, StreamTestWithHashStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(StreamTestWithHashStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends StreamTestWithHashStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends StreamTestWithHashStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<StreamTestWithHashStreamValueRow, byte[]> getValues(Collection<StreamTestWithHashStreamValueRow> rows) {
        Map<Cell, StreamTestWithHashStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (StreamTestWithHashStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<StreamTestWithHashStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(StreamTestWithHashStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<StreamTestWithHashStreamValueRow, byte[]> map) {
        Map<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestWithHashStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putValueUnlessExists(StreamTestWithHashStreamValueRow row, byte[] value) {
        putUnlessExists(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValueUnlessExists(Map<StreamTestWithHashStreamValueRow, byte[]> map) {
        Map<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<StreamTestWithHashStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<StreamTestWithHashStreamValueRow, ? extends StreamTestWithHashStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (StreamTestWithHashStreamValueTrigger trigger : triggers) {
            trigger.putStreamTestWithHashStreamValue(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<StreamTestWithHashStreamValueRow, ? extends StreamTestWithHashStreamValueNamedColumnValue<?>> rows) {
        Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<StreamTestWithHashStreamValueRow, ? extends StreamTestWithHashStreamValueNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteValue(StreamTestWithHashStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<StreamTestWithHashStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(StreamTestWithHashStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<StreamTestWithHashStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<StreamTestWithHashStreamValueRowResult> getRow(StreamTestWithHashStreamValueRow row) {
        return getRow(row, allColumns);
    }

    public Optional<StreamTestWithHashStreamValueRowResult> getRow(StreamTestWithHashStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(StreamTestWithHashStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<StreamTestWithHashStreamValueRowResult> getRows(Iterable<StreamTestWithHashStreamValueRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamValueRowResult> getRows(Iterable<StreamTestWithHashStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<StreamTestWithHashStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(StreamTestWithHashStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<StreamTestWithHashStreamValueNamedColumnValue<?>> getRowColumns(StreamTestWithHashStreamValueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamValueNamedColumnValue<?>> getRowColumns(StreamTestWithHashStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestWithHashStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestWithHashStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<StreamTestWithHashStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<StreamTestWithHashStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestWithHashStreamValueRow, BatchingVisitable<StreamTestWithHashStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestWithHashStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestWithHashStreamValueRow, BatchingVisitable<StreamTestWithHashStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestWithHashStreamValueNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestWithHashStreamValueRow, StreamTestWithHashStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<StreamTestWithHashStreamValueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestWithHashStreamValueNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<StreamTestWithHashStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestWithHashStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestWithHashStreamValueRowResult>() {
            @Override
            public StreamTestWithHashStreamValueRowResult apply(RowResult<byte[]> input) {
                return StreamTestWithHashStreamValueRowResult.of(input);
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
    static String __CLASS_HASH = "izNJk0+ziA9F3kbfaV1S+g==";
}
