package com.palantir.atlasdb.blob.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import com.google.common.base.Optional;
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
public final class HotspottyDataStreamValueTable implements
        AtlasDbMutablePersistentTable<HotspottyDataStreamValueTable.HotspottyDataStreamValueRow,
                                         HotspottyDataStreamValueTable.HotspottyDataStreamValueNamedColumnValue<?>,
                                         HotspottyDataStreamValueTable.HotspottyDataStreamValueRowResult>,
        AtlasDbNamedMutableTable<HotspottyDataStreamValueTable.HotspottyDataStreamValueRow,
                                    HotspottyDataStreamValueTable.HotspottyDataStreamValueNamedColumnValue<?>,
                                    HotspottyDataStreamValueTable.HotspottyDataStreamValueRowResult> {
    private final Transaction t;
    private final List<HotspottyDataStreamValueTrigger> triggers;
    private final static String rawTableName = "hotspottyData_stream_value";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(HotspottyDataStreamValueNamedColumn.values());

    static HotspottyDataStreamValueTable of(Transaction t, Namespace namespace) {
        return new HotspottyDataStreamValueTable(t, namespace, ImmutableList.<HotspottyDataStreamValueTrigger>of());
    }

    static HotspottyDataStreamValueTable of(Transaction t, Namespace namespace, HotspottyDataStreamValueTrigger trigger, HotspottyDataStreamValueTrigger... triggers) {
        return new HotspottyDataStreamValueTable(t, namespace, ImmutableList.<HotspottyDataStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static HotspottyDataStreamValueTable of(Transaction t, Namespace namespace, List<HotspottyDataStreamValueTrigger> triggers) {
        return new HotspottyDataStreamValueTable(t, namespace, triggers);
    }

    private HotspottyDataStreamValueTable(Transaction t, Namespace namespace, List<HotspottyDataStreamValueTrigger> triggers) {
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
     * HotspottyDataStreamValueRow {
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class HotspottyDataStreamValueRow implements Persistable, Comparable<HotspottyDataStreamValueRow> {
        private final long id;
        private final long blockId;

        public static HotspottyDataStreamValueRow of(long id, long blockId) {
            return new HotspottyDataStreamValueRow(id, blockId);
        }

        private HotspottyDataStreamValueRow(long id, long blockId) {
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<HotspottyDataStreamValueRow, Long> getIdFun() {
            return new Function<HotspottyDataStreamValueRow, Long>() {
                @Override
                public Long apply(HotspottyDataStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<HotspottyDataStreamValueRow, Long> getBlockIdFun() {
            return new Function<HotspottyDataStreamValueRow, Long>() {
                @Override
                public Long apply(HotspottyDataStreamValueRow row) {
                    return row.blockId;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeSignedVarLong(id);
            byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
            return EncodingUtils.add(idBytes, blockIdBytes);
        }

        public static final Hydrator<HotspottyDataStreamValueRow> BYTES_HYDRATOR = new Hydrator<HotspottyDataStreamValueRow>() {
            @Override
            public HotspottyDataStreamValueRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long id = EncodingUtils.decodeSignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfSignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return new HotspottyDataStreamValueRow(id, blockId);
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
            HotspottyDataStreamValueRow other = (HotspottyDataStreamValueRow) obj;
            return Objects.equals(id, other.id) && Objects.equals(blockId, other.blockId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ id, blockId });
        }

        @Override
        public int compareTo(HotspottyDataStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface HotspottyDataStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements HotspottyDataStreamValueNamedColumnValue<byte[]> {
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

    public interface HotspottyDataStreamValueTrigger {
        public void putHotspottyDataStreamValue(Multimap<HotspottyDataStreamValueRow, ? extends HotspottyDataStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class HotspottyDataStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static HotspottyDataStreamValueRowResult of(RowResult<byte[]> row) {
            return new HotspottyDataStreamValueRowResult(row);
        }

        private HotspottyDataStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public HotspottyDataStreamValueRow getRowName() {
            return HotspottyDataStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<HotspottyDataStreamValueRowResult, HotspottyDataStreamValueRow> getRowNameFun() {
            return new Function<HotspottyDataStreamValueRowResult, HotspottyDataStreamValueRow>() {
                @Override
                public HotspottyDataStreamValueRow apply(HotspottyDataStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, HotspottyDataStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, HotspottyDataStreamValueRowResult>() {
                @Override
                public HotspottyDataStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new HotspottyDataStreamValueRowResult(rowResult);
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

        public static Function<HotspottyDataStreamValueRowResult, byte[]> getValueFun() {
            return new Function<HotspottyDataStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(HotspottyDataStreamValueRowResult rowResult) {
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

    public enum HotspottyDataStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<HotspottyDataStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<HotspottyDataStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(HotspottyDataStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<HotspottyDataStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, HotspottyDataStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(HotspottyDataStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends HotspottyDataStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends HotspottyDataStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<HotspottyDataStreamValueRow, byte[]> getValues(Collection<HotspottyDataStreamValueRow> rows) {
        Map<Cell, HotspottyDataStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (HotspottyDataStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<HotspottyDataStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(HotspottyDataStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<HotspottyDataStreamValueRow, byte[]> map) {
        Map<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<HotspottyDataStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<HotspottyDataStreamValueRow, ? extends HotspottyDataStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (HotspottyDataStreamValueTrigger trigger : triggers) {
            trigger.putHotspottyDataStreamValue(rows);
        }
    }

    public void deleteValue(HotspottyDataStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<HotspottyDataStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(HotspottyDataStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<HotspottyDataStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<HotspottyDataStreamValueRowResult> getRow(HotspottyDataStreamValueRow row) {
        return getRow(row, allColumns);
    }

    public Optional<HotspottyDataStreamValueRowResult> getRow(HotspottyDataStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(HotspottyDataStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<HotspottyDataStreamValueRowResult> getRows(Iterable<HotspottyDataStreamValueRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<HotspottyDataStreamValueRowResult> getRows(Iterable<HotspottyDataStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<HotspottyDataStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(HotspottyDataStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<HotspottyDataStreamValueNamedColumnValue<?>> getRowColumns(HotspottyDataStreamValueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<HotspottyDataStreamValueNamedColumnValue<?>> getRowColumns(HotspottyDataStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<HotspottyDataStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<HotspottyDataStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<HotspottyDataStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<HotspottyDataStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            HotspottyDataStreamValueRow row = HotspottyDataStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<HotspottyDataStreamValueRow, BatchingVisitable<HotspottyDataStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<HotspottyDataStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<HotspottyDataStreamValueRow, BatchingVisitable<HotspottyDataStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            HotspottyDataStreamValueRow row = HotspottyDataStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<HotspottyDataStreamValueNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<HotspottyDataStreamValueRow, HotspottyDataStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<HotspottyDataStreamValueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            HotspottyDataStreamValueRow row = HotspottyDataStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            HotspottyDataStreamValueNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<HotspottyDataStreamValueRow, Iterator<HotspottyDataStreamValueNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<HotspottyDataStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<HotspottyDataStreamValueRow, Iterator<HotspottyDataStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            HotspottyDataStreamValueRow row = HotspottyDataStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<HotspottyDataStreamValueNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<HotspottyDataStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<HotspottyDataStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, HotspottyDataStreamValueRowResult>() {
            @Override
            public HotspottyDataStreamValueRowResult apply(RowResult<byte[]> input) {
                return HotspottyDataStreamValueRowResult.of(input);
            }
        });
    }

    @Override
    public List<String> findConstraintFailures(Map<Cell, byte[]> _writes,
                                               ConstraintCheckingTransaction _transaction,
                                               AtlasDbConstraintCheckingMode _constraintCheckingMode) {
        return ImmutableList.of();
    }

    @Override
    public List<String> findConstraintFailuresNoRead(Map<Cell, byte[]> _writes,
                                                     AtlasDbConstraintCheckingMode _constraintCheckingMode) {
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
    static String __CLASS_HASH = "WgGV3tTBVKug1Y6FQllJmw==";
}
