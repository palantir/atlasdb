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
public final class LatestSnapshotTable implements
        AtlasDbMutablePersistentTable<LatestSnapshotTable.LatestSnapshotRow,
                                         LatestSnapshotTable.LatestSnapshotNamedColumnValue<?>,
                                         LatestSnapshotTable.LatestSnapshotRowResult>,
        AtlasDbNamedMutableTable<LatestSnapshotTable.LatestSnapshotRow,
                                    LatestSnapshotTable.LatestSnapshotNamedColumnValue<?>,
                                    LatestSnapshotTable.LatestSnapshotRowResult> {
    private final Transaction t;
    private final List<LatestSnapshotTrigger> triggers;
    private final static String rawTableName = "latest_snapshot";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(LatestSnapshotNamedColumn.values());

    static LatestSnapshotTable of(Transaction t, Namespace namespace) {
        return new LatestSnapshotTable(t, namespace, ImmutableList.<LatestSnapshotTrigger>of());
    }

    static LatestSnapshotTable of(Transaction t, Namespace namespace, LatestSnapshotTrigger trigger, LatestSnapshotTrigger... triggers) {
        return new LatestSnapshotTable(t, namespace, ImmutableList.<LatestSnapshotTrigger>builder().add(trigger).add(triggers).build());
    }

    static LatestSnapshotTable of(Transaction t, Namespace namespace, List<LatestSnapshotTrigger> triggers) {
        return new LatestSnapshotTable(t, namespace, triggers);
    }

    private LatestSnapshotTable(Transaction t, Namespace namespace, List<LatestSnapshotTrigger> triggers) {
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
     * LatestSnapshotRow {
     *   {@literal Long key};
     * }
     * </pre>
     */
    public static final class LatestSnapshotRow implements Persistable, Comparable<LatestSnapshotRow> {
        private final long key;

        public static LatestSnapshotRow of(long key) {
            return new LatestSnapshotRow(key);
        }

        private LatestSnapshotRow(long key) {
            this.key = key;
        }

        public long getKey() {
            return key;
        }

        public static Function<LatestSnapshotRow, Long> getKeyFun() {
            return new Function<LatestSnapshotRow, Long>() {
                @Override
                public Long apply(LatestSnapshotRow row) {
                    return row.key;
                }
            };
        }

        public static Function<Long, LatestSnapshotRow> fromKeyFun() {
            return new Function<Long, LatestSnapshotRow>() {
                @Override
                public LatestSnapshotRow apply(Long row) {
                    return LatestSnapshotRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] keyBytes = PtBytes.toBytes(Long.MIN_VALUE ^ key);
            return EncodingUtils.add(keyBytes);
        }

        public static final Hydrator<LatestSnapshotRow> BYTES_HYDRATOR = new Hydrator<LatestSnapshotRow>() {
            @Override
            public LatestSnapshotRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long key = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                return new LatestSnapshotRow(key);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("key", key)
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
            LatestSnapshotRow other = (LatestSnapshotRow) obj;
            return Objects.equals(key, other.key);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        @Override
        public int compareTo(LatestSnapshotRow o) {
            return ComparisonChain.start()
                .compare(this.key, o.key)
                .result();
        }
    }

    public interface LatestSnapshotNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class StreamId implements LatestSnapshotNamedColumnValue<Long> {
        private final Long value;

        public static StreamId of(Long value) {
            return new StreamId(value);
        }

        private StreamId(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "stream_id";
        }

        @Override
        public String getShortColumnName() {
            return "i";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("i");
        }

        public static final Hydrator<StreamId> BYTES_HYDRATOR = new Hydrator<StreamId>() {
            @Override
            public StreamId hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(Long.MIN_VALUE ^ PtBytes.toLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface LatestSnapshotTrigger {
        public void putLatestSnapshot(Multimap<LatestSnapshotRow, ? extends LatestSnapshotNamedColumnValue<?>> newRows);
    }

    public static final class LatestSnapshotRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static LatestSnapshotRowResult of(RowResult<byte[]> row) {
            return new LatestSnapshotRowResult(row);
        }

        private LatestSnapshotRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public LatestSnapshotRow getRowName() {
            return LatestSnapshotRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<LatestSnapshotRowResult, LatestSnapshotRow> getRowNameFun() {
            return new Function<LatestSnapshotRowResult, LatestSnapshotRow>() {
                @Override
                public LatestSnapshotRow apply(LatestSnapshotRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, LatestSnapshotRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, LatestSnapshotRowResult>() {
                @Override
                public LatestSnapshotRowResult apply(RowResult<byte[]> rowResult) {
                    return new LatestSnapshotRowResult(rowResult);
                }
            };
        }

        public boolean hasStreamId() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("i"));
        }

        public Long getStreamId() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("i"));
            if (bytes == null) {
                return null;
            }
            StreamId value = StreamId.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<LatestSnapshotRowResult, Long> getStreamIdFun() {
            return new Function<LatestSnapshotRowResult, Long>() {
                @Override
                public Long apply(LatestSnapshotRowResult rowResult) {
                    return rowResult.getStreamId();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("StreamId", getStreamId())
                .toString();
        }
    }

    public enum LatestSnapshotNamedColumn {
        STREAM_ID {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("i");
            }
        };

        public abstract byte[] getShortName();

        public static Function<LatestSnapshotNamedColumn, byte[]> toShortName() {
            return new Function<LatestSnapshotNamedColumn, byte[]>() {
                @Override
                public byte[] apply(LatestSnapshotNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<LatestSnapshotNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, LatestSnapshotNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(LatestSnapshotNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends LatestSnapshotNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends LatestSnapshotNamedColumnValue<?>>>builder()
                .put("i", StreamId.BYTES_HYDRATOR)
                .build();

    public Map<LatestSnapshotRow, Long> getStreamIds(Collection<LatestSnapshotRow> rows) {
        Map<Cell, LatestSnapshotRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (LatestSnapshotRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("i")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<LatestSnapshotRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = StreamId.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putStreamId(LatestSnapshotRow row, Long value) {
        put(ImmutableMultimap.of(row, StreamId.of(value)));
    }

    public void putStreamId(Map<LatestSnapshotRow, Long> map) {
        Map<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<LatestSnapshotRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), StreamId.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<LatestSnapshotRow, ? extends LatestSnapshotNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (LatestSnapshotTrigger trigger : triggers) {
            trigger.putLatestSnapshot(rows);
        }
    }

    public void deleteStreamId(LatestSnapshotRow row) {
        deleteStreamId(ImmutableSet.of(row));
    }

    public void deleteStreamId(Iterable<LatestSnapshotRow> rows) {
        byte[] col = PtBytes.toCachedBytes("i");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(LatestSnapshotRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<LatestSnapshotRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("i")));
        t.delete(tableRef, cells);
    }

    public Optional<LatestSnapshotRowResult> getRow(LatestSnapshotRow row) {
        return getRow(row, allColumns);
    }

    public Optional<LatestSnapshotRowResult> getRow(LatestSnapshotRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(LatestSnapshotRowResult.of(rowResult));
        }
    }

    @Override
    public List<LatestSnapshotRowResult> getRows(Iterable<LatestSnapshotRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<LatestSnapshotRowResult> getRows(Iterable<LatestSnapshotRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<LatestSnapshotRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(LatestSnapshotRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<LatestSnapshotNamedColumnValue<?>> getRowColumns(LatestSnapshotRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<LatestSnapshotNamedColumnValue<?>> getRowColumns(LatestSnapshotRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<LatestSnapshotNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> getRowsMultimap(Iterable<LatestSnapshotRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> getRowsMultimap(Iterable<LatestSnapshotRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> getRowsMultimapInternal(Iterable<LatestSnapshotRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            LatestSnapshotRow row = LatestSnapshotRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<LatestSnapshotRow, BatchingVisitable<LatestSnapshotNamedColumnValue<?>>> getRowsColumnRange(Iterable<LatestSnapshotRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<LatestSnapshotRow, BatchingVisitable<LatestSnapshotNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            LatestSnapshotRow row = LatestSnapshotRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<LatestSnapshotNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<LatestSnapshotRow, LatestSnapshotNamedColumnValue<?>>> getRowsColumnRange(Iterable<LatestSnapshotRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            LatestSnapshotRow row = LatestSnapshotRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            LatestSnapshotNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<LatestSnapshotRow, Iterator<LatestSnapshotNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<LatestSnapshotRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<LatestSnapshotRow, Iterator<LatestSnapshotNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            LatestSnapshotRow row = LatestSnapshotRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<LatestSnapshotNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<LatestSnapshotRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<LatestSnapshotRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, LatestSnapshotRowResult>() {
            @Override
            public LatestSnapshotRowResult apply(RowResult<byte[]> input) {
                return LatestSnapshotRowResult.of(input);
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
    static String __CLASS_HASH = "uDjDzxHTwUPFE6axOmFPNw==";
}
