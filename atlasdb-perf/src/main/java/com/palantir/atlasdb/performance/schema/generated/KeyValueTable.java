package com.palantir.atlasdb.performance.schema.generated;

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
public final class KeyValueTable implements
        AtlasDbMutablePersistentTable<KeyValueTable.KeyValueRow,
                                         KeyValueTable.KeyValueNamedColumnValue<?>,
                                         KeyValueTable.KeyValueRowResult>,
        AtlasDbNamedMutableTable<KeyValueTable.KeyValueRow,
                                    KeyValueTable.KeyValueNamedColumnValue<?>,
                                    KeyValueTable.KeyValueRowResult> {
    private final Transaction t;
    private final List<KeyValueTrigger> triggers;
    private final static String rawTableName = "blobs";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(KeyValueNamedColumn.values());

    static KeyValueTable of(Transaction t, Namespace namespace) {
        return new KeyValueTable(t, namespace, ImmutableList.<KeyValueTrigger>of());
    }

    static KeyValueTable of(Transaction t, Namespace namespace, KeyValueTrigger trigger, KeyValueTrigger... triggers) {
        return new KeyValueTable(t, namespace, ImmutableList.<KeyValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static KeyValueTable of(Transaction t, Namespace namespace, List<KeyValueTrigger> triggers) {
        return new KeyValueTable(t, namespace, triggers);
    }

    private KeyValueTable(Transaction t, Namespace namespace, List<KeyValueTrigger> triggers) {
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
     * KeyValueRow {
     *   {@literal String key};
     * }
     * </pre>
     */
    public static final class KeyValueRow implements Persistable, Comparable<KeyValueRow> {
        private final String key;

        public static KeyValueRow of(String key) {
            return new KeyValueRow(key);
        }

        private KeyValueRow(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public static Function<KeyValueRow, String> getKeyFun() {
            return new Function<KeyValueRow, String>() {
                @Override
                public String apply(KeyValueRow row) {
                    return row.key;
                }
            };
        }

        public static Function<String, KeyValueRow> fromKeyFun() {
            return new Function<String, KeyValueRow>() {
                @Override
                public KeyValueRow apply(String row) {
                    return KeyValueRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] keyBytes = PtBytes.toBytes(key);
            return EncodingUtils.add(keyBytes);
        }

        public static final Hydrator<KeyValueRow> BYTES_HYDRATOR = new Hydrator<KeyValueRow>() {
            @Override
            public KeyValueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String key = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new KeyValueRow(key);
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
            KeyValueRow other = (KeyValueRow) obj;
            return Objects.equals(key, other.key);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        @Override
        public int compareTo(KeyValueRow o) {
            return ComparisonChain.start()
                .compare(this.key, o.key)
                .result();
        }
    }

    public interface KeyValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class StreamId implements KeyValueNamedColumnValue<Long> {
        private final Long value;

        public static StreamId of(Long value) {
            return new StreamId(value);
        }

        private StreamId(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "streamId";
        }

        @Override
        public String getShortColumnName() {
            return "s";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("s");
        }

        public static final Hydrator<StreamId> BYTES_HYDRATOR = new Hydrator<StreamId>() {
            @Override
            public StreamId hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeUnsignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface KeyValueTrigger {
        public void putKeyValue(Multimap<KeyValueRow, ? extends KeyValueNamedColumnValue<?>> newRows);
    }

    public static final class KeyValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static KeyValueRowResult of(RowResult<byte[]> row) {
            return new KeyValueRowResult(row);
        }

        private KeyValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public KeyValueRow getRowName() {
            return KeyValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<KeyValueRowResult, KeyValueRow> getRowNameFun() {
            return new Function<KeyValueRowResult, KeyValueRow>() {
                @Override
                public KeyValueRow apply(KeyValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, KeyValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, KeyValueRowResult>() {
                @Override
                public KeyValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new KeyValueRowResult(rowResult);
                }
            };
        }

        public boolean hasStreamId() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("s"));
        }

        public Long getStreamId() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("s"));
            if (bytes == null) {
                return null;
            }
            StreamId value = StreamId.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<KeyValueRowResult, Long> getStreamIdFun() {
            return new Function<KeyValueRowResult, Long>() {
                @Override
                public Long apply(KeyValueRowResult rowResult) {
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

    public enum KeyValueNamedColumn {
        STREAM_ID {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("s");
            }
        };

        public abstract byte[] getShortName();

        public static Function<KeyValueNamedColumn, byte[]> toShortName() {
            return new Function<KeyValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(KeyValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<KeyValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, KeyValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(KeyValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends KeyValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends KeyValueNamedColumnValue<?>>>builder()
                .put("s", StreamId.BYTES_HYDRATOR)
                .build();

    public Map<KeyValueRow, Long> getStreamIds(Collection<KeyValueRow> rows) {
        Map<Cell, KeyValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (KeyValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("s")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<KeyValueRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = StreamId.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putStreamId(KeyValueRow row, Long value) {
        put(ImmutableMultimap.of(row, StreamId.of(value)));
    }

    public void putStreamId(Map<KeyValueRow, Long> map) {
        Map<KeyValueRow, KeyValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<KeyValueRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), StreamId.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<KeyValueRow, ? extends KeyValueNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (KeyValueTrigger trigger : triggers) {
            trigger.putKeyValue(rows);
        }
    }

    public void deleteStreamId(KeyValueRow row) {
        deleteStreamId(ImmutableSet.of(row));
    }

    public void deleteStreamId(Iterable<KeyValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("s");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(KeyValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<KeyValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("s")));
        t.delete(tableRef, cells);
    }

    public Optional<KeyValueRowResult> getRow(KeyValueRow row) {
        return getRow(row, allColumns);
    }

    public Optional<KeyValueRowResult> getRow(KeyValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(KeyValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<KeyValueRowResult> getRows(Iterable<KeyValueRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<KeyValueRowResult> getRows(Iterable<KeyValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<KeyValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(KeyValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<KeyValueNamedColumnValue<?>> getRowColumns(KeyValueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<KeyValueNamedColumnValue<?>> getRowColumns(KeyValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<KeyValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<KeyValueRow, KeyValueNamedColumnValue<?>> getRowsMultimap(Iterable<KeyValueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<KeyValueRow, KeyValueNamedColumnValue<?>> getRowsMultimap(Iterable<KeyValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<KeyValueRow, KeyValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<KeyValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<KeyValueRow, KeyValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<KeyValueRow, KeyValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            KeyValueRow row = KeyValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<KeyValueRow, BatchingVisitable<KeyValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<KeyValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KeyValueRow, BatchingVisitable<KeyValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KeyValueRow row = KeyValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<KeyValueNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<KeyValueRow, KeyValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<KeyValueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            KeyValueRow row = KeyValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            KeyValueNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<KeyValueRow, Iterator<KeyValueNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<KeyValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<KeyValueRow, Iterator<KeyValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            KeyValueRow row = KeyValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<KeyValueNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private RangeRequest optimizeRangeRequest(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            return range.getBuilder().retainColumns(allColumns).build();
        }
        return range;
    }

    private Iterable<RangeRequest> optimizeRangeRequests(Iterable<RangeRequest> ranges) {
        return Iterables.transform(ranges, this::optimizeRangeRequest);
    }

    public BatchingVisitableView<KeyValueRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, KeyValueRowResult>() {
            @Override
            public KeyValueRowResult apply(RowResult<byte[]> input) {
                return KeyValueRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<KeyValueRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<KeyValueRowResult>>() {
            @Override
            public BatchingVisitable<KeyValueRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, KeyValueRowResult>() {
                    @Override
                    public KeyValueRowResult apply(RowResult<byte[]> row) {
                        return KeyValueRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<KeyValueRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KeyValueRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<KeyValueRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, KeyValueRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<KeyValueRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, KeyValueRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(KeyValueRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<KeyValueRow>, RuntimeException>() {
            @Override
            public boolean visit(List<KeyValueRow> rows) {
                delete(rows);
                return true;
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
    static String __CLASS_HASH = "Zj0rVKfCofCDDchhuD5R2w==";
}
