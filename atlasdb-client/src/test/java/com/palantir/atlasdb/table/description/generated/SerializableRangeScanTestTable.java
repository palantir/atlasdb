package com.palantir.atlasdb.table.description.generated;

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
public final class SerializableRangeScanTestTable implements
        AtlasDbMutablePersistentTable<SerializableRangeScanTestTable.SerializableRangeScanTestRow,
                                         SerializableRangeScanTestTable.SerializableRangeScanTestNamedColumnValue<?>,
                                         SerializableRangeScanTestTable.SerializableRangeScanTestRowResult>,
        AtlasDbNamedMutableTable<SerializableRangeScanTestTable.SerializableRangeScanTestRow,
                                    SerializableRangeScanTestTable.SerializableRangeScanTestNamedColumnValue<?>,
                                    SerializableRangeScanTestTable.SerializableRangeScanTestRowResult> {
    private final Transaction t;
    private final List<SerializableRangeScanTestTrigger> triggers;
    private final static String rawTableName = "serializableRangeScanTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SerializableRangeScanTestNamedColumn.values());

    static SerializableRangeScanTestTable of(Transaction t, Namespace namespace) {
        return new SerializableRangeScanTestTable(t, namespace, ImmutableList.<SerializableRangeScanTestTrigger>of());
    }

    static SerializableRangeScanTestTable of(Transaction t, Namespace namespace, SerializableRangeScanTestTrigger trigger, SerializableRangeScanTestTrigger... triggers) {
        return new SerializableRangeScanTestTable(t, namespace, ImmutableList.<SerializableRangeScanTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static SerializableRangeScanTestTable of(Transaction t, Namespace namespace, List<SerializableRangeScanTestTrigger> triggers) {
        return new SerializableRangeScanTestTable(t, namespace, triggers);
    }

    private SerializableRangeScanTestTable(Transaction t, Namespace namespace, List<SerializableRangeScanTestTrigger> triggers) {
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
     * SerializableRangeScanTestRow {
     *   {@literal String component1};
     * }
     * </pre>
     */
    public static final class SerializableRangeScanTestRow implements Persistable, Comparable<SerializableRangeScanTestRow> {
        private final String component1;

        public static SerializableRangeScanTestRow of(String component1) {
            return new SerializableRangeScanTestRow(component1);
        }

        private SerializableRangeScanTestRow(String component1) {
            this.component1 = component1;
        }

        public String getComponent1() {
            return component1;
        }

        public static Function<SerializableRangeScanTestRow, String> getComponent1Fun() {
            return new Function<SerializableRangeScanTestRow, String>() {
                @Override
                public String apply(SerializableRangeScanTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<String, SerializableRangeScanTestRow> fromComponent1Fun() {
            return new Function<String, SerializableRangeScanTestRow>() {
                @Override
                public SerializableRangeScanTestRow apply(String row) {
                    return SerializableRangeScanTestRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component1Bytes = PtBytes.toBytes(component1);
            return EncodingUtils.add(component1Bytes);
        }

        public static final Hydrator<SerializableRangeScanTestRow> BYTES_HYDRATOR = new Hydrator<SerializableRangeScanTestRow>() {
            @Override
            public SerializableRangeScanTestRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String component1 = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SerializableRangeScanTestRow(component1);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("component1", component1)
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
            SerializableRangeScanTestRow other = (SerializableRangeScanTestRow) obj;
            return Objects.equals(component1, other.component1);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(component1);
        }

        @Override
        public int compareTo(SerializableRangeScanTestRow o) {
            return ComparisonChain.start()
                .compare(this.component1, o.component1)
                .result();
        }
    }

    public interface SerializableRangeScanTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column1 implements SerializableRangeScanTestNamedColumnValue<Long> {
        private final Long value;

        public static Column1 of(Long value) {
            return new Column1(value);
        }

        private Column1(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column1";
        }

        @Override
        public String getShortColumnName() {
            return "c";
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
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<Column1> BYTES_HYDRATOR = new Hydrator<Column1>() {
            @Override
            public Column1 hydrateFromBytes(byte[] bytes) {
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

    public interface SerializableRangeScanTestTrigger {
        public void putSerializableRangeScanTest(Multimap<SerializableRangeScanTestRow, ? extends SerializableRangeScanTestNamedColumnValue<?>> newRows);
    }

    public static final class SerializableRangeScanTestRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SerializableRangeScanTestRowResult of(RowResult<byte[]> row) {
            return new SerializableRangeScanTestRowResult(row);
        }

        private SerializableRangeScanTestRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SerializableRangeScanTestRow getRowName() {
            return SerializableRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SerializableRangeScanTestRowResult, SerializableRangeScanTestRow> getRowNameFun() {
            return new Function<SerializableRangeScanTestRowResult, SerializableRangeScanTestRow>() {
                @Override
                public SerializableRangeScanTestRow apply(SerializableRangeScanTestRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SerializableRangeScanTestRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SerializableRangeScanTestRowResult>() {
                @Override
                public SerializableRangeScanTestRowResult apply(RowResult<byte[]> rowResult) {
                    return new SerializableRangeScanTestRowResult(rowResult);
                }
            };
        }

        public boolean hasColumn1() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public Long getColumn1() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            Column1 value = Column1.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SerializableRangeScanTestRowResult, Long> getColumn1Fun() {
            return new Function<SerializableRangeScanTestRowResult, Long>() {
                @Override
                public Long apply(SerializableRangeScanTestRowResult rowResult) {
                    return rowResult.getColumn1();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Column1", getColumn1())
                .toString();
        }
    }

    public enum SerializableRangeScanTestNamedColumn {
        COLUMN1 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SerializableRangeScanTestNamedColumn, byte[]> toShortName() {
            return new Function<SerializableRangeScanTestNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SerializableRangeScanTestNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SerializableRangeScanTestNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SerializableRangeScanTestNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SerializableRangeScanTestNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SerializableRangeScanTestNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SerializableRangeScanTestNamedColumnValue<?>>>builder()
                .put("c", Column1.BYTES_HYDRATOR)
                .build();

    public Map<SerializableRangeScanTestRow, Long> getColumn1s(Collection<SerializableRangeScanTestRow> rows) {
        Map<Cell, SerializableRangeScanTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SerializableRangeScanTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SerializableRangeScanTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column1.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn1(SerializableRangeScanTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1(Map<SerializableRangeScanTestRow, Long> map) {
        Map<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SerializableRangeScanTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SerializableRangeScanTestRow, ? extends SerializableRangeScanTestNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SerializableRangeScanTestTrigger trigger : triggers) {
            trigger.putSerializableRangeScanTest(rows);
        }
    }

    public void deleteColumn1(SerializableRangeScanTestRow row) {
        deleteColumn1(ImmutableSet.of(row));
    }

    public void deleteColumn1(Iterable<SerializableRangeScanTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SerializableRangeScanTestRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SerializableRangeScanTestRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        t.delete(tableRef, cells);
    }

    public Optional<SerializableRangeScanTestRowResult> getRow(SerializableRangeScanTestRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SerializableRangeScanTestRowResult> getRow(SerializableRangeScanTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SerializableRangeScanTestRowResult.of(rowResult));
        }
    }

    @Override
    public List<SerializableRangeScanTestRowResult> getRows(Iterable<SerializableRangeScanTestRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SerializableRangeScanTestRowResult> getRows(Iterable<SerializableRangeScanTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SerializableRangeScanTestRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SerializableRangeScanTestRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SerializableRangeScanTestNamedColumnValue<?>> getRowColumns(SerializableRangeScanTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SerializableRangeScanTestNamedColumnValue<?>> getRowColumns(SerializableRangeScanTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SerializableRangeScanTestNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> getRowsMultimap(Iterable<SerializableRangeScanTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> getRowsMultimap(Iterable<SerializableRangeScanTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SerializableRangeScanTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SerializableRangeScanTestRow row = SerializableRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SerializableRangeScanTestRow, BatchingVisitable<SerializableRangeScanTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<SerializableRangeScanTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SerializableRangeScanTestRow, BatchingVisitable<SerializableRangeScanTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SerializableRangeScanTestRow row = SerializableRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SerializableRangeScanTestNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SerializableRangeScanTestRow, SerializableRangeScanTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<SerializableRangeScanTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SerializableRangeScanTestRow row = SerializableRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SerializableRangeScanTestNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SerializableRangeScanTestRow, Iterator<SerializableRangeScanTestNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SerializableRangeScanTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SerializableRangeScanTestRow, Iterator<SerializableRangeScanTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SerializableRangeScanTestRow row = SerializableRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SerializableRangeScanTestNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<SerializableRangeScanTestRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, SerializableRangeScanTestRowResult>() {
            @Override
            public SerializableRangeScanTestRowResult apply(RowResult<byte[]> input) {
                return SerializableRangeScanTestRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<SerializableRangeScanTestRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<SerializableRangeScanTestRowResult>>() {
            @Override
            public BatchingVisitable<SerializableRangeScanTestRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, SerializableRangeScanTestRowResult>() {
                    @Override
                    public SerializableRangeScanTestRowResult apply(RowResult<byte[]> row) {
                        return SerializableRangeScanTestRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<SerializableRangeScanTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SerializableRangeScanTestRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<SerializableRangeScanTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SerializableRangeScanTestRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<SerializableRangeScanTestRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, SerializableRangeScanTestRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(SerializableRangeScanTestRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<SerializableRangeScanTestRow>, RuntimeException>() {
            @Override
            public boolean visit(List<SerializableRangeScanTestRow> rows) {
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
    static String __CLASS_HASH = "S7n1Yg2zfjD9NmmHwJCORQ==";
}
