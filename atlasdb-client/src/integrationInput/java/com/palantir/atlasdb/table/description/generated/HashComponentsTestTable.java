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
public final class HashComponentsTestTable implements
        AtlasDbMutablePersistentTable<HashComponentsTestTable.HashComponentsTestRow,
                                         HashComponentsTestTable.HashComponentsTestNamedColumnValue<?>,
                                         HashComponentsTestTable.HashComponentsTestRowResult>,
        AtlasDbNamedMutableTable<HashComponentsTestTable.HashComponentsTestRow,
                                    HashComponentsTestTable.HashComponentsTestNamedColumnValue<?>,
                                    HashComponentsTestTable.HashComponentsTestRowResult> {
    private final Transaction t;
    private final List<HashComponentsTestTrigger> triggers;
    private final static String rawTableName = "HashComponentsTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(HashComponentsTestNamedColumn.values());

    static HashComponentsTestTable of(Transaction t, Namespace namespace) {
        return new HashComponentsTestTable(t, namespace, ImmutableList.<HashComponentsTestTrigger>of());
    }

    static HashComponentsTestTable of(Transaction t, Namespace namespace, HashComponentsTestTrigger trigger, HashComponentsTestTrigger... triggers) {
        return new HashComponentsTestTable(t, namespace, ImmutableList.<HashComponentsTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static HashComponentsTestTable of(Transaction t, Namespace namespace, List<HashComponentsTestTrigger> triggers) {
        return new HashComponentsTestTable(t, namespace, triggers);
    }

    private HashComponentsTestTable(Transaction t, Namespace namespace, List<HashComponentsTestTrigger> triggers) {
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
     * HashComponentsTestRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long component1};
     *   {@literal String component2};
     * }
     * </pre>
     */
    public static final class HashComponentsTestRow implements Persistable, Comparable<HashComponentsTestRow> {
        private final long hashOfRowComponents;
        private final long component1;
        private final String component2;

        public static HashComponentsTestRow of(long component1, String component2) {
            long hashOfRowComponents = computeHashFirstComponents(component1, component2);
            return new HashComponentsTestRow(hashOfRowComponents, component1, component2);
        }

        private HashComponentsTestRow(long hashOfRowComponents, long component1, String component2) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.component1 = component1;
            this.component2 = component2;
        }

        public long getComponent1() {
            return component1;
        }

        public String getComponent2() {
            return component2;
        }

        public static Function<HashComponentsTestRow, Long> getComponent1Fun() {
            return new Function<HashComponentsTestRow, Long>() {
                @Override
                public Long apply(HashComponentsTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<HashComponentsTestRow, String> getComponent2Fun() {
            return new Function<HashComponentsTestRow, String>() {
                @Override
                public String apply(HashComponentsTestRow row) {
                    return row.component2;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(component1);
            byte[] component2Bytes = EncodingUtils.encodeVarString(component2);
            return EncodingUtils.add(hashOfRowComponentsBytes, component1Bytes, component2Bytes);
        }

        public static final Hydrator<HashComponentsTestRow> BYTES_HYDRATOR = new Hydrator<HashComponentsTestRow>() {
            @Override
            public HashComponentsTestRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                Long component1 = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(component1);
                String component2 = EncodingUtils.decodeVarString(_input, _index);
                _index += EncodingUtils.sizeOfVarString(component2);
                return new HashComponentsTestRow(hashOfRowComponents, component1, component2);
            }
        };

        public static long computeHashFirstComponents(long component1, String component2) {
            byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(component1);
            byte[] component2Bytes = EncodingUtils.encodeVarString(component2);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(component1Bytes, component2Bytes)).asLong();
        }

        public static RangeRequest.Builder createPrefixRangeUnsorted(long component1, String component2) {
            long hashOfRowComponents = computeHashFirstComponents(component1, component2);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(component1);
            byte[] component2Bytes = EncodingUtils.encodeVarString(component2);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(hashOfRowComponentsBytes, component1Bytes, component2Bytes));
        }

        public static Prefix prefixUnsorted(long component1, String component2) {
            long hashOfRowComponents = computeHashFirstComponents(component1, component2);
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(component1);
            byte[] component2Bytes = EncodingUtils.encodeVarString(component2);
            return new Prefix(EncodingUtils.add(hashOfRowComponentsBytes, component1Bytes, component2Bytes));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("component1", component1)
                .add("component2", component2)
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
            HashComponentsTestRow other = (HashComponentsTestRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(component1, other.component1) && Objects.equals(component2, other.component2);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, component1, component2 });
        }

        @Override
        public int compareTo(HashComponentsTestRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.component1, o.component1)
                .compare(this.component2, o.component2)
                .result();
        }
    }

    public interface HashComponentsTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class Column implements HashComponentsTestNamedColumnValue<String> {
        private final String value;

        public static Column of(String value) {
            return new Column(value);
        }

        private Column(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column";
        }

        @Override
        public String getShortColumnName() {
            return "c";
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<Column> BYTES_HYDRATOR = new Hydrator<Column>() {
            @Override
            public Column hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(PtBytes.toString(bytes, 0, bytes.length-0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface HashComponentsTestTrigger {
        public void putHashComponentsTest(Multimap<HashComponentsTestRow, ? extends HashComponentsTestNamedColumnValue<?>> newRows);
    }

    public static final class HashComponentsTestRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static HashComponentsTestRowResult of(RowResult<byte[]> row) {
            return new HashComponentsTestRowResult(row);
        }

        private HashComponentsTestRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public HashComponentsTestRow getRowName() {
            return HashComponentsTestRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<HashComponentsTestRowResult, HashComponentsTestRow> getRowNameFun() {
            return new Function<HashComponentsTestRowResult, HashComponentsTestRow>() {
                @Override
                public HashComponentsTestRow apply(HashComponentsTestRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, HashComponentsTestRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, HashComponentsTestRowResult>() {
                @Override
                public HashComponentsTestRowResult apply(RowResult<byte[]> rowResult) {
                    return new HashComponentsTestRowResult(rowResult);
                }
            };
        }

        public boolean hasColumn() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public String getColumn() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            Column value = Column.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<HashComponentsTestRowResult, String> getColumnFun() {
            return new Function<HashComponentsTestRowResult, String>() {
                @Override
                public String apply(HashComponentsTestRowResult rowResult) {
                    return rowResult.getColumn();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Column", getColumn())
                .toString();
        }
    }

    public enum HashComponentsTestNamedColumn {
        COLUMN {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        };

        public abstract byte[] getShortName();

        public static Function<HashComponentsTestNamedColumn, byte[]> toShortName() {
            return new Function<HashComponentsTestNamedColumn, byte[]>() {
                @Override
                public byte[] apply(HashComponentsTestNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<HashComponentsTestNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, HashComponentsTestNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(HashComponentsTestNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends HashComponentsTestNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends HashComponentsTestNamedColumnValue<?>>>builder()
                .put("c", Column.BYTES_HYDRATOR)
                .build();

    public Map<HashComponentsTestRow, String> getColumns(Collection<HashComponentsTestRow> rows) {
        Map<Cell, HashComponentsTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (HashComponentsTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<HashComponentsTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = Column.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn(HashComponentsTestRow row, String value) {
        put(ImmutableMultimap.of(row, Column.of(value)));
    }

    public void putColumn(Map<HashComponentsTestRow, String> map) {
        Map<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<HashComponentsTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Column.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<HashComponentsTestRow, ? extends HashComponentsTestNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (HashComponentsTestTrigger trigger : triggers) {
            trigger.putHashComponentsTest(rows);
        }
    }

    public void deleteColumn(HashComponentsTestRow row) {
        deleteColumn(ImmutableSet.of(row));
    }

    public void deleteColumn(Iterable<HashComponentsTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(HashComponentsTestRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<HashComponentsTestRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        t.delete(tableRef, cells);
    }

    public Optional<HashComponentsTestRowResult> getRow(HashComponentsTestRow row) {
        return getRow(row, allColumns);
    }

    public Optional<HashComponentsTestRowResult> getRow(HashComponentsTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(HashComponentsTestRowResult.of(rowResult));
        }
    }

    @Override
    public List<HashComponentsTestRowResult> getRows(Iterable<HashComponentsTestRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<HashComponentsTestRowResult> getRows(Iterable<HashComponentsTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<HashComponentsTestRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(HashComponentsTestRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<HashComponentsTestNamedColumnValue<?>> getRowColumns(HashComponentsTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<HashComponentsTestNamedColumnValue<?>> getRowColumns(HashComponentsTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<HashComponentsTestNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> getRowsMultimap(Iterable<HashComponentsTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> getRowsMultimap(Iterable<HashComponentsTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> getRowsMultimapInternal(Iterable<HashComponentsTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            HashComponentsTestRow row = HashComponentsTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<HashComponentsTestRow, BatchingVisitable<HashComponentsTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<HashComponentsTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<HashComponentsTestRow, BatchingVisitable<HashComponentsTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            HashComponentsTestRow row = HashComponentsTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<HashComponentsTestNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<HashComponentsTestRow, HashComponentsTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<HashComponentsTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            HashComponentsTestRow row = HashComponentsTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            HashComponentsTestNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<HashComponentsTestRow, Iterator<HashComponentsTestNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<HashComponentsTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<HashComponentsTestRow, Iterator<HashComponentsTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            HashComponentsTestRow row = HashComponentsTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<HashComponentsTestNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<HashComponentsTestRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, HashComponentsTestRowResult>() {
            @Override
            public HashComponentsTestRowResult apply(RowResult<byte[]> input) {
                return HashComponentsTestRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<HashComponentsTestRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<HashComponentsTestRowResult>>() {
            @Override
            public BatchingVisitable<HashComponentsTestRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, HashComponentsTestRowResult>() {
                    @Override
                    public HashComponentsTestRowResult apply(RowResult<byte[]> row) {
                        return HashComponentsTestRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<HashComponentsTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, HashComponentsTestRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<HashComponentsTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, HashComponentsTestRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<HashComponentsTestRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, HashComponentsTestRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(HashComponentsTestRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<HashComponentsTestRow>, RuntimeException>() {
            @Override
            public boolean visit(List<HashComponentsTestRow> rows) {
                delete(rows);
                return true;
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
    static String __CLASS_HASH = "f9YdA/BXtBTIWOZBN9hoWg==";
}
