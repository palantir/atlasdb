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
public final class TestHashComponentsStreamValueTable implements
        AtlasDbMutablePersistentTable<TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow,
                                         TestHashComponentsStreamValueTable.TestHashComponentsStreamValueNamedColumnValue<?>,
                                         TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRowResult>,
        AtlasDbNamedMutableTable<TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow,
                                    TestHashComponentsStreamValueTable.TestHashComponentsStreamValueNamedColumnValue<?>,
                                    TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRowResult> {
    private final Transaction t;
    private final List<TestHashComponentsStreamValueTrigger> triggers;
    private final static String rawTableName = "test_hash_components_stream_value";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TestHashComponentsStreamValueNamedColumn.values());

    static TestHashComponentsStreamValueTable of(Transaction t, Namespace namespace) {
        return new TestHashComponentsStreamValueTable(t, namespace, ImmutableList.<TestHashComponentsStreamValueTrigger>of());
    }

    static TestHashComponentsStreamValueTable of(Transaction t, Namespace namespace, TestHashComponentsStreamValueTrigger trigger, TestHashComponentsStreamValueTrigger... triggers) {
        return new TestHashComponentsStreamValueTable(t, namespace, ImmutableList.<TestHashComponentsStreamValueTrigger>builder().add(trigger).add(triggers).build());
    }

    static TestHashComponentsStreamValueTable of(Transaction t, Namespace namespace, List<TestHashComponentsStreamValueTrigger> triggers) {
        return new TestHashComponentsStreamValueTable(t, namespace, triggers);
    }

    private TestHashComponentsStreamValueTable(Transaction t, Namespace namespace, List<TestHashComponentsStreamValueTrigger> triggers) {
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
     * TestHashComponentsStreamValueRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long id};
     *   {@literal Long blockId};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamValueRow implements Persistable, Comparable<TestHashComponentsStreamValueRow> {
        private final long hashOfRowComponents;
        private final long id;
        private final long blockId;

        public static TestHashComponentsStreamValueRow of(long id, long blockId) {
            long hashOfRowComponents = computeHashFirstComponents(id, blockId);
            return new TestHashComponentsStreamValueRow(hashOfRowComponents, id, blockId);
        }

        private TestHashComponentsStreamValueRow(long hashOfRowComponents, long id, long blockId) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.id = id;
            this.blockId = blockId;
        }

        public long getId() {
            return id;
        }

        public long getBlockId() {
            return blockId;
        }

        public static Function<TestHashComponentsStreamValueRow, Long> getIdFun() {
            return new Function<TestHashComponentsStreamValueRow, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamValueRow row) {
                    return row.id;
                }
            };
        }

        public static Function<TestHashComponentsStreamValueRow, Long> getBlockIdFun() {
            return new Function<TestHashComponentsStreamValueRow, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamValueRow row) {
                    return row.blockId;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
            return EncodingUtils.add(hashOfRowComponentsBytes, idBytes, blockIdBytes);
        }

        public static final Hydrator<TestHashComponentsStreamValueRow> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamValueRow>() {
            @Override
            public TestHashComponentsStreamValueRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(id);
                Long blockId = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(blockId);
                return new TestHashComponentsStreamValueRow(hashOfRowComponents, id, blockId);
            }
        };

        public static long computeHashFirstComponents(long id, long blockId) {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(idBytes, blockIdBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
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
            TestHashComponentsStreamValueRow other = (TestHashComponentsStreamValueRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(id, other.id) && Objects.equals(blockId, other.blockId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, id, blockId });
        }

        @Override
        public int compareTo(TestHashComponentsStreamValueRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.id, o.id)
                .compare(this.blockId, o.blockId)
                .result();
        }
    }

    public interface TestHashComponentsStreamValueNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Value implements TestHashComponentsStreamValueNamedColumnValue<byte[]> {
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

    public interface TestHashComponentsStreamValueTrigger {
        public void putTestHashComponentsStreamValue(Multimap<TestHashComponentsStreamValueRow, ? extends TestHashComponentsStreamValueNamedColumnValue<?>> newRows);
    }

    public static final class TestHashComponentsStreamValueRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TestHashComponentsStreamValueRowResult of(RowResult<byte[]> row) {
            return new TestHashComponentsStreamValueRowResult(row);
        }

        private TestHashComponentsStreamValueRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TestHashComponentsStreamValueRow getRowName() {
            return TestHashComponentsStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TestHashComponentsStreamValueRowResult, TestHashComponentsStreamValueRow> getRowNameFun() {
            return new Function<TestHashComponentsStreamValueRowResult, TestHashComponentsStreamValueRow>() {
                @Override
                public TestHashComponentsStreamValueRow apply(TestHashComponentsStreamValueRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TestHashComponentsStreamValueRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TestHashComponentsStreamValueRowResult>() {
                @Override
                public TestHashComponentsStreamValueRowResult apply(RowResult<byte[]> rowResult) {
                    return new TestHashComponentsStreamValueRowResult(rowResult);
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

        public static Function<TestHashComponentsStreamValueRowResult, byte[]> getValueFun() {
            return new Function<TestHashComponentsStreamValueRowResult, byte[]>() {
                @Override
                public byte[] apply(TestHashComponentsStreamValueRowResult rowResult) {
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

    public enum TestHashComponentsStreamValueNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TestHashComponentsStreamValueNamedColumn, byte[]> toShortName() {
            return new Function<TestHashComponentsStreamValueNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TestHashComponentsStreamValueNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TestHashComponentsStreamValueNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TestHashComponentsStreamValueNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TestHashComponentsStreamValueNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TestHashComponentsStreamValueNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TestHashComponentsStreamValueNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<TestHashComponentsStreamValueRow, byte[]> getValues(Collection<TestHashComponentsStreamValueRow> rows) {
        Map<Cell, TestHashComponentsStreamValueRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TestHashComponentsStreamValueRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TestHashComponentsStreamValueRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(TestHashComponentsStreamValueRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<TestHashComponentsStreamValueRow, byte[]> map) {
        Map<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TestHashComponentsStreamValueRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TestHashComponentsStreamValueRow, ? extends TestHashComponentsStreamValueNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TestHashComponentsStreamValueTrigger trigger : triggers) {
            trigger.putTestHashComponentsStreamValue(rows);
        }
    }

    public void deleteValue(TestHashComponentsStreamValueRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<TestHashComponentsStreamValueRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TestHashComponentsStreamValueRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TestHashComponentsStreamValueRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<TestHashComponentsStreamValueRowResult> getRow(TestHashComponentsStreamValueRow row) {
        return getRow(row, allColumns);
    }

    public Optional<TestHashComponentsStreamValueRowResult> getRow(TestHashComponentsStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(TestHashComponentsStreamValueRowResult.of(rowResult));
        }
    }

    @Override
    public List<TestHashComponentsStreamValueRowResult> getRows(Iterable<TestHashComponentsStreamValueRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamValueRowResult> getRows(Iterable<TestHashComponentsStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TestHashComponentsStreamValueRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TestHashComponentsStreamValueRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TestHashComponentsStreamValueNamedColumnValue<?>> getRowColumns(TestHashComponentsStreamValueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamValueNamedColumnValue<?>> getRowColumns(TestHashComponentsStreamValueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TestHashComponentsStreamValueNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<TestHashComponentsStreamValueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> getRowsMultimap(Iterable<TestHashComponentsStreamValueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TestHashComponentsStreamValueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TestHashComponentsStreamValueRow, BatchingVisitable<TestHashComponentsStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<TestHashComponentsStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamValueRow, BatchingVisitable<TestHashComponentsStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TestHashComponentsStreamValueNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TestHashComponentsStreamValueRow, TestHashComponentsStreamValueNamedColumnValue<?>>> getRowsColumnRange(Iterable<TestHashComponentsStreamValueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TestHashComponentsStreamValueNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TestHashComponentsStreamValueRow, Iterator<TestHashComponentsStreamValueNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<TestHashComponentsStreamValueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamValueRow, Iterator<TestHashComponentsStreamValueNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamValueRow row = TestHashComponentsStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TestHashComponentsStreamValueNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<TestHashComponentsStreamValueRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TestHashComponentsStreamValueRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TestHashComponentsStreamValueRowResult>() {
            @Override
            public TestHashComponentsStreamValueRowResult apply(RowResult<byte[]> input) {
                return TestHashComponentsStreamValueRowResult.of(input);
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
    static String __CLASS_HASH = "RGE142O7UD2OKjOspNm8Hw==";
}
