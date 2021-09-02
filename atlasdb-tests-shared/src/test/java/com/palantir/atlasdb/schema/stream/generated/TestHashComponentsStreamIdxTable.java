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
public final class TestHashComponentsStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow,
                                                TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumn,
                                                TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue,
                                                TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRowResult> {
    private final Transaction t;
    private final List<TestHashComponentsStreamIdxTrigger> triggers;
    private final static String rawTableName = "test_hash_components_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static TestHashComponentsStreamIdxTable of(Transaction t, Namespace namespace) {
        return new TestHashComponentsStreamIdxTable(t, namespace, ImmutableList.<TestHashComponentsStreamIdxTrigger>of());
    }

    static TestHashComponentsStreamIdxTable of(Transaction t, Namespace namespace, TestHashComponentsStreamIdxTrigger trigger, TestHashComponentsStreamIdxTrigger... triggers) {
        return new TestHashComponentsStreamIdxTable(t, namespace, ImmutableList.<TestHashComponentsStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static TestHashComponentsStreamIdxTable of(Transaction t, Namespace namespace, List<TestHashComponentsStreamIdxTrigger> triggers) {
        return new TestHashComponentsStreamIdxTable(t, namespace, triggers);
    }

    private TestHashComponentsStreamIdxTable(Transaction t, Namespace namespace, List<TestHashComponentsStreamIdxTrigger> triggers) {
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
     * TestHashComponentsStreamIdxRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamIdxRow implements Persistable, Comparable<TestHashComponentsStreamIdxRow> {
        private final long hashOfRowComponents;
        private final long id;

        public static TestHashComponentsStreamIdxRow of(long id) {
            long hashOfRowComponents = computeHashFirstComponents(id);
            return new TestHashComponentsStreamIdxRow(hashOfRowComponents, id);
        }

        private TestHashComponentsStreamIdxRow(long hashOfRowComponents, long id) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<TestHashComponentsStreamIdxRow, Long> getIdFun() {
            return new Function<TestHashComponentsStreamIdxRow, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, TestHashComponentsStreamIdxRow> fromIdFun() {
            return new Function<Long, TestHashComponentsStreamIdxRow>() {
                @Override
                public TestHashComponentsStreamIdxRow apply(Long row) {
                    return TestHashComponentsStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(hashOfRowComponentsBytes, idBytes);
        }

        public static final Hydrator<TestHashComponentsStreamIdxRow> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamIdxRow>() {
            @Override
            public TestHashComponentsStreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new TestHashComponentsStreamIdxRow(hashOfRowComponents, id);
            }
        };

        public static long computeHashFirstComponents(long id) {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(idBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
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
            TestHashComponentsStreamIdxRow other = (TestHashComponentsStreamIdxRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, id });
        }

        @Override
        public int compareTo(TestHashComponentsStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * TestHashComponentsStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamIdxColumn implements Persistable, Comparable<TestHashComponentsStreamIdxColumn> {
        private final byte[] reference;

        public static TestHashComponentsStreamIdxColumn of(byte[] reference) {
            return new TestHashComponentsStreamIdxColumn(reference);
        }

        private TestHashComponentsStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<TestHashComponentsStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<TestHashComponentsStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(TestHashComponentsStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], TestHashComponentsStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], TestHashComponentsStreamIdxColumn>() {
                @Override
                public TestHashComponentsStreamIdxColumn apply(byte[] row) {
                    return TestHashComponentsStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<TestHashComponentsStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamIdxColumn>() {
            @Override
            public TestHashComponentsStreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return new TestHashComponentsStreamIdxColumn(reference);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("reference", reference)
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
            TestHashComponentsStreamIdxColumn other = (TestHashComponentsStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(TestHashComponentsStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface TestHashComponentsStreamIdxTrigger {
        public void putTestHashComponentsStreamIdx(Multimap<TestHashComponentsStreamIdxRow, ? extends TestHashComponentsStreamIdxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal byte[] reference};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamIdxColumnValue implements ColumnValue<Long> {
        private final TestHashComponentsStreamIdxColumn columnName;
        private final Long value;

        public static TestHashComponentsStreamIdxColumnValue of(TestHashComponentsStreamIdxColumn columnName, Long value) {
            return new TestHashComponentsStreamIdxColumnValue(columnName, value);
        }

        private TestHashComponentsStreamIdxColumnValue(TestHashComponentsStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public TestHashComponentsStreamIdxColumn getColumnName() {
            return columnName;
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static Long hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return EncodingUtils.decodeUnsignedVarLong(bytes, 0);
        }

        public static Function<TestHashComponentsStreamIdxColumnValue, TestHashComponentsStreamIdxColumn> getColumnNameFun() {
            return new Function<TestHashComponentsStreamIdxColumnValue, TestHashComponentsStreamIdxColumn>() {
                @Override
                public TestHashComponentsStreamIdxColumn apply(TestHashComponentsStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<TestHashComponentsStreamIdxColumnValue, Long> getValueFun() {
            return new Function<TestHashComponentsStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamIdxColumnValue columnValue) {
                    return columnValue.getValue();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("ColumnName", this.columnName)
                .add("Value", this.value)
                .toString();
        }
    }

    public static final class TestHashComponentsStreamIdxRowResult implements TypedRowResult {
        private final TestHashComponentsStreamIdxRow rowName;
        private final ImmutableSet<TestHashComponentsStreamIdxColumnValue> columnValues;

        public static TestHashComponentsStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            TestHashComponentsStreamIdxRow rowName = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<TestHashComponentsStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = TestHashComponentsStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(TestHashComponentsStreamIdxColumnValue.of(col, value));
            }
            return new TestHashComponentsStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private TestHashComponentsStreamIdxRowResult(TestHashComponentsStreamIdxRow rowName, ImmutableSet<TestHashComponentsStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public TestHashComponentsStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<TestHashComponentsStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<TestHashComponentsStreamIdxRowResult, TestHashComponentsStreamIdxRow> getRowNameFun() {
            return new Function<TestHashComponentsStreamIdxRowResult, TestHashComponentsStreamIdxRow>() {
                @Override
                public TestHashComponentsStreamIdxRow apply(TestHashComponentsStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<TestHashComponentsStreamIdxRowResult, ImmutableSet<TestHashComponentsStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<TestHashComponentsStreamIdxRowResult, ImmutableSet<TestHashComponentsStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<TestHashComponentsStreamIdxColumnValue> apply(TestHashComponentsStreamIdxRowResult rowResult) {
                    return rowResult.columnValues;
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("ColumnValues", getColumnValues())
                .toString();
        }
    }

    @Override
    public void delete(TestHashComponentsStreamIdxRow row, TestHashComponentsStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<TestHashComponentsStreamIdxRow> rows) {
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(TestHashComponentsStreamIdxRow rowName, Iterable<TestHashComponentsStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(TestHashComponentsStreamIdxRow rowName, TestHashComponentsStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<TestHashComponentsStreamIdxRow, ? extends TestHashComponentsStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (TestHashComponentsStreamIdxTrigger trigger : triggers) {
            trigger.putTestHashComponentsStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumn> values) {
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<TestHashComponentsStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(TestHashComponentsStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> get(Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                TestHashComponentsStreamIdxRow row = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, TestHashComponentsStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<TestHashComponentsStreamIdxColumnValue> getRowColumns(TestHashComponentsStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamIdxColumnValue> getRowColumns(TestHashComponentsStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TestHashComponentsStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(TestHashComponentsStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> getRowsMultimap(Iterable<TestHashComponentsStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> getRowsMultimap(Iterable<TestHashComponentsStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> getRowsMultimapInternal(Iterable<TestHashComponentsStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TestHashComponentsStreamIdxRow row = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, TestHashComponentsStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TestHashComponentsStreamIdxRow, BatchingVisitable<TestHashComponentsStreamIdxColumnValue>> getRowsColumnRange(Iterable<TestHashComponentsStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamIdxRow, BatchingVisitable<TestHashComponentsStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamIdxRow row = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TestHashComponentsStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(result.getValue());
                return TestHashComponentsStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxColumnValue>> getRowsColumnRange(Iterable<TestHashComponentsStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TestHashComponentsStreamIdxRow row = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(e.getValue());
            TestHashComponentsStreamIdxColumnValue colValue = TestHashComponentsStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TestHashComponentsStreamIdxRow, Iterator<TestHashComponentsStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<TestHashComponentsStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamIdxRow, Iterator<TestHashComponentsStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamIdxRow row = TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TestHashComponentsStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                TestHashComponentsStreamIdxColumn col = TestHashComponentsStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = TestHashComponentsStreamIdxColumnValue.hydrateValue(result.getValue());
                return TestHashComponentsStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<TestHashComponentsStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TestHashComponentsStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TestHashComponentsStreamIdxRowResult>() {
            @Override
            public TestHashComponentsStreamIdxRowResult apply(RowResult<byte[]> input) {
                return TestHashComponentsStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "Qn6mO587PWFhTpgT4rdwPw==";
}
