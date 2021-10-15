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
public final class TestHashComponentsStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow,
                                                TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumn,
                                                TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxColumnValue,
                                                TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<TestHashComponentsStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "test_hash_components_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static TestHashComponentsStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new TestHashComponentsStreamHashAidxTable(t, namespace, ImmutableList.<TestHashComponentsStreamHashAidxTrigger>of());
    }

    static TestHashComponentsStreamHashAidxTable of(Transaction t, Namespace namespace, TestHashComponentsStreamHashAidxTrigger trigger, TestHashComponentsStreamHashAidxTrigger... triggers) {
        return new TestHashComponentsStreamHashAidxTable(t, namespace, ImmutableList.<TestHashComponentsStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static TestHashComponentsStreamHashAidxTable of(Transaction t, Namespace namespace, List<TestHashComponentsStreamHashAidxTrigger> triggers) {
        return new TestHashComponentsStreamHashAidxTable(t, namespace, triggers);
    }

    private TestHashComponentsStreamHashAidxTable(Transaction t, Namespace namespace, List<TestHashComponentsStreamHashAidxTrigger> triggers) {
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
     * TestHashComponentsStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamHashAidxRow implements Persistable, Comparable<TestHashComponentsStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static TestHashComponentsStreamHashAidxRow of(Sha256Hash hash) {
            return new TestHashComponentsStreamHashAidxRow(hash);
        }

        private TestHashComponentsStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<TestHashComponentsStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<TestHashComponentsStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(TestHashComponentsStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, TestHashComponentsStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, TestHashComponentsStreamHashAidxRow>() {
                @Override
                public TestHashComponentsStreamHashAidxRow apply(Sha256Hash row) {
                    return TestHashComponentsStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<TestHashComponentsStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamHashAidxRow>() {
            @Override
            public TestHashComponentsStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new TestHashComponentsStreamHashAidxRow(hash);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hash", hash)
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
            TestHashComponentsStreamHashAidxRow other = (TestHashComponentsStreamHashAidxRow) obj;
            return Objects.equals(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(TestHashComponentsStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * TestHashComponentsStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamHashAidxColumn implements Persistable, Comparable<TestHashComponentsStreamHashAidxColumn> {
        private final long streamId;

        public static TestHashComponentsStreamHashAidxColumn of(long streamId) {
            return new TestHashComponentsStreamHashAidxColumn(streamId);
        }

        private TestHashComponentsStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<TestHashComponentsStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<TestHashComponentsStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, TestHashComponentsStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, TestHashComponentsStreamHashAidxColumn>() {
                @Override
                public TestHashComponentsStreamHashAidxColumn apply(Long row) {
                    return TestHashComponentsStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<TestHashComponentsStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamHashAidxColumn>() {
            @Override
            public TestHashComponentsStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new TestHashComponentsStreamHashAidxColumn(streamId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("streamId", streamId)
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
            TestHashComponentsStreamHashAidxColumn other = (TestHashComponentsStreamHashAidxColumn) obj;
            return Objects.equals(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(TestHashComponentsStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface TestHashComponentsStreamHashAidxTrigger {
        public void putTestHashComponentsStreamHashAidx(Multimap<TestHashComponentsStreamHashAidxRow, ? extends TestHashComponentsStreamHashAidxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long streamId};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final TestHashComponentsStreamHashAidxColumn columnName;
        private final Long value;

        public static TestHashComponentsStreamHashAidxColumnValue of(TestHashComponentsStreamHashAidxColumn columnName, Long value) {
            return new TestHashComponentsStreamHashAidxColumnValue(columnName, value);
        }

        private TestHashComponentsStreamHashAidxColumnValue(TestHashComponentsStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public TestHashComponentsStreamHashAidxColumn getColumnName() {
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

        public static Function<TestHashComponentsStreamHashAidxColumnValue, TestHashComponentsStreamHashAidxColumn> getColumnNameFun() {
            return new Function<TestHashComponentsStreamHashAidxColumnValue, TestHashComponentsStreamHashAidxColumn>() {
                @Override
                public TestHashComponentsStreamHashAidxColumn apply(TestHashComponentsStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<TestHashComponentsStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<TestHashComponentsStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamHashAidxColumnValue columnValue) {
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

    public static final class TestHashComponentsStreamHashAidxRowResult implements TypedRowResult {
        private final TestHashComponentsStreamHashAidxRow rowName;
        private final ImmutableSet<TestHashComponentsStreamHashAidxColumnValue> columnValues;

        public static TestHashComponentsStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            TestHashComponentsStreamHashAidxRow rowName = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<TestHashComponentsStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(TestHashComponentsStreamHashAidxColumnValue.of(col, value));
            }
            return new TestHashComponentsStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private TestHashComponentsStreamHashAidxRowResult(TestHashComponentsStreamHashAidxRow rowName, ImmutableSet<TestHashComponentsStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public TestHashComponentsStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<TestHashComponentsStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<TestHashComponentsStreamHashAidxRowResult, TestHashComponentsStreamHashAidxRow> getRowNameFun() {
            return new Function<TestHashComponentsStreamHashAidxRowResult, TestHashComponentsStreamHashAidxRow>() {
                @Override
                public TestHashComponentsStreamHashAidxRow apply(TestHashComponentsStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<TestHashComponentsStreamHashAidxRowResult, ImmutableSet<TestHashComponentsStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<TestHashComponentsStreamHashAidxRowResult, ImmutableSet<TestHashComponentsStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<TestHashComponentsStreamHashAidxColumnValue> apply(TestHashComponentsStreamHashAidxRowResult rowResult) {
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
    public void delete(TestHashComponentsStreamHashAidxRow row, TestHashComponentsStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<TestHashComponentsStreamHashAidxRow> rows) {
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(TestHashComponentsStreamHashAidxRow rowName, Iterable<TestHashComponentsStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(TestHashComponentsStreamHashAidxRow rowName, TestHashComponentsStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<TestHashComponentsStreamHashAidxRow, ? extends TestHashComponentsStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (TestHashComponentsStreamHashAidxTrigger trigger : triggers) {
            trigger.putTestHashComponentsStreamHashAidx(values);
        }
    }

    @Override
    public void touch(Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumn> values) {
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<TestHashComponentsStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(TestHashComponentsStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> get(Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                TestHashComponentsStreamHashAidxRow row = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, TestHashComponentsStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<TestHashComponentsStreamHashAidxColumnValue> getRowColumns(TestHashComponentsStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamHashAidxColumnValue> getRowColumns(TestHashComponentsStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TestHashComponentsStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(TestHashComponentsStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> getRowsMultimap(Iterable<TestHashComponentsStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> getRowsMultimap(Iterable<TestHashComponentsStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<TestHashComponentsStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TestHashComponentsStreamHashAidxRow row = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, TestHashComponentsStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TestHashComponentsStreamHashAidxRow, BatchingVisitable<TestHashComponentsStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<TestHashComponentsStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamHashAidxRow, BatchingVisitable<TestHashComponentsStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamHashAidxRow row = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TestHashComponentsStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return TestHashComponentsStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TestHashComponentsStreamHashAidxRow, TestHashComponentsStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<TestHashComponentsStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TestHashComponentsStreamHashAidxRow row = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(e.getValue());
            TestHashComponentsStreamHashAidxColumnValue colValue = TestHashComponentsStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TestHashComponentsStreamHashAidxRow, Iterator<TestHashComponentsStreamHashAidxColumnValue>> getRowsColumnRangeIterator(Iterable<TestHashComponentsStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamHashAidxRow, Iterator<TestHashComponentsStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamHashAidxRow row = TestHashComponentsStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TestHashComponentsStreamHashAidxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                TestHashComponentsStreamHashAidxColumn col = TestHashComponentsStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = TestHashComponentsStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return TestHashComponentsStreamHashAidxColumnValue.of(col, val);
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

    public BatchingVisitableView<TestHashComponentsStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TestHashComponentsStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TestHashComponentsStreamHashAidxRowResult>() {
            @Override
            public TestHashComponentsStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return TestHashComponentsStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "2Qzy1IodgvsDDYRguLne/Q==";
}
