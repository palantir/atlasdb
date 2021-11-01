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
public final class TestHashComponentsStreamMetadataTable implements
        AtlasDbMutablePersistentTable<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow,
                                         TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataNamedColumnValue<?>,
                                         TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRowResult>,
        AtlasDbNamedMutableTable<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow,
                                    TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataNamedColumnValue<?>,
                                    TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRowResult> {
    private final Transaction t;
    private final List<TestHashComponentsStreamMetadataTrigger> triggers;
    private final static String rawTableName = "test_hash_components_stream_metadata";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TestHashComponentsStreamMetadataNamedColumn.values());

    static TestHashComponentsStreamMetadataTable of(Transaction t, Namespace namespace) {
        return new TestHashComponentsStreamMetadataTable(t, namespace, ImmutableList.<TestHashComponentsStreamMetadataTrigger>of());
    }

    static TestHashComponentsStreamMetadataTable of(Transaction t, Namespace namespace, TestHashComponentsStreamMetadataTrigger trigger, TestHashComponentsStreamMetadataTrigger... triggers) {
        return new TestHashComponentsStreamMetadataTable(t, namespace, ImmutableList.<TestHashComponentsStreamMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static TestHashComponentsStreamMetadataTable of(Transaction t, Namespace namespace, List<TestHashComponentsStreamMetadataTrigger> triggers) {
        return new TestHashComponentsStreamMetadataTable(t, namespace, triggers);
    }

    private TestHashComponentsStreamMetadataTable(Transaction t, Namespace namespace, List<TestHashComponentsStreamMetadataTrigger> triggers) {
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
     * TestHashComponentsStreamMetadataRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class TestHashComponentsStreamMetadataRow implements Persistable, Comparable<TestHashComponentsStreamMetadataRow> {
        private final long hashOfRowComponents;
        private final long id;

        public static TestHashComponentsStreamMetadataRow of(long id) {
            long hashOfRowComponents = computeHashFirstComponents(id);
            return new TestHashComponentsStreamMetadataRow(hashOfRowComponents, id);
        }

        private TestHashComponentsStreamMetadataRow(long hashOfRowComponents, long id) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<TestHashComponentsStreamMetadataRow, Long> getIdFun() {
            return new Function<TestHashComponentsStreamMetadataRow, Long>() {
                @Override
                public Long apply(TestHashComponentsStreamMetadataRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, TestHashComponentsStreamMetadataRow> fromIdFun() {
            return new Function<Long, TestHashComponentsStreamMetadataRow>() {
                @Override
                public TestHashComponentsStreamMetadataRow apply(Long row) {
                    return TestHashComponentsStreamMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(hashOfRowComponentsBytes, idBytes);
        }

        public static final Hydrator<TestHashComponentsStreamMetadataRow> BYTES_HYDRATOR = new Hydrator<TestHashComponentsStreamMetadataRow>() {
            @Override
            public TestHashComponentsStreamMetadataRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new TestHashComponentsStreamMetadataRow(hashOfRowComponents, id);
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
            TestHashComponentsStreamMetadataRow other = (TestHashComponentsStreamMetadataRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, id });
        }

        @Override
        public int compareTo(TestHashComponentsStreamMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface TestHashComponentsStreamMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
     *   name: "StreamMetadata"
     *   field {
     *     name: "status"
     *     number: 1
     *     label: LABEL_REQUIRED
     *     type: TYPE_ENUM
     *     type_name: ".com.palantir.atlasdb.protos.generated.Status"
     *   }
     *   field {
     *     name: "length"
     *     number: 2
     *     label: LABEL_REQUIRED
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "hash"
     *     number: 3
     *     label: LABEL_REQUIRED
     *     type: TYPE_BYTES
     *   }
     * }
     * </pre>
     */
    public static final class Metadata implements TestHashComponentsStreamMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> {
        private final com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value;

        public static Metadata of(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
            return new Metadata(value);
        }

        private Metadata(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "metadata";
        }

        @Override
        public String getShortColumnName() {
            return "md";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("md");
        }

        public static final Hydrator<Metadata> BYTES_HYDRATOR = new Hydrator<Metadata>() {
            @Override
            public Metadata hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface TestHashComponentsStreamMetadataTrigger {
        public void putTestHashComponentsStreamMetadata(Multimap<TestHashComponentsStreamMetadataRow, ? extends TestHashComponentsStreamMetadataNamedColumnValue<?>> newRows);
    }

    public static final class TestHashComponentsStreamMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TestHashComponentsStreamMetadataRowResult of(RowResult<byte[]> row) {
            return new TestHashComponentsStreamMetadataRowResult(row);
        }

        private TestHashComponentsStreamMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TestHashComponentsStreamMetadataRow getRowName() {
            return TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TestHashComponentsStreamMetadataRowResult, TestHashComponentsStreamMetadataRow> getRowNameFun() {
            return new Function<TestHashComponentsStreamMetadataRowResult, TestHashComponentsStreamMetadataRow>() {
                @Override
                public TestHashComponentsStreamMetadataRow apply(TestHashComponentsStreamMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TestHashComponentsStreamMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TestHashComponentsStreamMetadataRowResult>() {
                @Override
                public TestHashComponentsStreamMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new TestHashComponentsStreamMetadataRowResult(rowResult);
                }
            };
        }

        public boolean hasMetadata() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("md"));
        }

        public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata getMetadata() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("md"));
            if (bytes == null) {
                return null;
            }
            Metadata value = Metadata.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TestHashComponentsStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadataFun() {
            return new Function<TestHashComponentsStreamMetadataRowResult, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata>() {
                @Override
                public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata apply(TestHashComponentsStreamMetadataRowResult rowResult) {
                    return rowResult.getMetadata();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Metadata", getMetadata())
                .toString();
        }
    }

    public enum TestHashComponentsStreamMetadataNamedColumn {
        METADATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("md");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TestHashComponentsStreamMetadataNamedColumn, byte[]> toShortName() {
            return new Function<TestHashComponentsStreamMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TestHashComponentsStreamMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TestHashComponentsStreamMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TestHashComponentsStreamMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TestHashComponentsStreamMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TestHashComponentsStreamMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TestHashComponentsStreamMetadataNamedColumnValue<?>>>builder()
                .put("md", Metadata.BYTES_HYDRATOR)
                .build();

    public Map<TestHashComponentsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> getMetadatas(Collection<TestHashComponentsStreamMetadataRow> rows) {
        Map<Cell, TestHashComponentsStreamMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TestHashComponentsStreamMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("md")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TestHashComponentsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putMetadata(TestHashComponentsStreamMetadataRow row, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata value) {
        put(ImmutableMultimap.of(row, Metadata.of(value)));
    }

    public void putMetadata(Map<TestHashComponentsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> map) {
        Map<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TestHashComponentsStreamMetadataRow, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TestHashComponentsStreamMetadataRow, ? extends TestHashComponentsStreamMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TestHashComponentsStreamMetadataTrigger trigger : triggers) {
            trigger.putTestHashComponentsStreamMetadata(rows);
        }
    }

    public void deleteMetadata(TestHashComponentsStreamMetadataRow row) {
        deleteMetadata(ImmutableSet.of(row));
    }

    public void deleteMetadata(Iterable<TestHashComponentsStreamMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("md");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TestHashComponentsStreamMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TestHashComponentsStreamMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("md")));
        t.delete(tableRef, cells);
    }

    public Optional<TestHashComponentsStreamMetadataRowResult> getRow(TestHashComponentsStreamMetadataRow row) {
        return getRow(row, allColumns);
    }

    public Optional<TestHashComponentsStreamMetadataRowResult> getRow(TestHashComponentsStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(TestHashComponentsStreamMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<TestHashComponentsStreamMetadataRowResult> getRows(Iterable<TestHashComponentsStreamMetadataRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamMetadataRowResult> getRows(Iterable<TestHashComponentsStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TestHashComponentsStreamMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TestHashComponentsStreamMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowColumns(TestHashComponentsStreamMetadataRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowColumns(TestHashComponentsStreamMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TestHashComponentsStreamMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<TestHashComponentsStreamMetadataRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<TestHashComponentsStreamMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TestHashComponentsStreamMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TestHashComponentsStreamMetadataRow row = TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TestHashComponentsStreamMetadataRow, BatchingVisitable<TestHashComponentsStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<TestHashComponentsStreamMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamMetadataRow, BatchingVisitable<TestHashComponentsStreamMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamMetadataRow row = TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TestHashComponentsStreamMetadataNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TestHashComponentsStreamMetadataRow, TestHashComponentsStreamMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<TestHashComponentsStreamMetadataRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TestHashComponentsStreamMetadataRow row = TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TestHashComponentsStreamMetadataNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TestHashComponentsStreamMetadataRow, Iterator<TestHashComponentsStreamMetadataNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<TestHashComponentsStreamMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TestHashComponentsStreamMetadataRow, Iterator<TestHashComponentsStreamMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TestHashComponentsStreamMetadataRow row = TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TestHashComponentsStreamMetadataNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<TestHashComponentsStreamMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TestHashComponentsStreamMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TestHashComponentsStreamMetadataRowResult>() {
            @Override
            public TestHashComponentsStreamMetadataRowResult apply(RowResult<byte[]> input) {
                return TestHashComponentsStreamMetadataRowResult.of(input);
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
    static String __CLASS_HASH = "UicfW27TGG9bWrV+QAqsfg==";
}
