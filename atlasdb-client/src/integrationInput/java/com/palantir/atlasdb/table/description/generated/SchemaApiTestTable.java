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
public final class SchemaApiTestTable implements
        AtlasDbMutablePersistentTable<SchemaApiTestTable.SchemaApiTestRow,
                                         SchemaApiTestTable.SchemaApiTestNamedColumnValue<?>,
                                         SchemaApiTestTable.SchemaApiTestRowResult>,
        AtlasDbNamedMutableTable<SchemaApiTestTable.SchemaApiTestRow,
                                    SchemaApiTestTable.SchemaApiTestNamedColumnValue<?>,
                                    SchemaApiTestTable.SchemaApiTestRowResult> {
    private final Transaction t;
    private final List<SchemaApiTestTrigger> triggers;
    private final static String rawTableName = "SchemaApiTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SchemaApiTestNamedColumn.values());

    static SchemaApiTestTable of(Transaction t, Namespace namespace) {
        return new SchemaApiTestTable(t, namespace, ImmutableList.<SchemaApiTestTrigger>of());
    }

    static SchemaApiTestTable of(Transaction t, Namespace namespace, SchemaApiTestTrigger trigger, SchemaApiTestTrigger... triggers) {
        return new SchemaApiTestTable(t, namespace, ImmutableList.<SchemaApiTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static SchemaApiTestTable of(Transaction t, Namespace namespace, List<SchemaApiTestTrigger> triggers) {
        return new SchemaApiTestTable(t, namespace, triggers);
    }

    private SchemaApiTestTable(Transaction t, Namespace namespace, List<SchemaApiTestTrigger> triggers) {
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
     * SchemaApiTestRow {
     *   {@literal String component1};
     * }
     * </pre>
     */
    public static final class SchemaApiTestRow implements Persistable, Comparable<SchemaApiTestRow> {
        private final String component1;

        public static SchemaApiTestRow of(String component1) {
            return new SchemaApiTestRow(component1);
        }

        private SchemaApiTestRow(String component1) {
            this.component1 = component1;
        }

        public String getComponent1() {
            return component1;
        }

        public static Function<SchemaApiTestRow, String> getComponent1Fun() {
            return new Function<SchemaApiTestRow, String>() {
                @Override
                public String apply(SchemaApiTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<String, SchemaApiTestRow> fromComponent1Fun() {
            return new Function<String, SchemaApiTestRow>() {
                @Override
                public SchemaApiTestRow apply(String row) {
                    return SchemaApiTestRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component1Bytes = PtBytes.toBytes(component1);
            return EncodingUtils.add(component1Bytes);
        }

        public static final Hydrator<SchemaApiTestRow> BYTES_HYDRATOR = new Hydrator<SchemaApiTestRow>() {
            @Override
            public SchemaApiTestRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String component1 = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SchemaApiTestRow(component1);
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
            SchemaApiTestRow other = (SchemaApiTestRow) obj;
            return Objects.equals(component1, other.component1);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(component1);
        }

        @Override
        public int compareTo(SchemaApiTestRow o) {
            return ComparisonChain.start()
                .compare(this.component1, o.component1)
                .result();
        }
    }

    public interface SchemaApiTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column1 implements SchemaApiTestNamedColumnValue<Long> {
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

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.table.description.test.StringValue;
     * }
     * </pre>
     */
    public static final class Column2 implements SchemaApiTestNamedColumnValue<com.palantir.atlasdb.table.description.test.StringValue> {
        private final com.palantir.atlasdb.table.description.test.StringValue value;

        public static Column2 of(com.palantir.atlasdb.table.description.test.StringValue value) {
            return new Column2(value);
        }

        private Column2(com.palantir.atlasdb.table.description.test.StringValue value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column2";
        }

        @Override
        public String getShortColumnName() {
            return "d";
        }

        @Override
        public com.palantir.atlasdb.table.description.test.StringValue getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = com.palantir.atlasdb.compress.CompressionUtils.compress(new com.palantir.atlasdb.table.description.test.StringValuePersister().persistToBytes(value), com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<Column2> BYTES_HYDRATOR = new Hydrator<Column2>() {
            @Override
            public Column2 hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(REUSABLE_PERSISTER.hydrateFromBytes(com.palantir.atlasdb.compress.CompressionUtils.decompress(bytes, com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE)));
            }
            private final com.palantir.atlasdb.table.description.test.StringValuePersister REUSABLE_PERSISTER = new com.palantir.atlasdb.table.description.test.StringValuePersister();
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface SchemaApiTestTrigger {
        public void putSchemaApiTest(Multimap<SchemaApiTestRow, ? extends SchemaApiTestNamedColumnValue<?>> newRows);
    }

    public static final class SchemaApiTestRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SchemaApiTestRowResult of(RowResult<byte[]> row) {
            return new SchemaApiTestRowResult(row);
        }

        private SchemaApiTestRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SchemaApiTestRow getRowName() {
            return SchemaApiTestRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SchemaApiTestRowResult, SchemaApiTestRow> getRowNameFun() {
            return new Function<SchemaApiTestRowResult, SchemaApiTestRow>() {
                @Override
                public SchemaApiTestRow apply(SchemaApiTestRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SchemaApiTestRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SchemaApiTestRowResult>() {
                @Override
                public SchemaApiTestRowResult apply(RowResult<byte[]> rowResult) {
                    return new SchemaApiTestRowResult(rowResult);
                }
            };
        }

        public boolean hasColumn1() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public boolean hasColumn2() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public Long getColumn1() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            Column1 value = Column1.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public com.palantir.atlasdb.table.description.test.StringValue getColumn2() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            Column2 value = Column2.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SchemaApiTestRowResult, Long> getColumn1Fun() {
            return new Function<SchemaApiTestRowResult, Long>() {
                @Override
                public Long apply(SchemaApiTestRowResult rowResult) {
                    return rowResult.getColumn1();
                }
            };
        }

        public static Function<SchemaApiTestRowResult, com.palantir.atlasdb.table.description.test.StringValue> getColumn2Fun() {
            return new Function<SchemaApiTestRowResult, com.palantir.atlasdb.table.description.test.StringValue>() {
                @Override
                public com.palantir.atlasdb.table.description.test.StringValue apply(SchemaApiTestRowResult rowResult) {
                    return rowResult.getColumn2();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Column1", getColumn1())
                .add("Column2", getColumn2())
                .toString();
        }
    }

    public enum SchemaApiTestNamedColumn {
        COLUMN1 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        },
        COLUMN2 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SchemaApiTestNamedColumn, byte[]> toShortName() {
            return new Function<SchemaApiTestNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SchemaApiTestNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SchemaApiTestNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SchemaApiTestNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SchemaApiTestNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SchemaApiTestNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SchemaApiTestNamedColumnValue<?>>>builder()
                .put("c", Column1.BYTES_HYDRATOR)
                .put("d", Column2.BYTES_HYDRATOR)
                .build();

    public Map<SchemaApiTestRow, Long> getColumn1s(Collection<SchemaApiTestRow> rows) {
        Map<Cell, SchemaApiTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SchemaApiTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SchemaApiTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column1.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SchemaApiTestRow, com.palantir.atlasdb.table.description.test.StringValue> getColumn2s(Collection<SchemaApiTestRow> rows) {
        Map<Cell, SchemaApiTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SchemaApiTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SchemaApiTestRow, com.palantir.atlasdb.table.description.test.StringValue> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.table.description.test.StringValue val = Column2.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn1(SchemaApiTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1(Map<SchemaApiTestRow, Long> map) {
        Map<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SchemaApiTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn2(SchemaApiTestRow row, com.palantir.atlasdb.table.description.test.StringValue value) {
        put(ImmutableMultimap.of(row, Column2.of(value)));
    }

    public void putColumn2(Map<SchemaApiTestRow, com.palantir.atlasdb.table.description.test.StringValue> map) {
        Map<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SchemaApiTestRow, com.palantir.atlasdb.table.description.test.StringValue> e : map.entrySet()) {
            toPut.put(e.getKey(), Column2.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SchemaApiTestRow, ? extends SchemaApiTestNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SchemaApiTestTrigger trigger : triggers) {
            trigger.putSchemaApiTest(rows);
        }
    }

    public void deleteColumn1(SchemaApiTestRow row) {
        deleteColumn1(ImmutableSet.of(row));
    }

    public void deleteColumn1(Iterable<SchemaApiTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn2(SchemaApiTestRow row) {
        deleteColumn2(ImmutableSet.of(row));
    }

    public void deleteColumn2(Iterable<SchemaApiTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SchemaApiTestRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SchemaApiTestRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 2);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    public Optional<SchemaApiTestRowResult> getRow(SchemaApiTestRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SchemaApiTestRowResult> getRow(SchemaApiTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SchemaApiTestRowResult.of(rowResult));
        }
    }

    @Override
    public List<SchemaApiTestRowResult> getRows(Iterable<SchemaApiTestRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SchemaApiTestRowResult> getRows(Iterable<SchemaApiTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SchemaApiTestRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SchemaApiTestRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SchemaApiTestNamedColumnValue<?>> getRowColumns(SchemaApiTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SchemaApiTestNamedColumnValue<?>> getRowColumns(SchemaApiTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SchemaApiTestNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> getRowsMultimap(Iterable<SchemaApiTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> getRowsMultimap(Iterable<SchemaApiTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SchemaApiTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SchemaApiTestRow row = SchemaApiTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SchemaApiTestRow, BatchingVisitable<SchemaApiTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<SchemaApiTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SchemaApiTestRow, BatchingVisitable<SchemaApiTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SchemaApiTestRow row = SchemaApiTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SchemaApiTestNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SchemaApiTestRow, SchemaApiTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<SchemaApiTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SchemaApiTestRow row = SchemaApiTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SchemaApiTestNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SchemaApiTestRow, Iterator<SchemaApiTestNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SchemaApiTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SchemaApiTestRow, Iterator<SchemaApiTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SchemaApiTestRow row = SchemaApiTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SchemaApiTestNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<SchemaApiTestRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, SchemaApiTestRowResult>() {
            @Override
            public SchemaApiTestRowResult apply(RowResult<byte[]> input) {
                return SchemaApiTestRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<SchemaApiTestRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<SchemaApiTestRowResult>>() {
            @Override
            public BatchingVisitable<SchemaApiTestRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, SchemaApiTestRowResult>() {
                    @Override
                    public SchemaApiTestRowResult apply(RowResult<byte[]> row) {
                        return SchemaApiTestRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<SchemaApiTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SchemaApiTestRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<SchemaApiTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SchemaApiTestRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<SchemaApiTestRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, SchemaApiTestRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(SchemaApiTestRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<SchemaApiTestRow>, RuntimeException>() {
            @Override
            public boolean visit(List<SchemaApiTestRow> rows) {
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
    static String __CLASS_HASH = "0gf/hBo1iBOZCh4wzyO6Mg==";
}
