package com.palantir.atlasdb.table.description.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
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
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedExpiringSet;
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
@SuppressWarnings("all")
public final class GenericTestTable implements
        AtlasDbMutablePersistentTable<GenericTestTable.GenericTestRow,
                                         GenericTestTable.GenericTestNamedColumnValue<?>,
                                         GenericTestTable.GenericTestRowResult>,
        AtlasDbNamedMutableTable<GenericTestTable.GenericTestRow,
                                    GenericTestTable.GenericTestNamedColumnValue<?>,
                                    GenericTestTable.GenericTestRowResult> {
    private final Transaction t;
    private final List<GenericTestTrigger> triggers;
    private final static String rawTableName = "GenericTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(GenericTestNamedColumn.values());

    static GenericTestTable of(Transaction t, Namespace namespace) {
        return new GenericTestTable(t, namespace, ImmutableList.<GenericTestTrigger>of());
    }

    static GenericTestTable of(Transaction t, Namespace namespace, GenericTestTrigger trigger, GenericTestTrigger... triggers) {
        return new GenericTestTable(t, namespace, ImmutableList.<GenericTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static GenericTestTable of(Transaction t, Namespace namespace, List<GenericTestTrigger> triggers) {
        return new GenericTestTable(t, namespace, triggers);
    }

    private GenericTestTable(Transaction t, Namespace namespace, List<GenericTestTrigger> triggers) {
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
     * GenericTestRow {
     *   {@literal String component1};
     * }
     * </pre>
     */
    public static final class GenericTestRow implements Persistable, Comparable<GenericTestRow> {
        private final String component1;

        public static GenericTestRow of(String component1) {
            return new GenericTestRow(component1);
        }

        private GenericTestRow(String component1) {
            this.component1 = component1;
        }

        public String getComponent1() {
            return component1;
        }

        public static Function<GenericTestRow, String> getComponent1Fun() {
            return new Function<GenericTestRow, String>() {
                @Override
                public String apply(GenericTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<String, GenericTestRow> fromComponent1Fun() {
            return new Function<String, GenericTestRow>() {
                @Override
                public GenericTestRow apply(String row) {
                    return GenericTestRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component1Bytes = PtBytes.toBytes(component1);
            return EncodingUtils.add(component1Bytes);
        }

        public static final Hydrator<GenericTestRow> BYTES_HYDRATOR = new Hydrator<GenericTestRow>() {
            @Override
            public GenericTestRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String component1 = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new GenericTestRow(component1);
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
            GenericTestRow other = (GenericTestRow) obj;
            return Objects.equal(component1, other.component1);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(component1);
        }

        @Override
        public int compareTo(GenericTestRow o) {
            return ComparisonChain.start()
                .compare(this.component1, o.component1)
                .result();
        }
    }

    public interface GenericTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column1 implements GenericTestNamedColumnValue<Long> {
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
     *   type: String;
     * }
     * </pre>
     */
    public static final class Column2 implements GenericTestNamedColumnValue<String> {
        private final String value;

        public static Column2 of(String value) {
            return new Column2(value);
        }

        private Column2(String value) {
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
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<Column2> BYTES_HYDRATOR = new Hydrator<Column2>() {
            @Override
            public Column2 hydrateFromBytes(byte[] bytes) {
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

    public interface GenericTestTrigger {
        public void putGenericTest(Multimap<GenericTestRow, ? extends GenericTestNamedColumnValue<?>> newRows);
    }

    public static final class GenericTestRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static GenericTestRowResult of(RowResult<byte[]> row) {
            return new GenericTestRowResult(row);
        }

        private GenericTestRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public GenericTestRow getRowName() {
            return GenericTestRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<GenericTestRowResult, GenericTestRow> getRowNameFun() {
            return new Function<GenericTestRowResult, GenericTestRow>() {
                @Override
                public GenericTestRow apply(GenericTestRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, GenericTestRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, GenericTestRowResult>() {
                @Override
                public GenericTestRowResult apply(RowResult<byte[]> rowResult) {
                    return new GenericTestRowResult(rowResult);
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

        public String getColumn2() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            Column2 value = Column2.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<GenericTestRowResult, Long> getColumn1Fun() {
            return new Function<GenericTestRowResult, Long>() {
                @Override
                public Long apply(GenericTestRowResult rowResult) {
                    return rowResult.getColumn1();
                }
            };
        }

        public static Function<GenericTestRowResult, String> getColumn2Fun() {
            return new Function<GenericTestRowResult, String>() {
                @Override
                public String apply(GenericTestRowResult rowResult) {
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

    public enum GenericTestNamedColumn {
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

        public static Function<GenericTestNamedColumn, byte[]> toShortName() {
            return new Function<GenericTestNamedColumn, byte[]>() {
                @Override
                public byte[] apply(GenericTestNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<GenericTestNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, GenericTestNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(GenericTestNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends GenericTestNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends GenericTestNamedColumnValue<?>>>builder()
                .put("c", Column1.BYTES_HYDRATOR)
                .put("d", Column2.BYTES_HYDRATOR)
                .build();

    public Map<GenericTestRow, Long> getColumn1s(Collection<GenericTestRow> rows) {
        Map<Cell, GenericTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (GenericTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<GenericTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column1.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<GenericTestRow, String> getColumn2s(Collection<GenericTestRow> rows) {
        Map<Cell, GenericTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (GenericTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<GenericTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = Column2.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn1(GenericTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1(Map<GenericTestRow, Long> map) {
        Map<GenericTestRow, GenericTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<GenericTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn1UnlessExists(GenericTestRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1UnlessExists(Map<GenericTestRow, Long> map) {
        Map<GenericTestRow, GenericTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<GenericTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putColumn2(GenericTestRow row, String value) {
        put(ImmutableMultimap.of(row, Column2.of(value)));
    }

    public void putColumn2(Map<GenericTestRow, String> map) {
        Map<GenericTestRow, GenericTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<GenericTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Column2.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn2UnlessExists(GenericTestRow row, String value) {
        putUnlessExists(ImmutableMultimap.of(row, Column2.of(value)));
    }

    public void putColumn2UnlessExists(Map<GenericTestRow, String> map) {
        Map<GenericTestRow, GenericTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<GenericTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Column2.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<GenericTestRow, ? extends GenericTestNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (GenericTestTrigger trigger : triggers) {
            trigger.putGenericTest(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<GenericTestRow, ? extends GenericTestNamedColumnValue<?>> rows) {
        Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<GenericTestRow, ? extends GenericTestNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteColumn1(GenericTestRow row) {
        deleteColumn1(ImmutableSet.of(row));
    }

    public void deleteColumn1(Iterable<GenericTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn2(GenericTestRow row) {
        deleteColumn2(ImmutableSet.of(row));
    }

    public void deleteColumn2(Iterable<GenericTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(GenericTestRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<GenericTestRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 2);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    public Optional<GenericTestRowResult> getRow(GenericTestRow row) {
        return getRow(row, allColumns);
    }

    public Optional<GenericTestRowResult> getRow(GenericTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(GenericTestRowResult.of(rowResult));
        }
    }

    @Override
    public List<GenericTestRowResult> getRows(Iterable<GenericTestRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<GenericTestRowResult> getRows(Iterable<GenericTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<GenericTestRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(GenericTestRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<GenericTestNamedColumnValue<?>> getRowColumns(GenericTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<GenericTestNamedColumnValue<?>> getRowColumns(GenericTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<GenericTestNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> getRowsMultimap(Iterable<GenericTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> getRowsMultimap(Iterable<GenericTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> getRowsMultimapInternal(Iterable<GenericTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<GenericTestRow, GenericTestNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            GenericTestRow row = GenericTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<GenericTestRow, BatchingVisitable<GenericTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<GenericTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<GenericTestRow, BatchingVisitable<GenericTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            GenericTestRow row = GenericTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<GenericTestNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<GenericTestRow, GenericTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<GenericTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            GenericTestRow row = GenericTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            GenericTestNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<GenericTestRowResult> getRange(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            range = range.getBuilder().retainColumns(allColumns).build();
        }

        return BatchingVisitables.transform(t.getRange(tableRef, range), new Function<RowResult<byte[]>, GenericTestRowResult>() {
            @Override
            public GenericTestRowResult apply(RowResult<byte[]> input) {
                return GenericTestRowResult.of(input);
            }
        });
    }

    public IterableView<BatchingVisitable<GenericTestRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, ranges);
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<GenericTestRowResult>>() {
            @Override
            public BatchingVisitable<GenericTestRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, GenericTestRowResult>() {
                    @Override
                    public GenericTestRowResult apply(RowResult<byte[]> row) {
                        return GenericTestRowResult.of(row);
                    }
                });
            }
        });
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(GenericTestRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<GenericTestRow>, RuntimeException>() {
            @Override
            public boolean visit(List<GenericTestRow> rows) {
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
     * {@link AtlasDbDynamicMutableExpiringTable}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutableExpiringTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedExpiringSet}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
     * {@link BatchColumnRangeSelection}
     * {@link BatchingVisitable}
     * {@link BatchingVisitableView}
     * {@link BatchingVisitables}
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
     * {@link Supplier}
     * {@link TableReference}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "W1b6fTw1obAo9RnQkX/Z2A==";
}
