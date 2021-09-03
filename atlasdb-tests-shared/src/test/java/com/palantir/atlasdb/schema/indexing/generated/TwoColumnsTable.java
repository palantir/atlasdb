package com.palantir.atlasdb.schema.indexing.generated;

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
public final class TwoColumnsTable implements
        AtlasDbMutablePersistentTable<TwoColumnsTable.TwoColumnsRow,
                                         TwoColumnsTable.TwoColumnsNamedColumnValue<?>,
                                         TwoColumnsTable.TwoColumnsRowResult>,
        AtlasDbNamedMutableTable<TwoColumnsTable.TwoColumnsRow,
                                    TwoColumnsTable.TwoColumnsNamedColumnValue<?>,
                                    TwoColumnsTable.TwoColumnsRowResult> {
    private final Transaction t;
    private final List<TwoColumnsTrigger> triggers;
    private final static String rawTableName = "two_columns";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TwoColumnsNamedColumn.values());

    static TwoColumnsTable of(Transaction t, Namespace namespace) {
        return new TwoColumnsTable(t, namespace, ImmutableList.<TwoColumnsTrigger>of());
    }

    static TwoColumnsTable of(Transaction t, Namespace namespace, TwoColumnsTrigger trigger, TwoColumnsTrigger... triggers) {
        return new TwoColumnsTable(t, namespace, ImmutableList.<TwoColumnsTrigger>builder().add(trigger).add(triggers).build());
    }

    static TwoColumnsTable of(Transaction t, Namespace namespace, List<TwoColumnsTrigger> triggers) {
        return new TwoColumnsTable(t, namespace, triggers);
    }

    private TwoColumnsTable(Transaction t, Namespace namespace, List<TwoColumnsTrigger> triggers) {
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
     * TwoColumnsRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class TwoColumnsRow implements Persistable, Comparable<TwoColumnsRow> {
        private final long id;

        public static TwoColumnsRow of(long id) {
            return new TwoColumnsRow(id);
        }

        private TwoColumnsRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<TwoColumnsRow, Long> getIdFun() {
            return new Function<TwoColumnsRow, Long>() {
                @Override
                public Long apply(TwoColumnsRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, TwoColumnsRow> fromIdFun() {
            return new Function<Long, TwoColumnsRow>() {
                @Override
                public TwoColumnsRow apply(Long row) {
                    return TwoColumnsRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<TwoColumnsRow> BYTES_HYDRATOR = new Hydrator<TwoColumnsRow>() {
            @Override
            public TwoColumnsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new TwoColumnsRow(id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
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
            TwoColumnsRow other = (TwoColumnsRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(TwoColumnsRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface TwoColumnsNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Bar implements TwoColumnsNamedColumnValue<Long> {
        private final Long value;

        public static Bar of(Long value) {
            return new Bar(value);
        }

        private Bar(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "bar";
        }

        @Override
        public String getShortColumnName() {
            return "b";
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
            return PtBytes.toCachedBytes("b");
        }

        public static final Hydrator<Bar> BYTES_HYDRATOR = new Hydrator<Bar>() {
            @Override
            public Bar hydrateFromBytes(byte[] bytes) {
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

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Foo implements TwoColumnsNamedColumnValue<Long> {
        private final Long value;

        public static Foo of(Long value) {
            return new Foo(value);
        }

        private Foo(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "foo";
        }

        @Override
        public String getShortColumnName() {
            return "f";
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
            return PtBytes.toCachedBytes("f");
        }

        public static final Hydrator<Foo> BYTES_HYDRATOR = new Hydrator<Foo>() {
            @Override
            public Foo hydrateFromBytes(byte[] bytes) {
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

    public interface TwoColumnsTrigger {
        public void putTwoColumns(Multimap<TwoColumnsRow, ? extends TwoColumnsNamedColumnValue<?>> newRows);
    }

    public static final class TwoColumnsRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TwoColumnsRowResult of(RowResult<byte[]> row) {
            return new TwoColumnsRowResult(row);
        }

        private TwoColumnsRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TwoColumnsRow getRowName() {
            return TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TwoColumnsRowResult, TwoColumnsRow> getRowNameFun() {
            return new Function<TwoColumnsRowResult, TwoColumnsRow>() {
                @Override
                public TwoColumnsRow apply(TwoColumnsRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TwoColumnsRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TwoColumnsRowResult>() {
                @Override
                public TwoColumnsRowResult apply(RowResult<byte[]> rowResult) {
                    return new TwoColumnsRowResult(rowResult);
                }
            };
        }

        public boolean hasBar() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("b"));
        }

        public boolean hasFoo() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("f"));
        }

        public Long getBar() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("b"));
            if (bytes == null) {
                return null;
            }
            Bar value = Bar.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getFoo() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("f"));
            if (bytes == null) {
                return null;
            }
            Foo value = Foo.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TwoColumnsRowResult, Long> getBarFun() {
            return new Function<TwoColumnsRowResult, Long>() {
                @Override
                public Long apply(TwoColumnsRowResult rowResult) {
                    return rowResult.getBar();
                }
            };
        }

        public static Function<TwoColumnsRowResult, Long> getFooFun() {
            return new Function<TwoColumnsRowResult, Long>() {
                @Override
                public Long apply(TwoColumnsRowResult rowResult) {
                    return rowResult.getFoo();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Bar", getBar())
                .add("Foo", getFoo())
                .toString();
        }
    }

    public enum TwoColumnsNamedColumn {
        BAR {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("b");
            }
        },
        FOO {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("f");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TwoColumnsNamedColumn, byte[]> toShortName() {
            return new Function<TwoColumnsNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TwoColumnsNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TwoColumnsNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TwoColumnsNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TwoColumnsNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TwoColumnsNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TwoColumnsNamedColumnValue<?>>>builder()
                .put("f", Foo.BYTES_HYDRATOR)
                .put("b", Bar.BYTES_HYDRATOR)
                .build();

    public Map<TwoColumnsRow, Long> getFoos(Collection<TwoColumnsRow> rows) {
        Map<Cell, TwoColumnsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TwoColumnsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("f")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TwoColumnsRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Foo.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<TwoColumnsRow, Long> getBars(Collection<TwoColumnsRow> rows) {
        Map<Cell, TwoColumnsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TwoColumnsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("b")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TwoColumnsRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Bar.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putFoo(TwoColumnsRow row, Long value) {
        put(ImmutableMultimap.of(row, Foo.of(value)));
    }

    public void putFoo(Map<TwoColumnsRow, Long> map) {
        Map<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TwoColumnsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Foo.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putBar(TwoColumnsRow row, Long value) {
        put(ImmutableMultimap.of(row, Bar.of(value)));
    }

    public void putBar(Map<TwoColumnsRow, Long> map) {
        Map<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TwoColumnsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Bar.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TwoColumnsRow, ? extends TwoColumnsNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> affectedCells = getAffectedCells(rows);
        deleteFooToIdCondIdx(affectedCells);
        deleteFooToIdIdx(affectedCells);
        for (Entry<TwoColumnsRow, ? extends TwoColumnsNamedColumnValue<?>> e : rows.entries()) {
            if (e.getValue() instanceof Foo)
            {
                Foo col = (Foo) e.getValue();
                if (col.getValue() > 1)
                {
                    TwoColumnsRow row = e.getKey();
                    FooToIdCondIdxTable table = FooToIdCondIdxTable.of(this);
                    long foo = col.getValue();
                    long id = row.getId();
                    FooToIdCondIdxTable.FooToIdCondIdxRow indexRow = FooToIdCondIdxTable.FooToIdCondIdxRow.of(foo);
                    FooToIdCondIdxTable.FooToIdCondIdxColumn indexCol = FooToIdCondIdxTable.FooToIdCondIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    FooToIdCondIdxTable.FooToIdCondIdxColumnValue indexColVal = FooToIdCondIdxTable.FooToIdCondIdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
            if (e.getValue() instanceof Foo)
            {
                Foo col = (Foo) e.getValue();
                {
                    TwoColumnsRow row = e.getKey();
                    FooToIdIdxTable table = FooToIdIdxTable.of(this);
                    long foo = col.getValue();
                    long id = row.getId();
                    FooToIdIdxTable.FooToIdIdxRow indexRow = FooToIdIdxTable.FooToIdIdxRow.of(foo);
                    FooToIdIdxTable.FooToIdIdxColumn indexCol = FooToIdIdxTable.FooToIdIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    FooToIdIdxTable.FooToIdIdxColumnValue indexColVal = FooToIdIdxTable.FooToIdIdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
        }
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TwoColumnsTrigger trigger : triggers) {
            trigger.putTwoColumns(rows);
        }
    }

    public void deleteFoo(TwoColumnsRow row) {
        deleteFoo(ImmutableSet.of(row));
    }

    public void deleteFoo(Iterable<TwoColumnsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("f");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        Map<Cell, byte[]> results = t.get(tableRef, cells);
        deleteFooToIdCondIdxRaw(results);
        deleteFooToIdIdxRaw(results);
        t.delete(tableRef, cells);
    }

    private void deleteFooToIdCondIdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Foo col = (Foo) shortNameToHydrator.get("f").hydrateFromBytes(result.getValue());
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long foo = col.getValue();
            long id = row.getId();
            FooToIdCondIdxTable.FooToIdCondIdxRow indexRow = FooToIdCondIdxTable.FooToIdCondIdxRow.of(foo);
            FooToIdCondIdxTable.FooToIdCondIdxColumn indexCol = FooToIdCondIdxTable.FooToIdCondIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.foo_to_id_cond_idx"), indexCells);
    }

    private void deleteFooToIdIdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Foo col = (Foo) shortNameToHydrator.get("f").hydrateFromBytes(result.getValue());
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long foo = col.getValue();
            long id = row.getId();
            FooToIdIdxTable.FooToIdIdxRow indexRow = FooToIdIdxTable.FooToIdIdxRow.of(foo);
            FooToIdIdxTable.FooToIdIdxColumn indexCol = FooToIdIdxTable.FooToIdIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.foo_to_id_idx"), indexCells);
    }

    public void deleteBar(TwoColumnsRow row) {
        deleteBar(ImmutableSet.of(row));
    }

    public void deleteBar(Iterable<TwoColumnsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("b");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TwoColumnsRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TwoColumnsRow> rows) {
        Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> result = getRowsMultimap(rows);
        deleteFooToIdCondIdx(result);
        deleteFooToIdIdx(result);
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 2);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("b")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("f")));
        t.delete(tableRef, cells);
    }

    public Optional<TwoColumnsRowResult> getRow(TwoColumnsRow row) {
        return getRow(row, allColumns);
    }

    public Optional<TwoColumnsRowResult> getRow(TwoColumnsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(TwoColumnsRowResult.of(rowResult));
        }
    }

    @Override
    public List<TwoColumnsRowResult> getRows(Iterable<TwoColumnsRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TwoColumnsRowResult> getRows(Iterable<TwoColumnsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TwoColumnsRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TwoColumnsRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TwoColumnsNamedColumnValue<?>> getRowColumns(TwoColumnsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TwoColumnsNamedColumnValue<?>> getRowColumns(TwoColumnsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TwoColumnsNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> getRowsMultimap(Iterable<TwoColumnsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> getRowsMultimap(Iterable<TwoColumnsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TwoColumnsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TwoColumnsRow, BatchingVisitable<TwoColumnsNamedColumnValue<?>>> getRowsColumnRange(Iterable<TwoColumnsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TwoColumnsRow, BatchingVisitable<TwoColumnsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TwoColumnsNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TwoColumnsRow, TwoColumnsNamedColumnValue<?>>> getRowsColumnRange(Iterable<TwoColumnsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TwoColumnsNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TwoColumnsRow, Iterator<TwoColumnsNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<TwoColumnsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TwoColumnsRow, Iterator<TwoColumnsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TwoColumnsRow row = TwoColumnsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TwoColumnsNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> getAffectedCells(Multimap<TwoColumnsRow, ? extends TwoColumnsNamedColumnValue<?>> rows) {
        Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> oldData = getRowsMultimap(rows.keySet());
        Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> cellsAffected = ArrayListMultimap.create();
        for (TwoColumnsRow row : oldData.keySet()) {
            Set<String> columns = new HashSet<String>();
            for (TwoColumnsNamedColumnValue<?> v : rows.get(row)) {
                columns.add(v.getColumnName());
            }
            for (TwoColumnsNamedColumnValue<?> v : oldData.get(row)) {
                if (columns.contains(v.getColumnName())) {
                    cellsAffected.put(row, v);
                }
            }
        }
        return cellsAffected;
    }

    private void deleteFooToIdCondIdx(Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Foo) {
                Foo col = (Foo) e.getValue();
                if (col.getValue() > 1) {
                    TwoColumnsRow row = e.getKey();
                    long foo = col.getValue();
                    long id = row.getId();
                    FooToIdCondIdxTable.FooToIdCondIdxRow indexRow = FooToIdCondIdxTable.FooToIdCondIdxRow.of(foo);
                    FooToIdCondIdxTable.FooToIdCondIdxColumn indexCol = FooToIdCondIdxTable.FooToIdCondIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.foo_to_id_cond_idx"), indexCells.build());
    }

    private void deleteFooToIdIdx(Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Foo) {
                Foo col = (Foo) e.getValue();{
                    TwoColumnsRow row = e.getKey();
                    long foo = col.getValue();
                    long id = row.getId();
                    FooToIdIdxTable.FooToIdIdxRow indexRow = FooToIdIdxTable.FooToIdIdxRow.of(foo);
                    FooToIdIdxTable.FooToIdIdxColumn indexCol = FooToIdIdxTable.FooToIdIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.foo_to_id_idx"), indexCells.build());
    }

    private ColumnSelection optimizeColumnSelection(ColumnSelection columns) {
        if (columns.allColumnsSelected()) {
            return allColumns;
        }
        return columns;
    }

    public BatchingVisitableView<TwoColumnsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TwoColumnsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TwoColumnsRowResult>() {
            @Override
            public TwoColumnsRowResult apply(RowResult<byte[]> input) {
                return TwoColumnsRowResult.of(input);
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

    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class FooToIdCondIdxTable implements
            AtlasDbDynamicMutablePersistentTable<FooToIdCondIdxTable.FooToIdCondIdxRow,
                                                    FooToIdCondIdxTable.FooToIdCondIdxColumn,
                                                    FooToIdCondIdxTable.FooToIdCondIdxColumnValue,
                                                    FooToIdCondIdxTable.FooToIdCondIdxRowResult> {
        private final Transaction t;
        private final List<FooToIdCondIdxTrigger> triggers;
        private final static String rawTableName = "foo_to_id_cond_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static FooToIdCondIdxTable of(TwoColumnsTable table) {
            return new FooToIdCondIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<FooToIdCondIdxTrigger>of());
        }

        public static FooToIdCondIdxTable of(TwoColumnsTable table, FooToIdCondIdxTrigger trigger, FooToIdCondIdxTrigger... triggers) {
            return new FooToIdCondIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<FooToIdCondIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static FooToIdCondIdxTable of(TwoColumnsTable table, List<FooToIdCondIdxTrigger> triggers) {
            return new FooToIdCondIdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private FooToIdCondIdxTable(Transaction t, Namespace namespace, List<FooToIdCondIdxTrigger> triggers) {
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
         * FooToIdCondIdxRow {
         *   {@literal Long foo};
         * }
         * </pre>
         */
        public static final class FooToIdCondIdxRow implements Persistable, Comparable<FooToIdCondIdxRow> {
            private final long foo;

            public static FooToIdCondIdxRow of(long foo) {
                return new FooToIdCondIdxRow(foo);
            }

            private FooToIdCondIdxRow(long foo) {
                this.foo = foo;
            }

            public long getFoo() {
                return foo;
            }

            public static Function<FooToIdCondIdxRow, Long> getFooFun() {
                return new Function<FooToIdCondIdxRow, Long>() {
                    @Override
                    public Long apply(FooToIdCondIdxRow row) {
                        return row.foo;
                    }
                };
            }

            public static Function<Long, FooToIdCondIdxRow> fromFooFun() {
                return new Function<Long, FooToIdCondIdxRow>() {
                    @Override
                    public FooToIdCondIdxRow apply(Long row) {
                        return FooToIdCondIdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] fooBytes = PtBytes.toBytes(Long.MIN_VALUE ^ foo);
                return EncodingUtils.add(fooBytes);
            }

            public static final Hydrator<FooToIdCondIdxRow> BYTES_HYDRATOR = new Hydrator<FooToIdCondIdxRow>() {
                @Override
                public FooToIdCondIdxRow hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    Long foo = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    return new FooToIdCondIdxRow(foo);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("foo", foo)
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
                FooToIdCondIdxRow other = (FooToIdCondIdxRow) obj;
                return Objects.equals(foo, other.foo);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(foo);
            }

            @Override
            public int compareTo(FooToIdCondIdxRow o) {
                return ComparisonChain.start()
                    .compare(this.foo, o.foo)
                    .result();
            }
        }

        /**
         * <pre>
         * FooToIdCondIdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal Long id};
         * }
         * </pre>
         */
        public static final class FooToIdCondIdxColumn implements Persistable, Comparable<FooToIdCondIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final long id;

            public static FooToIdCondIdxColumn of(byte[] rowName, byte[] columnName, long id) {
                return new FooToIdCondIdxColumn(rowName, columnName, id);
            }

            private FooToIdCondIdxColumn(byte[] rowName, byte[] columnName, long id) {
                this.rowName = rowName;
                this.columnName = columnName;
                this.id = id;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public long getId() {
                return id;
            }

            public static Function<FooToIdCondIdxColumn, byte[]> getRowNameFun() {
                return new Function<FooToIdCondIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(FooToIdCondIdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<FooToIdCondIdxColumn, byte[]> getColumnNameFun() {
                return new Function<FooToIdCondIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(FooToIdCondIdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<FooToIdCondIdxColumn, Long> getIdFun() {
                return new Function<FooToIdCondIdxColumn, Long>() {
                    @Override
                    public Long apply(FooToIdCondIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<FooToIdCondIdxColumn> BYTES_HYDRATOR = new Hydrator<FooToIdCondIdxColumn>() {
                @Override
                public FooToIdCondIdxColumn hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(columnName);
                    Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    return new FooToIdCondIdxColumn(rowName, columnName, id);
                }
            };

            public static BatchColumnRangeSelection createPrefixRangeUnsorted(byte[] rowName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes), batchSize);
            }

            public static Prefix prefixUnsorted(byte[] rowName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return new Prefix(EncodingUtils.add(rowNameBytes));
            }

            public static BatchColumnRangeSelection createPrefixRange(byte[] rowName, byte[] columnName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes, columnNameBytes), batchSize);
            }

            public static Prefix prefix(byte[] rowName, byte[] columnName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return new Prefix(EncodingUtils.add(rowNameBytes, columnNameBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                FooToIdCondIdxColumn other = (FooToIdCondIdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(FooToIdCondIdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface FooToIdCondIdxTrigger {
            public void putFooToIdCondIdx(Multimap<FooToIdCondIdxRow, ? extends FooToIdCondIdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal Long id};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class FooToIdCondIdxColumnValue implements ColumnValue<Long> {
            private final FooToIdCondIdxColumn columnName;
            private final Long value;

            public static FooToIdCondIdxColumnValue of(FooToIdCondIdxColumn columnName, Long value) {
                return new FooToIdCondIdxColumnValue(columnName, value);
            }

            private FooToIdCondIdxColumnValue(FooToIdCondIdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public FooToIdCondIdxColumn getColumnName() {
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

            public static Function<FooToIdCondIdxColumnValue, FooToIdCondIdxColumn> getColumnNameFun() {
                return new Function<FooToIdCondIdxColumnValue, FooToIdCondIdxColumn>() {
                    @Override
                    public FooToIdCondIdxColumn apply(FooToIdCondIdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<FooToIdCondIdxColumnValue, Long> getValueFun() {
                return new Function<FooToIdCondIdxColumnValue, Long>() {
                    @Override
                    public Long apply(FooToIdCondIdxColumnValue columnValue) {
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

        public static final class FooToIdCondIdxRowResult implements TypedRowResult {
            private final FooToIdCondIdxRow rowName;
            private final ImmutableSet<FooToIdCondIdxColumnValue> columnValues;

            public static FooToIdCondIdxRowResult of(RowResult<byte[]> rowResult) {
                FooToIdCondIdxRow rowName = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<FooToIdCondIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = FooToIdCondIdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(FooToIdCondIdxColumnValue.of(col, value));
                }
                return new FooToIdCondIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private FooToIdCondIdxRowResult(FooToIdCondIdxRow rowName, ImmutableSet<FooToIdCondIdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public FooToIdCondIdxRow getRowName() {
                return rowName;
            }

            public Set<FooToIdCondIdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<FooToIdCondIdxRowResult, FooToIdCondIdxRow> getRowNameFun() {
                return new Function<FooToIdCondIdxRowResult, FooToIdCondIdxRow>() {
                    @Override
                    public FooToIdCondIdxRow apply(FooToIdCondIdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<FooToIdCondIdxRowResult, ImmutableSet<FooToIdCondIdxColumnValue>> getColumnValuesFun() {
                return new Function<FooToIdCondIdxRowResult, ImmutableSet<FooToIdCondIdxColumnValue>>() {
                    @Override
                    public ImmutableSet<FooToIdCondIdxColumnValue> apply(FooToIdCondIdxRowResult rowResult) {
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
        public void delete(FooToIdCondIdxRow row, FooToIdCondIdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<FooToIdCondIdxRow> rows) {
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumn> toRemove = HashMultimap.create();
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(FooToIdCondIdxRow rowName, Iterable<FooToIdCondIdxColumnValue> values) {
            put(ImmutableMultimap.<FooToIdCondIdxRow, FooToIdCondIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(FooToIdCondIdxRow rowName, FooToIdCondIdxColumnValue... values) {
            put(ImmutableMultimap.<FooToIdCondIdxRow, FooToIdCondIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<FooToIdCondIdxRow, ? extends FooToIdCondIdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (FooToIdCondIdxTrigger trigger : triggers) {
                trigger.putFooToIdCondIdx(values);
            }
        }

        @Override
        public void touch(Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumn> values) {
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<FooToIdCondIdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(FooToIdCondIdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> get(Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    FooToIdCondIdxRow row = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = FooToIdCondIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, FooToIdCondIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<FooToIdCondIdxColumnValue> getRowColumns(FooToIdCondIdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<FooToIdCondIdxColumnValue> getRowColumns(FooToIdCondIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<FooToIdCondIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = FooToIdCondIdxColumnValue.hydrateValue(e.getValue());
                    ret.add(FooToIdCondIdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> getRowsMultimap(Iterable<FooToIdCondIdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> getRowsMultimap(Iterable<FooToIdCondIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> getRowsMultimapInternal(Iterable<FooToIdCondIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<FooToIdCondIdxRow, FooToIdCondIdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                FooToIdCondIdxRow row = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = FooToIdCondIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, FooToIdCondIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<FooToIdCondIdxRow, BatchingVisitable<FooToIdCondIdxColumnValue>> getRowsColumnRange(Iterable<FooToIdCondIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<FooToIdCondIdxRow, BatchingVisitable<FooToIdCondIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                FooToIdCondIdxRow row = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<FooToIdCondIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = FooToIdCondIdxColumnValue.hydrateValue(result.getValue());
                    return FooToIdCondIdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<FooToIdCondIdxRow, FooToIdCondIdxColumnValue>> getRowsColumnRange(Iterable<FooToIdCondIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                FooToIdCondIdxRow row = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = FooToIdCondIdxColumnValue.hydrateValue(e.getValue());
                FooToIdCondIdxColumnValue colValue = FooToIdCondIdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<FooToIdCondIdxRow, Iterator<FooToIdCondIdxColumnValue>> getRowsColumnRangeIterator(Iterable<FooToIdCondIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<FooToIdCondIdxRow, Iterator<FooToIdCondIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                FooToIdCondIdxRow row = FooToIdCondIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<FooToIdCondIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    FooToIdCondIdxColumn col = FooToIdCondIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = FooToIdCondIdxColumnValue.hydrateValue(result.getValue());
                    return FooToIdCondIdxColumnValue.of(col, val);
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

        public BatchingVisitableView<FooToIdCondIdxRowResult> getAllRowsUnordered() {
            return getAllRowsUnordered(allColumns);
        }

        public BatchingVisitableView<FooToIdCondIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
            return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                    .retainColumns(optimizeColumnSelection(columns)).build()),
                    new Function<RowResult<byte[]>, FooToIdCondIdxRowResult>() {
                @Override
                public FooToIdCondIdxRowResult apply(RowResult<byte[]> input) {
                    return FooToIdCondIdxRowResult.of(input);
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class FooToIdIdxTable implements
            AtlasDbDynamicMutablePersistentTable<FooToIdIdxTable.FooToIdIdxRow,
                                                    FooToIdIdxTable.FooToIdIdxColumn,
                                                    FooToIdIdxTable.FooToIdIdxColumnValue,
                                                    FooToIdIdxTable.FooToIdIdxRowResult> {
        private final Transaction t;
        private final List<FooToIdIdxTrigger> triggers;
        private final static String rawTableName = "foo_to_id_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static FooToIdIdxTable of(TwoColumnsTable table) {
            return new FooToIdIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<FooToIdIdxTrigger>of());
        }

        public static FooToIdIdxTable of(TwoColumnsTable table, FooToIdIdxTrigger trigger, FooToIdIdxTrigger... triggers) {
            return new FooToIdIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<FooToIdIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static FooToIdIdxTable of(TwoColumnsTable table, List<FooToIdIdxTrigger> triggers) {
            return new FooToIdIdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private FooToIdIdxTable(Transaction t, Namespace namespace, List<FooToIdIdxTrigger> triggers) {
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
         * FooToIdIdxRow {
         *   {@literal Long hashOfRowComponents};
         *   {@literal Long foo};
         * }
         * </pre>
         */
        public static final class FooToIdIdxRow implements Persistable, Comparable<FooToIdIdxRow> {
            private final long hashOfRowComponents;
            private final long foo;

            public static FooToIdIdxRow of(long foo) {
                long hashOfRowComponents = computeHashFirstComponents(foo);
                return new FooToIdIdxRow(hashOfRowComponents, foo);
            }

            private FooToIdIdxRow(long hashOfRowComponents, long foo) {
                this.hashOfRowComponents = hashOfRowComponents;
                this.foo = foo;
            }

            public long getFoo() {
                return foo;
            }

            public static Function<FooToIdIdxRow, Long> getFooFun() {
                return new Function<FooToIdIdxRow, Long>() {
                    @Override
                    public Long apply(FooToIdIdxRow row) {
                        return row.foo;
                    }
                };
            }

            public static Function<Long, FooToIdIdxRow> fromFooFun() {
                return new Function<Long, FooToIdIdxRow>() {
                    @Override
                    public FooToIdIdxRow apply(Long row) {
                        return FooToIdIdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
                byte[] fooBytes = PtBytes.toBytes(Long.MIN_VALUE ^ foo);
                return EncodingUtils.add(hashOfRowComponentsBytes, fooBytes);
            }

            public static final Hydrator<FooToIdIdxRow> BYTES_HYDRATOR = new Hydrator<FooToIdIdxRow>() {
                @Override
                public FooToIdIdxRow hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    Long foo = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    return new FooToIdIdxRow(hashOfRowComponents, foo);
                }
            };

            public static long computeHashFirstComponents(long foo) {
                byte[] fooBytes = PtBytes.toBytes(Long.MIN_VALUE ^ foo);
                return Hashing.murmur3_128().hashBytes(EncodingUtils.add(fooBytes)).asLong();
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("hashOfRowComponents", hashOfRowComponents)
                    .add("foo", foo)
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
                FooToIdIdxRow other = (FooToIdIdxRow) obj;
                return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(foo, other.foo);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, foo });
            }

            @Override
            public int compareTo(FooToIdIdxRow o) {
                return ComparisonChain.start()
                    .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                    .compare(this.foo, o.foo)
                    .result();
            }
        }

        /**
         * <pre>
         * FooToIdIdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal Long id};
         * }
         * </pre>
         */
        public static final class FooToIdIdxColumn implements Persistable, Comparable<FooToIdIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final long id;

            public static FooToIdIdxColumn of(byte[] rowName, byte[] columnName, long id) {
                return new FooToIdIdxColumn(rowName, columnName, id);
            }

            private FooToIdIdxColumn(byte[] rowName, byte[] columnName, long id) {
                this.rowName = rowName;
                this.columnName = columnName;
                this.id = id;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public long getId() {
                return id;
            }

            public static Function<FooToIdIdxColumn, byte[]> getRowNameFun() {
                return new Function<FooToIdIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(FooToIdIdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<FooToIdIdxColumn, byte[]> getColumnNameFun() {
                return new Function<FooToIdIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(FooToIdIdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<FooToIdIdxColumn, Long> getIdFun() {
                return new Function<FooToIdIdxColumn, Long>() {
                    @Override
                    public Long apply(FooToIdIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<FooToIdIdxColumn> BYTES_HYDRATOR = new Hydrator<FooToIdIdxColumn>() {
                @Override
                public FooToIdIdxColumn hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(columnName);
                    Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    return new FooToIdIdxColumn(rowName, columnName, id);
                }
            };

            public static BatchColumnRangeSelection createPrefixRangeUnsorted(byte[] rowName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes), batchSize);
            }

            public static Prefix prefixUnsorted(byte[] rowName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return new Prefix(EncodingUtils.add(rowNameBytes));
            }

            public static BatchColumnRangeSelection createPrefixRange(byte[] rowName, byte[] columnName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes, columnNameBytes), batchSize);
            }

            public static Prefix prefix(byte[] rowName, byte[] columnName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return new Prefix(EncodingUtils.add(rowNameBytes, columnNameBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                FooToIdIdxColumn other = (FooToIdIdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(FooToIdIdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface FooToIdIdxTrigger {
            public void putFooToIdIdx(Multimap<FooToIdIdxRow, ? extends FooToIdIdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal Long id};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class FooToIdIdxColumnValue implements ColumnValue<Long> {
            private final FooToIdIdxColumn columnName;
            private final Long value;

            public static FooToIdIdxColumnValue of(FooToIdIdxColumn columnName, Long value) {
                return new FooToIdIdxColumnValue(columnName, value);
            }

            private FooToIdIdxColumnValue(FooToIdIdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public FooToIdIdxColumn getColumnName() {
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

            public static Function<FooToIdIdxColumnValue, FooToIdIdxColumn> getColumnNameFun() {
                return new Function<FooToIdIdxColumnValue, FooToIdIdxColumn>() {
                    @Override
                    public FooToIdIdxColumn apply(FooToIdIdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<FooToIdIdxColumnValue, Long> getValueFun() {
                return new Function<FooToIdIdxColumnValue, Long>() {
                    @Override
                    public Long apply(FooToIdIdxColumnValue columnValue) {
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

        public static final class FooToIdIdxRowResult implements TypedRowResult {
            private final FooToIdIdxRow rowName;
            private final ImmutableSet<FooToIdIdxColumnValue> columnValues;

            public static FooToIdIdxRowResult of(RowResult<byte[]> rowResult) {
                FooToIdIdxRow rowName = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<FooToIdIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = FooToIdIdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(FooToIdIdxColumnValue.of(col, value));
                }
                return new FooToIdIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private FooToIdIdxRowResult(FooToIdIdxRow rowName, ImmutableSet<FooToIdIdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public FooToIdIdxRow getRowName() {
                return rowName;
            }

            public Set<FooToIdIdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<FooToIdIdxRowResult, FooToIdIdxRow> getRowNameFun() {
                return new Function<FooToIdIdxRowResult, FooToIdIdxRow>() {
                    @Override
                    public FooToIdIdxRow apply(FooToIdIdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<FooToIdIdxRowResult, ImmutableSet<FooToIdIdxColumnValue>> getColumnValuesFun() {
                return new Function<FooToIdIdxRowResult, ImmutableSet<FooToIdIdxColumnValue>>() {
                    @Override
                    public ImmutableSet<FooToIdIdxColumnValue> apply(FooToIdIdxRowResult rowResult) {
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
        public void delete(FooToIdIdxRow row, FooToIdIdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<FooToIdIdxRow> rows) {
            Multimap<FooToIdIdxRow, FooToIdIdxColumn> toRemove = HashMultimap.create();
            Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<FooToIdIdxRow, FooToIdIdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<FooToIdIdxRow, FooToIdIdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(FooToIdIdxRow rowName, Iterable<FooToIdIdxColumnValue> values) {
            put(ImmutableMultimap.<FooToIdIdxRow, FooToIdIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(FooToIdIdxRow rowName, FooToIdIdxColumnValue... values) {
            put(ImmutableMultimap.<FooToIdIdxRow, FooToIdIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<FooToIdIdxRow, ? extends FooToIdIdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (FooToIdIdxTrigger trigger : triggers) {
                trigger.putFooToIdIdx(values);
            }
        }

        @Override
        public void touch(Multimap<FooToIdIdxRow, FooToIdIdxColumn> values) {
            Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<FooToIdIdxRow, FooToIdIdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<FooToIdIdxRow, FooToIdIdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<FooToIdIdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(FooToIdIdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> get(Multimap<FooToIdIdxRow, FooToIdIdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    FooToIdIdxRow row = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = FooToIdIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, FooToIdIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<FooToIdIdxColumnValue> getRowColumns(FooToIdIdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<FooToIdIdxColumnValue> getRowColumns(FooToIdIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<FooToIdIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = FooToIdIdxColumnValue.hydrateValue(e.getValue());
                    ret.add(FooToIdIdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> getRowsMultimap(Iterable<FooToIdIdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> getRowsMultimap(Iterable<FooToIdIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> getRowsMultimapInternal(Iterable<FooToIdIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<FooToIdIdxRow, FooToIdIdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                FooToIdIdxRow row = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = FooToIdIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, FooToIdIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<FooToIdIdxRow, BatchingVisitable<FooToIdIdxColumnValue>> getRowsColumnRange(Iterable<FooToIdIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<FooToIdIdxRow, BatchingVisitable<FooToIdIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                FooToIdIdxRow row = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<FooToIdIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = FooToIdIdxColumnValue.hydrateValue(result.getValue());
                    return FooToIdIdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<FooToIdIdxRow, FooToIdIdxColumnValue>> getRowsColumnRange(Iterable<FooToIdIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                FooToIdIdxRow row = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = FooToIdIdxColumnValue.hydrateValue(e.getValue());
                FooToIdIdxColumnValue colValue = FooToIdIdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<FooToIdIdxRow, Iterator<FooToIdIdxColumnValue>> getRowsColumnRangeIterator(Iterable<FooToIdIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<FooToIdIdxRow, Iterator<FooToIdIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                FooToIdIdxRow row = FooToIdIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<FooToIdIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    FooToIdIdxColumn col = FooToIdIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = FooToIdIdxColumnValue.hydrateValue(result.getValue());
                    return FooToIdIdxColumnValue.of(col, val);
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

        public BatchingVisitableView<FooToIdIdxRowResult> getAllRowsUnordered() {
            return getAllRowsUnordered(allColumns);
        }

        public BatchingVisitableView<FooToIdIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
            return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                    .retainColumns(optimizeColumnSelection(columns)).build()),
                    new Function<RowResult<byte[]>, FooToIdIdxRowResult>() {
                @Override
                public FooToIdIdxRowResult apply(RowResult<byte[]> input) {
                    return FooToIdIdxRowResult.of(input);
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
    static String __CLASS_HASH = "WaJpH1EGwXSVcsjD1Ik0iQ==";
}
