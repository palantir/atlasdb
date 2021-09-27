package com.palantir.atlasdb.schema.generated;

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
public final class SweepNameToIdTable implements
        AtlasDbMutablePersistentTable<SweepNameToIdTable.SweepNameToIdRow,
                                         SweepNameToIdTable.SweepNameToIdNamedColumnValue<?>,
                                         SweepNameToIdTable.SweepNameToIdRowResult>,
        AtlasDbNamedMutableTable<SweepNameToIdTable.SweepNameToIdRow,
                                    SweepNameToIdTable.SweepNameToIdNamedColumnValue<?>,
                                    SweepNameToIdTable.SweepNameToIdRowResult> {
    private final Transaction t;
    private final List<SweepNameToIdTrigger> triggers;
    private final static String rawTableName = "sweepNameToId";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SweepNameToIdNamedColumn.values());

    static SweepNameToIdTable of(Transaction t, Namespace namespace) {
        return new SweepNameToIdTable(t, namespace, ImmutableList.<SweepNameToIdTrigger>of());
    }

    static SweepNameToIdTable of(Transaction t, Namespace namespace, SweepNameToIdTrigger trigger, SweepNameToIdTrigger... triggers) {
        return new SweepNameToIdTable(t, namespace, ImmutableList.<SweepNameToIdTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepNameToIdTable of(Transaction t, Namespace namespace, List<SweepNameToIdTrigger> triggers) {
        return new SweepNameToIdTable(t, namespace, triggers);
    }

    private SweepNameToIdTable(Transaction t, Namespace namespace, List<SweepNameToIdTrigger> triggers) {
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
     * SweepNameToIdRow {
     *   {@literal Long hashOfRowComponents};
     *   {@literal String table};
     * }
     * </pre>
     */
    public static final class SweepNameToIdRow implements Persistable, Comparable<SweepNameToIdRow> {
        private final long hashOfRowComponents;
        private final String table;

        public static SweepNameToIdRow of(String table) {
            long hashOfRowComponents = computeHashFirstComponents(table);
            return new SweepNameToIdRow(hashOfRowComponents, table);
        }

        private SweepNameToIdRow(long hashOfRowComponents, String table) {
            this.hashOfRowComponents = hashOfRowComponents;
            this.table = table;
        }

        public String getTable() {
            return table;
        }

        public static Function<SweepNameToIdRow, String> getTableFun() {
            return new Function<SweepNameToIdRow, String>() {
                @Override
                public String apply(SweepNameToIdRow row) {
                    return row.table;
                }
            };
        }

        public static Function<String, SweepNameToIdRow> fromTableFun() {
            return new Function<String, SweepNameToIdRow>() {
                @Override
                public SweepNameToIdRow apply(String row) {
                    return SweepNameToIdRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
            byte[] tableBytes = PtBytes.toBytes(table);
            return EncodingUtils.add(hashOfRowComponentsBytes, tableBytes);
        }

        public static final Hydrator<SweepNameToIdRow> BYTES_HYDRATOR = new Hydrator<SweepNameToIdRow>() {
            @Override
            public SweepNameToIdRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String table = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SweepNameToIdRow(hashOfRowComponents, table);
            }
        };

        public static long computeHashFirstComponents(String table) {
            byte[] tableBytes = PtBytes.toBytes(table);
            return Hashing.murmur3_128().hashBytes(EncodingUtils.add(tableBytes)).asLong();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashOfRowComponents", hashOfRowComponents)
                .add("table", table)
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
            SweepNameToIdRow other = (SweepNameToIdRow) obj;
            return Objects.equals(hashOfRowComponents, other.hashOfRowComponents) && Objects.equals(table, other.table);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ hashOfRowComponents, table });
        }

        @Override
        public int compareTo(SweepNameToIdRow o) {
            return ComparisonChain.start()
                .compare(this.hashOfRowComponents, o.hashOfRowComponents)
                .compare(this.table, o.table)
                .result();
        }
    }

    public interface SweepNameToIdNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier;
     * }
     * </pre>
     */
    public static final class Id implements SweepNameToIdNamedColumnValue<com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> {
        private final com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier value;

        public static Id of(com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier value) {
            return new Id(value);
        }

        private Id(com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "id";
        }

        @Override
        public String getShortColumnName() {
            return "i";
        }

        @Override
        public com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.persistToBytes();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("i");
        }

        public static final Hydrator<Id> BYTES_HYDRATOR = new Hydrator<Id>() {
            @Override
            public Id hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier.BYTES_HYDRATOR.hydrateFromBytes(bytes));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface SweepNameToIdTrigger {
        public void putSweepNameToId(Multimap<SweepNameToIdRow, ? extends SweepNameToIdNamedColumnValue<?>> newRows);
    }

    public static final class SweepNameToIdRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SweepNameToIdRowResult of(RowResult<byte[]> row) {
            return new SweepNameToIdRowResult(row);
        }

        private SweepNameToIdRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SweepNameToIdRow getRowName() {
            return SweepNameToIdRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SweepNameToIdRowResult, SweepNameToIdRow> getRowNameFun() {
            return new Function<SweepNameToIdRowResult, SweepNameToIdRow>() {
                @Override
                public SweepNameToIdRow apply(SweepNameToIdRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SweepNameToIdRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SweepNameToIdRowResult>() {
                @Override
                public SweepNameToIdRowResult apply(RowResult<byte[]> rowResult) {
                    return new SweepNameToIdRowResult(rowResult);
                }
            };
        }

        public boolean hasId() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("i"));
        }

        public com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier getId() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("i"));
            if (bytes == null) {
                return null;
            }
            Id value = Id.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SweepNameToIdRowResult, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> getIdFun() {
            return new Function<SweepNameToIdRowResult, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier>() {
                @Override
                public com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier apply(SweepNameToIdRowResult rowResult) {
                    return rowResult.getId();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Id", getId())
                .toString();
        }
    }

    public enum SweepNameToIdNamedColumn {
        ID {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("i");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SweepNameToIdNamedColumn, byte[]> toShortName() {
            return new Function<SweepNameToIdNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SweepNameToIdNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SweepNameToIdNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SweepNameToIdNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SweepNameToIdNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SweepNameToIdNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SweepNameToIdNamedColumnValue<?>>>builder()
                .put("i", Id.BYTES_HYDRATOR)
                .build();

    public Map<SweepNameToIdRow, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> getIds(Collection<SweepNameToIdRow> rows) {
        Map<Cell, SweepNameToIdRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepNameToIdRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("i")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepNameToIdRow, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier val = Id.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putId(SweepNameToIdRow row, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier value) {
        put(ImmutableMultimap.of(row, Id.of(value)));
    }

    public void putId(Map<SweepNameToIdRow, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> map) {
        Map<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepNameToIdRow, com.palantir.atlasdb.sweep.queue.id.SweepTableIdentifier> e : map.entrySet()) {
            toPut.put(e.getKey(), Id.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SweepNameToIdRow, ? extends SweepNameToIdNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SweepNameToIdTrigger trigger : triggers) {
            trigger.putSweepNameToId(rows);
        }
    }

    public void deleteId(SweepNameToIdRow row) {
        deleteId(ImmutableSet.of(row));
    }

    public void deleteId(Iterable<SweepNameToIdRow> rows) {
        byte[] col = PtBytes.toCachedBytes("i");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SweepNameToIdRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SweepNameToIdRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("i")));
        t.delete(tableRef, cells);
    }

    public Optional<SweepNameToIdRowResult> getRow(SweepNameToIdRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SweepNameToIdRowResult> getRow(SweepNameToIdRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SweepNameToIdRowResult.of(rowResult));
        }
    }

    @Override
    public List<SweepNameToIdRowResult> getRows(Iterable<SweepNameToIdRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SweepNameToIdRowResult> getRows(Iterable<SweepNameToIdRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SweepNameToIdRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SweepNameToIdRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SweepNameToIdNamedColumnValue<?>> getRowColumns(SweepNameToIdRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepNameToIdNamedColumnValue<?>> getRowColumns(SweepNameToIdRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepNameToIdNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> getRowsMultimap(Iterable<SweepNameToIdRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> getRowsMultimap(Iterable<SweepNameToIdRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SweepNameToIdRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepNameToIdRow row = SweepNameToIdRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepNameToIdRow, BatchingVisitable<SweepNameToIdNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepNameToIdRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepNameToIdRow, BatchingVisitable<SweepNameToIdNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepNameToIdRow row = SweepNameToIdRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepNameToIdNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepNameToIdRow, SweepNameToIdNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepNameToIdRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepNameToIdRow row = SweepNameToIdRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepNameToIdNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepNameToIdRow, Iterator<SweepNameToIdNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SweepNameToIdRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepNameToIdRow, Iterator<SweepNameToIdNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepNameToIdRow row = SweepNameToIdRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepNameToIdNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<SweepNameToIdRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<SweepNameToIdRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, SweepNameToIdRowResult>() {
            @Override
            public SweepNameToIdRowResult apply(RowResult<byte[]> input) {
                return SweepNameToIdRowResult.of(input);
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
    static String __CLASS_HASH = "26eSHzi0SrHfYPlHdTiJHQ==";
}
