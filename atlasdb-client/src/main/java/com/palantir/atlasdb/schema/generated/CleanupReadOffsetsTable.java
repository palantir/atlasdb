package com.palantir.atlasdb.schema.generated;

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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

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
public final class CleanupReadOffsetsTable implements
        AtlasDbMutablePersistentTable<CleanupReadOffsetsTable.CleanupReadOffsetsRow,
                                         CleanupReadOffsetsTable.CleanupReadOffsetsNamedColumnValue<?>,
                                         CleanupReadOffsetsTable.CleanupReadOffsetsRowResult>,
        AtlasDbNamedMutableTable<CleanupReadOffsetsTable.CleanupReadOffsetsRow,
                                    CleanupReadOffsetsTable.CleanupReadOffsetsNamedColumnValue<?>,
                                    CleanupReadOffsetsTable.CleanupReadOffsetsRowResult> {
    private final Transaction t;
    private final List<CleanupReadOffsetsTrigger> triggers;
    private final static String rawTableName = "cleanup_read_offsets";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(CleanupReadOffsetsNamedColumn.values());

    static CleanupReadOffsetsTable of(Transaction t, Namespace namespace) {
        return new CleanupReadOffsetsTable(t, namespace, ImmutableList.<CleanupReadOffsetsTrigger>of());
    }

    static CleanupReadOffsetsTable of(Transaction t, Namespace namespace, CleanupReadOffsetsTrigger trigger, CleanupReadOffsetsTrigger... triggers) {
        return new CleanupReadOffsetsTable(t, namespace, ImmutableList.<CleanupReadOffsetsTrigger>builder().add(trigger).add(triggers).build());
    }

    static CleanupReadOffsetsTable of(Transaction t, Namespace namespace, List<CleanupReadOffsetsTrigger> triggers) {
        return new CleanupReadOffsetsTable(t, namespace, triggers);
    }

    private CleanupReadOffsetsTable(Transaction t, Namespace namespace, List<CleanupReadOffsetsTrigger> triggers) {
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
     * CleanupReadOffsetsRow {
     *   {@literal String fullTableName};
     * }
     * </pre>
     */
    public static final class CleanupReadOffsetsRow implements Persistable, Comparable<CleanupReadOffsetsRow> {
        private final String fullTableName;

        public static CleanupReadOffsetsRow of(String fullTableName) {
            return new CleanupReadOffsetsRow(fullTableName);
        }

        private CleanupReadOffsetsRow(String fullTableName) {
            this.fullTableName = fullTableName;
        }

        public String getFullTableName() {
            return fullTableName;
        }

        public static Function<CleanupReadOffsetsRow, String> getFullTableNameFun() {
            return new Function<CleanupReadOffsetsRow, String>() {
                @Override
                public String apply(CleanupReadOffsetsRow row) {
                    return row.fullTableName;
                }
            };
        }

        public static Function<String, CleanupReadOffsetsRow> fromFullTableNameFun() {
            return new Function<String, CleanupReadOffsetsRow>() {
                @Override
                public CleanupReadOffsetsRow apply(String row) {
                    return CleanupReadOffsetsRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] fullTableNameBytes = PtBytes.toBytes(fullTableName);
            return EncodingUtils.add(fullTableNameBytes);
        }

        public static final Hydrator<CleanupReadOffsetsRow> BYTES_HYDRATOR = new Hydrator<CleanupReadOffsetsRow>() {
            @Override
            public CleanupReadOffsetsRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String fullTableName = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new CleanupReadOffsetsRow(fullTableName);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("fullTableName", fullTableName)
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
            CleanupReadOffsetsRow other = (CleanupReadOffsetsRow) obj;
            return Objects.equal(fullTableName, other.fullTableName);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(fullTableName);
        }

        @Override
        public int compareTo(CleanupReadOffsetsRow o) {
            return ComparisonChain.start()
                .compare(this.fullTableName, o.fullTableName)
                .result();
        }
    }

    public interface CleanupReadOffsetsNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Offset implements CleanupReadOffsetsNamedColumnValue<Long> {
        private final Long value;

        public static Offset of(Long value) {
            return new Offset(value);
        }

        private Offset(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "offset";
        }

        @Override
        public String getShortColumnName() {
            return "o";
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
            return PtBytes.toCachedBytes("o");
        }

        public static final Hydrator<Offset> BYTES_HYDRATOR = new Hydrator<Offset>() {
            @Override
            public Offset hydrateFromBytes(byte[] bytes) {
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

    public interface CleanupReadOffsetsTrigger {
        public void putCleanupReadOffsets(Multimap<CleanupReadOffsetsRow, ? extends CleanupReadOffsetsNamedColumnValue<?>> newRows);
    }

    public static final class CleanupReadOffsetsRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static CleanupReadOffsetsRowResult of(RowResult<byte[]> row) {
            return new CleanupReadOffsetsRowResult(row);
        }

        private CleanupReadOffsetsRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public CleanupReadOffsetsRow getRowName() {
            return CleanupReadOffsetsRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<CleanupReadOffsetsRowResult, CleanupReadOffsetsRow> getRowNameFun() {
            return new Function<CleanupReadOffsetsRowResult, CleanupReadOffsetsRow>() {
                @Override
                public CleanupReadOffsetsRow apply(CleanupReadOffsetsRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, CleanupReadOffsetsRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, CleanupReadOffsetsRowResult>() {
                @Override
                public CleanupReadOffsetsRowResult apply(RowResult<byte[]> rowResult) {
                    return new CleanupReadOffsetsRowResult(rowResult);
                }
            };
        }

        public boolean hasOffset() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("o"));
        }

        public Long getOffset() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("o"));
            if (bytes == null) {
                return null;
            }
            Offset value = Offset.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<CleanupReadOffsetsRowResult, Long> getOffsetFun() {
            return new Function<CleanupReadOffsetsRowResult, Long>() {
                @Override
                public Long apply(CleanupReadOffsetsRowResult rowResult) {
                    return rowResult.getOffset();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Offset", getOffset())
                .toString();
        }
    }

    public enum CleanupReadOffsetsNamedColumn {
        OFFSET {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("o");
            }
        };

        public abstract byte[] getShortName();

        public static Function<CleanupReadOffsetsNamedColumn, byte[]> toShortName() {
            return new Function<CleanupReadOffsetsNamedColumn, byte[]>() {
                @Override
                public byte[] apply(CleanupReadOffsetsNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<CleanupReadOffsetsNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, CleanupReadOffsetsNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(CleanupReadOffsetsNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends CleanupReadOffsetsNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends CleanupReadOffsetsNamedColumnValue<?>>>builder()
                .put("o", Offset.BYTES_HYDRATOR)
                .build();

    public Map<CleanupReadOffsetsRow, Long> getOffsets(Collection<CleanupReadOffsetsRow> rows) {
        Map<Cell, CleanupReadOffsetsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (CleanupReadOffsetsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("o")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<CleanupReadOffsetsRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Offset.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putOffset(CleanupReadOffsetsRow row, Long value) {
        put(ImmutableMultimap.of(row, Offset.of(value)));
    }

    public void putOffset(Map<CleanupReadOffsetsRow, Long> map) {
        Map<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<CleanupReadOffsetsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Offset.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putOffsetUnlessExists(CleanupReadOffsetsRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, Offset.of(value)));
    }

    public void putOffsetUnlessExists(Map<CleanupReadOffsetsRow, Long> map) {
        Map<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<CleanupReadOffsetsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Offset.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<CleanupReadOffsetsRow, ? extends CleanupReadOffsetsNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (CleanupReadOffsetsTrigger trigger : triggers) {
            trigger.putCleanupReadOffsets(rows);
        }
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(Multimap<CleanupReadOffsetsRow, ? extends CleanupReadOffsetsNamedColumnValue<?>> rows) {
        Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<CleanupReadOffsetsRow, ? extends CleanupReadOffsetsNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteOffset(CleanupReadOffsetsRow row) {
        deleteOffset(ImmutableSet.of(row));
    }

    public void deleteOffset(Iterable<CleanupReadOffsetsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("o");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(CleanupReadOffsetsRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<CleanupReadOffsetsRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("o")));
        t.delete(tableRef, cells);
    }

    public Optional<CleanupReadOffsetsRowResult> getRow(CleanupReadOffsetsRow row) {
        return getRow(row, allColumns);
    }

    public Optional<CleanupReadOffsetsRowResult> getRow(CleanupReadOffsetsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(CleanupReadOffsetsRowResult.of(rowResult));
        }
    }

    @Override
    public List<CleanupReadOffsetsRowResult> getRows(Iterable<CleanupReadOffsetsRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<CleanupReadOffsetsRowResult> getRows(Iterable<CleanupReadOffsetsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<CleanupReadOffsetsRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(CleanupReadOffsetsRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<CleanupReadOffsetsNamedColumnValue<?>> getRowColumns(CleanupReadOffsetsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<CleanupReadOffsetsNamedColumnValue<?>> getRowColumns(CleanupReadOffsetsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<CleanupReadOffsetsNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> getRowsMultimap(Iterable<CleanupReadOffsetsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> getRowsMultimap(Iterable<CleanupReadOffsetsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> getRowsMultimapInternal(Iterable<CleanupReadOffsetsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            CleanupReadOffsetsRow row = CleanupReadOffsetsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<CleanupReadOffsetsRow, BatchingVisitable<CleanupReadOffsetsNamedColumnValue<?>>> getRowsColumnRange(Iterable<CleanupReadOffsetsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<CleanupReadOffsetsRow, BatchingVisitable<CleanupReadOffsetsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            CleanupReadOffsetsRow row = CleanupReadOffsetsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<CleanupReadOffsetsNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<CleanupReadOffsetsRow, CleanupReadOffsetsNamedColumnValue<?>>> getRowsColumnRange(Iterable<CleanupReadOffsetsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            CleanupReadOffsetsRow row = CleanupReadOffsetsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            CleanupReadOffsetsNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<CleanupReadOffsetsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<CleanupReadOffsetsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, CleanupReadOffsetsRowResult>() {
            @Override
            public CleanupReadOffsetsRowResult apply(RowResult<byte[]> input) {
                return CleanupReadOffsetsRowResult.of(input);
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
    static String __CLASS_HASH = "zX/4GQebz3UbxIy4HnAQ/w==";
}
