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
public final class CompactMetadataTable implements
        AtlasDbMutablePersistentTable<CompactMetadataTable.CompactMetadataRow,
                                         CompactMetadataTable.CompactMetadataNamedColumnValue<?>,
                                         CompactMetadataTable.CompactMetadataRowResult>,
        AtlasDbNamedMutableTable<CompactMetadataTable.CompactMetadataRow,
                                    CompactMetadataTable.CompactMetadataNamedColumnValue<?>,
                                    CompactMetadataTable.CompactMetadataRowResult> {
    private final Transaction t;
    private final List<CompactMetadataTrigger> triggers;
    private final static String rawTableName = "metadata";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(CompactMetadataNamedColumn.values());

    static CompactMetadataTable of(Transaction t, Namespace namespace) {
        return new CompactMetadataTable(t, namespace, ImmutableList.<CompactMetadataTrigger>of());
    }

    static CompactMetadataTable of(Transaction t, Namespace namespace, CompactMetadataTrigger trigger, CompactMetadataTrigger... triggers) {
        return new CompactMetadataTable(t, namespace, ImmutableList.<CompactMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static CompactMetadataTable of(Transaction t, Namespace namespace, List<CompactMetadataTrigger> triggers) {
        return new CompactMetadataTable(t, namespace, triggers);
    }

    private CompactMetadataTable(Transaction t, Namespace namespace, List<CompactMetadataTrigger> triggers) {
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
     * CompactMetadataRow {
     *   {@literal String fullTableName};
     * }
     * </pre>
     */
    public static final class CompactMetadataRow implements Persistable, Comparable<CompactMetadataRow> {
        private final String fullTableName;

        public static CompactMetadataRow of(String fullTableName) {
            return new CompactMetadataRow(fullTableName);
        }

        private CompactMetadataRow(String fullTableName) {
            this.fullTableName = fullTableName;
        }

        public String getFullTableName() {
            return fullTableName;
        }

        public static Function<CompactMetadataRow, String> getFullTableNameFun() {
            return new Function<CompactMetadataRow, String>() {
                @Override
                public String apply(CompactMetadataRow row) {
                    return row.fullTableName;
                }
            };
        }

        public static Function<String, CompactMetadataRow> fromFullTableNameFun() {
            return new Function<String, CompactMetadataRow>() {
                @Override
                public CompactMetadataRow apply(String row) {
                    return CompactMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] fullTableNameBytes = PtBytes.toBytes(fullTableName);
            return EncodingUtils.add(fullTableNameBytes);
        }

        public static final Hydrator<CompactMetadataRow> BYTES_HYDRATOR = new Hydrator<CompactMetadataRow>() {
            @Override
            public CompactMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String fullTableName = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new CompactMetadataRow(fullTableName);
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
            CompactMetadataRow other = (CompactMetadataRow) obj;
            return Objects.equals(fullTableName, other.fullTableName);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(fullTableName);
        }

        @Override
        public int compareTo(CompactMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.fullTableName, o.fullTableName)
                .result();
        }
    }

    public interface CompactMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class LastCompactTime implements CompactMetadataNamedColumnValue<Long> {
        private final Long value;

        public static LastCompactTime of(Long value) {
            return new LastCompactTime(value);
        }

        private LastCompactTime(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "last_compact_time";
        }

        @Override
        public String getShortColumnName() {
            return "t";
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
            return PtBytes.toCachedBytes("t");
        }

        public static final Hydrator<LastCompactTime> BYTES_HYDRATOR = new Hydrator<LastCompactTime>() {
            @Override
            public LastCompactTime hydrateFromBytes(byte[] bytes) {
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

    public interface CompactMetadataTrigger {
        public void putCompactMetadata(Multimap<CompactMetadataRow, ? extends CompactMetadataNamedColumnValue<?>> newRows);
    }

    public static final class CompactMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static CompactMetadataRowResult of(RowResult<byte[]> row) {
            return new CompactMetadataRowResult(row);
        }

        private CompactMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public CompactMetadataRow getRowName() {
            return CompactMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<CompactMetadataRowResult, CompactMetadataRow> getRowNameFun() {
            return new Function<CompactMetadataRowResult, CompactMetadataRow>() {
                @Override
                public CompactMetadataRow apply(CompactMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, CompactMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, CompactMetadataRowResult>() {
                @Override
                public CompactMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new CompactMetadataRowResult(rowResult);
                }
            };
        }

        public boolean hasLastCompactTime() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("t"));
        }

        public Long getLastCompactTime() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("t"));
            if (bytes == null) {
                return null;
            }
            LastCompactTime value = LastCompactTime.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<CompactMetadataRowResult, Long> getLastCompactTimeFun() {
            return new Function<CompactMetadataRowResult, Long>() {
                @Override
                public Long apply(CompactMetadataRowResult rowResult) {
                    return rowResult.getLastCompactTime();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("LastCompactTime", getLastCompactTime())
                .toString();
        }
    }

    public enum CompactMetadataNamedColumn {
        LAST_COMPACT_TIME {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("t");
            }
        };

        public abstract byte[] getShortName();

        public static Function<CompactMetadataNamedColumn, byte[]> toShortName() {
            return new Function<CompactMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(CompactMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<CompactMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, CompactMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(CompactMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends CompactMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends CompactMetadataNamedColumnValue<?>>>builder()
                .put("t", LastCompactTime.BYTES_HYDRATOR)
                .build();

    public Map<CompactMetadataRow, Long> getLastCompactTimes(Collection<CompactMetadataRow> rows) {
        Map<Cell, CompactMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (CompactMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("t")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<CompactMetadataRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = LastCompactTime.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putLastCompactTime(CompactMetadataRow row, Long value) {
        put(ImmutableMultimap.of(row, LastCompactTime.of(value)));
    }

    public void putLastCompactTime(Map<CompactMetadataRow, Long> map) {
        Map<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<CompactMetadataRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), LastCompactTime.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<CompactMetadataRow, ? extends CompactMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (CompactMetadataTrigger trigger : triggers) {
            trigger.putCompactMetadata(rows);
        }
    }

    public void deleteLastCompactTime(CompactMetadataRow row) {
        deleteLastCompactTime(ImmutableSet.of(row));
    }

    public void deleteLastCompactTime(Iterable<CompactMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("t");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(CompactMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<CompactMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("t")));
        t.delete(tableRef, cells);
    }

    public Optional<CompactMetadataRowResult> getRow(CompactMetadataRow row) {
        return getRow(row, allColumns);
    }

    public Optional<CompactMetadataRowResult> getRow(CompactMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(CompactMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<CompactMetadataRowResult> getRows(Iterable<CompactMetadataRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<CompactMetadataRowResult> getRows(Iterable<CompactMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<CompactMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(CompactMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<CompactMetadataNamedColumnValue<?>> getRowColumns(CompactMetadataRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<CompactMetadataNamedColumnValue<?>> getRowColumns(CompactMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<CompactMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<CompactMetadataRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<CompactMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<CompactMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<CompactMetadataRow, CompactMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            CompactMetadataRow row = CompactMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<CompactMetadataRow, BatchingVisitable<CompactMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<CompactMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<CompactMetadataRow, BatchingVisitable<CompactMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            CompactMetadataRow row = CompactMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<CompactMetadataNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<CompactMetadataRow, CompactMetadataNamedColumnValue<?>>> getRowsColumnRange(Iterable<CompactMetadataRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            CompactMetadataRow row = CompactMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            CompactMetadataNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<CompactMetadataRow, Iterator<CompactMetadataNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<CompactMetadataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<CompactMetadataRow, Iterator<CompactMetadataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            CompactMetadataRow row = CompactMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<CompactMetadataNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<CompactMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<CompactMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, CompactMetadataRowResult>() {
            @Override
            public CompactMetadataRowResult apply(RowResult<byte[]> input) {
                return CompactMetadataRowResult.of(input);
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
    static String __CLASS_HASH = "2gGbOYcqE0bNlw1kWmNLIQ==";
}
