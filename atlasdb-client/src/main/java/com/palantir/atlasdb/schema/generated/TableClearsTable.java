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
public final class TableClearsTable implements
        AtlasDbMutablePersistentTable<TableClearsTable.TableClearsRow,
                                         TableClearsTable.TableClearsNamedColumnValue<?>,
                                         TableClearsTable.TableClearsRowResult>,
        AtlasDbNamedMutableTable<TableClearsTable.TableClearsRow,
                                    TableClearsTable.TableClearsNamedColumnValue<?>,
                                    TableClearsTable.TableClearsRowResult> {
    private final Transaction t;
    private final List<TableClearsTrigger> triggers;
    private final static String rawTableName = "tableClears";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TableClearsNamedColumn.values());

    static TableClearsTable of(Transaction t, Namespace namespace) {
        return new TableClearsTable(t, namespace, ImmutableList.<TableClearsTrigger>of());
    }

    static TableClearsTable of(Transaction t, Namespace namespace, TableClearsTrigger trigger, TableClearsTrigger... triggers) {
        return new TableClearsTable(t, namespace, ImmutableList.<TableClearsTrigger>builder().add(trigger).add(triggers).build());
    }

    static TableClearsTable of(Transaction t, Namespace namespace, List<TableClearsTrigger> triggers) {
        return new TableClearsTable(t, namespace, triggers);
    }

    private TableClearsTable(Transaction t, Namespace namespace, List<TableClearsTrigger> triggers) {
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
     * TableClearsRow {
     *   {@literal String table};
     * }
     * </pre>
     */
    public static final class TableClearsRow implements Persistable, Comparable<TableClearsRow> {
        private final String table;

        public static TableClearsRow of(String table) {
            return new TableClearsRow(table);
        }

        private TableClearsRow(String table) {
            this.table = table;
        }

        public String getTable() {
            return table;
        }

        public static Function<TableClearsRow, String> getTableFun() {
            return new Function<TableClearsRow, String>() {
                @Override
                public String apply(TableClearsRow row) {
                    return row.table;
                }
            };
        }

        public static Function<String, TableClearsRow> fromTableFun() {
            return new Function<String, TableClearsRow>() {
                @Override
                public TableClearsRow apply(String row) {
                    return TableClearsRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] tableBytes = PtBytes.toBytes(table);
            return EncodingUtils.add(tableBytes);
        }

        public static final Hydrator<TableClearsRow> BYTES_HYDRATOR = new Hydrator<TableClearsRow>() {
            @Override
            public TableClearsRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                String table = PtBytes.toString(_input, _index, _input.length-_index);
                _index += 0;
                return new TableClearsRow(table);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
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
            TableClearsRow other = (TableClearsRow) obj;
            return Objects.equals(table, other.table);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(table);
        }

        @Override
        public int compareTo(TableClearsRow o) {
            return ComparisonChain.start()
                .compare(this.table, o.table)
                .result();
        }
    }

    public interface TableClearsNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class LastClearedTimestamp implements TableClearsNamedColumnValue<Long> {
        private final Long value;

        public static LastClearedTimestamp of(Long value) {
            return new LastClearedTimestamp(value);
        }

        private LastClearedTimestamp(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "lastClearedTimestamp";
        }

        @Override
        public String getShortColumnName() {
            return "l";
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
            return PtBytes.toCachedBytes("l");
        }

        public static final Hydrator<LastClearedTimestamp> BYTES_HYDRATOR = new Hydrator<LastClearedTimestamp>() {
            @Override
            public LastClearedTimestamp hydrateFromBytes(byte[] bytes) {
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

    public interface TableClearsTrigger {
        public void putTableClears(Multimap<TableClearsRow, ? extends TableClearsNamedColumnValue<?>> newRows);
    }

    public static final class TableClearsRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TableClearsRowResult of(RowResult<byte[]> row) {
            return new TableClearsRowResult(row);
        }

        private TableClearsRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TableClearsRow getRowName() {
            return TableClearsRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TableClearsRowResult, TableClearsRow> getRowNameFun() {
            return new Function<TableClearsRowResult, TableClearsRow>() {
                @Override
                public TableClearsRow apply(TableClearsRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TableClearsRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TableClearsRowResult>() {
                @Override
                public TableClearsRowResult apply(RowResult<byte[]> rowResult) {
                    return new TableClearsRowResult(rowResult);
                }
            };
        }

        public boolean hasLastClearedTimestamp() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("l"));
        }

        public Long getLastClearedTimestamp() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("l"));
            if (bytes == null) {
                return null;
            }
            LastClearedTimestamp value = LastClearedTimestamp.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TableClearsRowResult, Long> getLastClearedTimestampFun() {
            return new Function<TableClearsRowResult, Long>() {
                @Override
                public Long apply(TableClearsRowResult rowResult) {
                    return rowResult.getLastClearedTimestamp();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("LastClearedTimestamp", getLastClearedTimestamp())
                .toString();
        }
    }

    public enum TableClearsNamedColumn {
        LAST_CLEARED_TIMESTAMP {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("l");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TableClearsNamedColumn, byte[]> toShortName() {
            return new Function<TableClearsNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TableClearsNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TableClearsNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TableClearsNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TableClearsNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TableClearsNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TableClearsNamedColumnValue<?>>>builder()
                .put("l", LastClearedTimestamp.BYTES_HYDRATOR)
                .build();

    public Map<TableClearsRow, Long> getLastClearedTimestamps(Collection<TableClearsRow> rows) {
        Map<Cell, TableClearsRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TableClearsRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("l")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TableClearsRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = LastClearedTimestamp.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putLastClearedTimestamp(TableClearsRow row, Long value) {
        put(ImmutableMultimap.of(row, LastClearedTimestamp.of(value)));
    }

    public void putLastClearedTimestamp(Map<TableClearsRow, Long> map) {
        Map<TableClearsRow, TableClearsNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TableClearsRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), LastClearedTimestamp.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TableClearsRow, ? extends TableClearsNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TableClearsTrigger trigger : triggers) {
            trigger.putTableClears(rows);
        }
    }

    public void deleteLastClearedTimestamp(TableClearsRow row) {
        deleteLastClearedTimestamp(ImmutableSet.of(row));
    }

    public void deleteLastClearedTimestamp(Iterable<TableClearsRow> rows) {
        byte[] col = PtBytes.toCachedBytes("l");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TableClearsRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TableClearsRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("l")));
        t.delete(tableRef, cells);
    }

    public Optional<TableClearsRowResult> getRow(TableClearsRow row) {
        return getRow(row, allColumns);
    }

    public Optional<TableClearsRowResult> getRow(TableClearsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(TableClearsRowResult.of(rowResult));
        }
    }

    @Override
    public List<TableClearsRowResult> getRows(Iterable<TableClearsRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TableClearsRowResult> getRows(Iterable<TableClearsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TableClearsRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TableClearsRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TableClearsNamedColumnValue<?>> getRowColumns(TableClearsRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TableClearsNamedColumnValue<?>> getRowColumns(TableClearsRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TableClearsNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TableClearsRow, TableClearsNamedColumnValue<?>> getRowsMultimap(Iterable<TableClearsRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TableClearsRow, TableClearsNamedColumnValue<?>> getRowsMultimap(Iterable<TableClearsRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<TableClearsRow, TableClearsNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TableClearsRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TableClearsRow, TableClearsNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TableClearsRow, TableClearsNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TableClearsRow row = TableClearsRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TableClearsRow, BatchingVisitable<TableClearsNamedColumnValue<?>>> getRowsColumnRange(Iterable<TableClearsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TableClearsRow, BatchingVisitable<TableClearsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TableClearsRow row = TableClearsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TableClearsNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TableClearsRow, TableClearsNamedColumnValue<?>>> getRowsColumnRange(Iterable<TableClearsRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TableClearsRow row = TableClearsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TableClearsNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<TableClearsRow, Iterator<TableClearsNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<TableClearsRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TableClearsRow, Iterator<TableClearsNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TableClearsRow row = TableClearsRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<TableClearsNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<TableClearsRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TableClearsRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, TableClearsRowResult>() {
            @Override
            public TableClearsRowResult apply(RowResult<byte[]> input) {
                return TableClearsRowResult.of(input);
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
    static String __CLASS_HASH = "ZTQrUhB7dvmc+eZsO+E7CA==";
}
