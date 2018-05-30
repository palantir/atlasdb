package com.palantir.atlasdb.cas.generated;

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
public final class CheckAndSetTable implements
        AtlasDbMutablePersistentTable<CheckAndSetTable.CheckAndSetRow,
                                         CheckAndSetTable.CheckAndSetNamedColumnValue<?>,
                                         CheckAndSetTable.CheckAndSetRowResult>,
        AtlasDbNamedMutableTable<CheckAndSetTable.CheckAndSetRow,
                                    CheckAndSetTable.CheckAndSetNamedColumnValue<?>,
                                    CheckAndSetTable.CheckAndSetRowResult> {
    private final Transaction t;
    private final List<CheckAndSetTrigger> triggers;
    private final static String rawTableName = "check_and_set";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(CheckAndSetNamedColumn.values());

    static CheckAndSetTable of(Transaction t, Namespace namespace) {
        return new CheckAndSetTable(t, namespace, ImmutableList.<CheckAndSetTrigger>of());
    }

    static CheckAndSetTable of(Transaction t, Namespace namespace, CheckAndSetTrigger trigger, CheckAndSetTrigger... triggers) {
        return new CheckAndSetTable(t, namespace, ImmutableList.<CheckAndSetTrigger>builder().add(trigger).add(triggers).build());
    }

    static CheckAndSetTable of(Transaction t, Namespace namespace, List<CheckAndSetTrigger> triggers) {
        return new CheckAndSetTable(t, namespace, triggers);
    }

    private CheckAndSetTable(Transaction t, Namespace namespace, List<CheckAndSetTrigger> triggers) {
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
     * CheckAndSetRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class CheckAndSetRow implements Persistable, Comparable<CheckAndSetRow> {
        private final long id;

        public static CheckAndSetRow of(long id) {
            return new CheckAndSetRow(id);
        }

        private CheckAndSetRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<CheckAndSetRow, Long> getIdFun() {
            return new Function<CheckAndSetRow, Long>() {
                @Override
                public Long apply(CheckAndSetRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, CheckAndSetRow> fromIdFun() {
            return new Function<Long, CheckAndSetRow>() {
                @Override
                public CheckAndSetRow apply(Long row) {
                    return CheckAndSetRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<CheckAndSetRow> BYTES_HYDRATOR = new Hydrator<CheckAndSetRow>() {
            @Override
            public CheckAndSetRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return new CheckAndSetRow(id);
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
            CheckAndSetRow other = (CheckAndSetRow) obj;
            return Objects.equal(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(CheckAndSetRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface CheckAndSetNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Value implements CheckAndSetNamedColumnValue<Long> {
        private final Long value;

        public static Value of(Long value) {
            return new Value(value);
        }

        private Value(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "value";
        }

        @Override
        public String getShortColumnName() {
            return "v";
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
            return PtBytes.toCachedBytes("v");
        }

        public static final Hydrator<Value> BYTES_HYDRATOR = new Hydrator<Value>() {
            @Override
            public Value hydrateFromBytes(byte[] bytes) {
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

    public interface CheckAndSetTrigger {
        public void putCheckAndSet(Multimap<CheckAndSetRow, ? extends CheckAndSetNamedColumnValue<?>> newRows);
    }

    public static final class CheckAndSetRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static CheckAndSetRowResult of(RowResult<byte[]> row) {
            return new CheckAndSetRowResult(row);
        }

        private CheckAndSetRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public CheckAndSetRow getRowName() {
            return CheckAndSetRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<CheckAndSetRowResult, CheckAndSetRow> getRowNameFun() {
            return new Function<CheckAndSetRowResult, CheckAndSetRow>() {
                @Override
                public CheckAndSetRow apply(CheckAndSetRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, CheckAndSetRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, CheckAndSetRowResult>() {
                @Override
                public CheckAndSetRowResult apply(RowResult<byte[]> rowResult) {
                    return new CheckAndSetRowResult(rowResult);
                }
            };
        }

        public boolean hasValue() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("v"));
        }

        public Long getValue() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("v"));
            if (bytes == null) {
                return null;
            }
            Value value = Value.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<CheckAndSetRowResult, Long> getValueFun() {
            return new Function<CheckAndSetRowResult, Long>() {
                @Override
                public Long apply(CheckAndSetRowResult rowResult) {
                    return rowResult.getValue();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Value", getValue())
                .toString();
        }
    }

    public enum CheckAndSetNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<CheckAndSetNamedColumn, byte[]> toShortName() {
            return new Function<CheckAndSetNamedColumn, byte[]>() {
                @Override
                public byte[] apply(CheckAndSetNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<CheckAndSetNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, CheckAndSetNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(CheckAndSetNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends CheckAndSetNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends CheckAndSetNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<CheckAndSetRow, Long> getValues(Collection<CheckAndSetRow> rows) {
        Map<Cell, CheckAndSetRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (CheckAndSetRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<CheckAndSetRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(CheckAndSetRow row, Long value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<CheckAndSetRow, Long> map) {
        Map<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<CheckAndSetRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putValueUnlessExists(CheckAndSetRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValueUnlessExists(Map<CheckAndSetRow, Long> map) {
        Map<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<CheckAndSetRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<CheckAndSetRow, ? extends CheckAndSetNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (CheckAndSetTrigger trigger : triggers) {
            trigger.putCheckAndSet(rows);
        }
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(Multimap<CheckAndSetRow, ? extends CheckAndSetNamedColumnValue<?>> rows) {
        Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<CheckAndSetRow, ? extends CheckAndSetNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteValue(CheckAndSetRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<CheckAndSetRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(CheckAndSetRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<CheckAndSetRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<CheckAndSetRowResult> getRow(CheckAndSetRow row) {
        return getRow(row, allColumns);
    }

    public Optional<CheckAndSetRowResult> getRow(CheckAndSetRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(CheckAndSetRowResult.of(rowResult));
        }
    }

    @Override
    public List<CheckAndSetRowResult> getRows(Iterable<CheckAndSetRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<CheckAndSetRowResult> getRows(Iterable<CheckAndSetRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<CheckAndSetRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(CheckAndSetRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<CheckAndSetNamedColumnValue<?>> getRowColumns(CheckAndSetRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<CheckAndSetNamedColumnValue<?>> getRowColumns(CheckAndSetRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<CheckAndSetNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> getRowsMultimap(Iterable<CheckAndSetRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> getRowsMultimap(Iterable<CheckAndSetRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> getRowsMultimapInternal(Iterable<CheckAndSetRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<CheckAndSetRow, CheckAndSetNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            CheckAndSetRow row = CheckAndSetRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<CheckAndSetRow, BatchingVisitable<CheckAndSetNamedColumnValue<?>>> getRowsColumnRange(Iterable<CheckAndSetRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<CheckAndSetRow, BatchingVisitable<CheckAndSetNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            CheckAndSetRow row = CheckAndSetRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<CheckAndSetNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    public BatchingVisitableView<CheckAndSetRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<CheckAndSetRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, CheckAndSetRowResult>() {
            @Override
            public CheckAndSetRowResult apply(RowResult<byte[]> input) {
                return CheckAndSetRowResult.of(input);
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
    static String __CLASS_HASH = "pwTXtHhg21BhbdpmnwCIvQ==";
}
