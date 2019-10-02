package com.palantir.atlasdb.migration.generated;

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
public final class ProgressTable implements
        AtlasDbMutablePersistentTable<ProgressTable.ProgressRow,
                                         ProgressTable.ProgressNamedColumnValue<?>,
                                         ProgressTable.ProgressRowResult>,
        AtlasDbNamedMutableTable<ProgressTable.ProgressRow,
                                    ProgressTable.ProgressNamedColumnValue<?>,
                                    ProgressTable.ProgressRowResult> {
    private final Transaction t;
    private final List<ProgressTrigger> triggers;
    private final static String rawTableName = "progress";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(ProgressNamedColumn.values());

    static ProgressTable of(Transaction t, Namespace namespace) {
        return new ProgressTable(t, namespace, ImmutableList.<ProgressTrigger>of());
    }

    static ProgressTable of(Transaction t, Namespace namespace, ProgressTrigger trigger, ProgressTrigger... triggers) {
        return new ProgressTable(t, namespace, ImmutableList.<ProgressTrigger>builder().add(trigger).add(triggers).build());
    }

    static ProgressTable of(Transaction t, Namespace namespace, List<ProgressTrigger> triggers) {
        return new ProgressTable(t, namespace, triggers);
    }

    private ProgressTable(Transaction t, Namespace namespace, List<ProgressTrigger> triggers) {
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
     * ProgressRow {
     *   {@literal byte[] row};
     * }
     * </pre>
     */
    public static final class ProgressRow implements Persistable, Comparable<ProgressRow> {
        private final byte[] row;

        public static ProgressRow of(byte[] row) {
            return new ProgressRow(row);
        }

        private ProgressRow(byte[] row) {
            this.row = row;
        }

        public byte[] getRow() {
            return row;
        }

        public static Function<ProgressRow, byte[]> getRowFun() {
            return new Function<ProgressRow, byte[]>() {
                @Override
                public byte[] apply(ProgressRow row) {
                    return row.row;
                }
            };
        }

        public static Function<byte[], ProgressRow> fromRowFun() {
            return new Function<byte[], ProgressRow>() {
                @Override
                public ProgressRow apply(byte[] row) {
                    return ProgressRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] rowBytes = row;
            return EncodingUtils.add(rowBytes);
        }

        public static final Hydrator<ProgressRow> BYTES_HYDRATOR = new Hydrator<ProgressRow>() {
            @Override
            public ProgressRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] row = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new ProgressRow(row);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("row", row)
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
            ProgressRow other = (ProgressRow) obj;
            return Arrays.equals(row, other.row);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(row);
        }

        @Override
        public int compareTo(ProgressRow o) {
            return ComparisonChain.start()
                .compare(this.row, o.row, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface ProgressNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class IsDone implements ProgressNamedColumnValue<Long> {
        private final Long value;

        public static IsDone of(Long value) {
            return new IsDone(value);
        }

        private IsDone(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "isDone";
        }

        @Override
        public String getShortColumnName() {
            return "d";
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
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<IsDone> BYTES_HYDRATOR = new Hydrator<IsDone>() {
            @Override
            public IsDone hydrateFromBytes(byte[] bytes) {
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
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Progress implements ProgressNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Progress of(byte[] value) {
            return new Progress(value);
        }

        private Progress(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "progress";
        }

        @Override
        public String getShortColumnName() {
            return "p";
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value;
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("p");
        }

        public static final Hydrator<Progress> BYTES_HYDRATOR = new Hydrator<Progress>() {
            @Override
            public Progress hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.getBytesFromOffsetToEnd(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface ProgressTrigger {
        public void putProgress(Multimap<ProgressRow, ? extends ProgressNamedColumnValue<?>> newRows);
    }

    public static final class ProgressRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static ProgressRowResult of(RowResult<byte[]> row) {
            return new ProgressRowResult(row);
        }

        private ProgressRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public ProgressRow getRowName() {
            return ProgressRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<ProgressRowResult, ProgressRow> getRowNameFun() {
            return new Function<ProgressRowResult, ProgressRow>() {
                @Override
                public ProgressRow apply(ProgressRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, ProgressRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, ProgressRowResult>() {
                @Override
                public ProgressRowResult apply(RowResult<byte[]> rowResult) {
                    return new ProgressRowResult(rowResult);
                }
            };
        }

        public boolean hasIsDone() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public boolean hasProgress() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("p"));
        }

        public Long getIsDone() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            IsDone value = IsDone.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public byte[] getProgress() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("p"));
            if (bytes == null) {
                return null;
            }
            Progress value = Progress.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<ProgressRowResult, Long> getIsDoneFun() {
            return new Function<ProgressRowResult, Long>() {
                @Override
                public Long apply(ProgressRowResult rowResult) {
                    return rowResult.getIsDone();
                }
            };
        }

        public static Function<ProgressRowResult, byte[]> getProgressFun() {
            return new Function<ProgressRowResult, byte[]>() {
                @Override
                public byte[] apply(ProgressRowResult rowResult) {
                    return rowResult.getProgress();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("IsDone", getIsDone())
                .add("Progress", getProgress())
                .toString();
        }
    }

    public enum ProgressNamedColumn {
        IS_DONE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        },
        PROGRESS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("p");
            }
        };

        public abstract byte[] getShortName();

        public static Function<ProgressNamedColumn, byte[]> toShortName() {
            return new Function<ProgressNamedColumn, byte[]>() {
                @Override
                public byte[] apply(ProgressNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<ProgressNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, ProgressNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(ProgressNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends ProgressNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends ProgressNamedColumnValue<?>>>builder()
                .put("p", Progress.BYTES_HYDRATOR)
                .put("d", IsDone.BYTES_HYDRATOR)
                .build();

    public Map<ProgressRow, byte[]> getProgresss(Collection<ProgressRow> rows) {
        Map<Cell, ProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (ProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("p")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<ProgressRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Progress.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<ProgressRow, Long> getIsDones(Collection<ProgressRow> rows) {
        Map<Cell, ProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (ProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<ProgressRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = IsDone.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putProgress(ProgressRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Progress.of(value)));
    }

    public void putProgress(Map<ProgressRow, byte[]> map) {
        Map<ProgressRow, ProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<ProgressRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Progress.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putIsDone(ProgressRow row, Long value) {
        put(ImmutableMultimap.of(row, IsDone.of(value)));
    }

    public void putIsDone(Map<ProgressRow, Long> map) {
        Map<ProgressRow, ProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<ProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), IsDone.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<ProgressRow, ? extends ProgressNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (ProgressTrigger trigger : triggers) {
            trigger.putProgress(rows);
        }
    }

    public void deleteProgress(ProgressRow row) {
        deleteProgress(ImmutableSet.of(row));
    }

    public void deleteProgress(Iterable<ProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("p");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteIsDone(ProgressRow row) {
        deleteIsDone(ImmutableSet.of(row));
    }

    public void deleteIsDone(Iterable<ProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(ProgressRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<ProgressRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 2);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("p")));
        t.delete(tableRef, cells);
    }

    public Optional<ProgressRowResult> getRow(ProgressRow row) {
        return getRow(row, allColumns);
    }

    public Optional<ProgressRowResult> getRow(ProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(ProgressRowResult.of(rowResult));
        }
    }

    @Override
    public List<ProgressRowResult> getRows(Iterable<ProgressRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<ProgressRowResult> getRows(Iterable<ProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<ProgressRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(ProgressRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<ProgressNamedColumnValue<?>> getRowColumns(ProgressRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<ProgressNamedColumnValue<?>> getRowColumns(ProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<ProgressNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<ProgressRow, ProgressNamedColumnValue<?>> getRowsMultimap(Iterable<ProgressRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<ProgressRow, ProgressNamedColumnValue<?>> getRowsMultimap(Iterable<ProgressRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<ProgressRow, ProgressNamedColumnValue<?>> getRowsMultimapInternal(Iterable<ProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<ProgressRow, ProgressNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<ProgressRow, ProgressNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            ProgressRow row = ProgressRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<ProgressRow, BatchingVisitable<ProgressNamedColumnValue<?>>> getRowsColumnRange(Iterable<ProgressRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<ProgressRow, BatchingVisitable<ProgressNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            ProgressRow row = ProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<ProgressNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<ProgressRow, ProgressNamedColumnValue<?>>> getRowsColumnRange(Iterable<ProgressRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            ProgressRow row = ProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            ProgressNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<ProgressRow, Iterator<ProgressNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<ProgressRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<ProgressRow, Iterator<ProgressNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            ProgressRow row = ProgressRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<ProgressNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    public BatchingVisitableView<ProgressRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<ProgressRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, ProgressRowResult>() {
            @Override
            public ProgressRowResult apply(RowResult<byte[]> input) {
                return ProgressRowResult.of(input);
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
    static String __CLASS_HASH = "eKtG1Mmp/m2dWO7A9phS1A==";
}
