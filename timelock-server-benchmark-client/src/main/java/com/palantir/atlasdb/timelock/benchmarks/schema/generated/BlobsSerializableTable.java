package com.palantir.atlasdb.timelock.benchmarks.schema.generated;

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
public final class BlobsSerializableTable implements
        AtlasDbMutablePersistentTable<BlobsSerializableTable.BlobsSerializableRow,
                                         BlobsSerializableTable.BlobsSerializableNamedColumnValue<?>,
                                         BlobsSerializableTable.BlobsSerializableRowResult>,
        AtlasDbNamedMutableTable<BlobsSerializableTable.BlobsSerializableRow,
                                    BlobsSerializableTable.BlobsSerializableNamedColumnValue<?>,
                                    BlobsSerializableTable.BlobsSerializableRowResult> {
    private final Transaction t;
    private final List<BlobsSerializableTrigger> triggers;
    private final static String rawTableName = "BlobsSerializable";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(BlobsSerializableNamedColumn.values());

    static BlobsSerializableTable of(Transaction t, Namespace namespace) {
        return new BlobsSerializableTable(t, namespace, ImmutableList.<BlobsSerializableTrigger>of());
    }

    static BlobsSerializableTable of(Transaction t, Namespace namespace, BlobsSerializableTrigger trigger, BlobsSerializableTrigger... triggers) {
        return new BlobsSerializableTable(t, namespace, ImmutableList.<BlobsSerializableTrigger>builder().add(trigger).add(triggers).build());
    }

    static BlobsSerializableTable of(Transaction t, Namespace namespace, List<BlobsSerializableTrigger> triggers) {
        return new BlobsSerializableTable(t, namespace, triggers);
    }

    private BlobsSerializableTable(Transaction t, Namespace namespace, List<BlobsSerializableTrigger> triggers) {
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
     * BlobsSerializableRow {
     *   {@literal byte[] key};
     * }
     * </pre>
     */
    public static final class BlobsSerializableRow implements Persistable, Comparable<BlobsSerializableRow> {
        private final byte[] key;

        public static BlobsSerializableRow of(byte[] key) {
            return new BlobsSerializableRow(key);
        }

        private BlobsSerializableRow(byte[] key) {
            this.key = key;
        }

        public byte[] getKey() {
            return key;
        }

        public static Function<BlobsSerializableRow, byte[]> getKeyFun() {
            return new Function<BlobsSerializableRow, byte[]>() {
                @Override
                public byte[] apply(BlobsSerializableRow row) {
                    return row.key;
                }
            };
        }

        public static Function<byte[], BlobsSerializableRow> fromKeyFun() {
            return new Function<byte[], BlobsSerializableRow>() {
                @Override
                public BlobsSerializableRow apply(byte[] row) {
                    return BlobsSerializableRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] keyBytes = key;
            return EncodingUtils.add(keyBytes);
        }

        public static final Hydrator<BlobsSerializableRow> BYTES_HYDRATOR = new Hydrator<BlobsSerializableRow>() {
            @Override
            public BlobsSerializableRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] key = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new BlobsSerializableRow(key);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("key", key)
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
            BlobsSerializableRow other = (BlobsSerializableRow) obj;
            return Arrays.equals(key, other.key);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        @Override
        public int compareTo(BlobsSerializableRow o) {
            return ComparisonChain.start()
                .compare(this.key, o.key, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface BlobsSerializableNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Data implements BlobsSerializableNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Data of(byte[] value) {
            return new Data(value);
        }

        private Data(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "data";
        }

        @Override
        public String getShortColumnName() {
            return "d";
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
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<Data> BYTES_HYDRATOR = new Hydrator<Data>() {
            @Override
            public Data hydrateFromBytes(byte[] bytes) {
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

    public interface BlobsSerializableTrigger {
        public void putBlobsSerializable(Multimap<BlobsSerializableRow, ? extends BlobsSerializableNamedColumnValue<?>> newRows);
    }

    public static final class BlobsSerializableRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static BlobsSerializableRowResult of(RowResult<byte[]> row) {
            return new BlobsSerializableRowResult(row);
        }

        private BlobsSerializableRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public BlobsSerializableRow getRowName() {
            return BlobsSerializableRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<BlobsSerializableRowResult, BlobsSerializableRow> getRowNameFun() {
            return new Function<BlobsSerializableRowResult, BlobsSerializableRow>() {
                @Override
                public BlobsSerializableRow apply(BlobsSerializableRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, BlobsSerializableRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, BlobsSerializableRowResult>() {
                @Override
                public BlobsSerializableRowResult apply(RowResult<byte[]> rowResult) {
                    return new BlobsSerializableRowResult(rowResult);
                }
            };
        }

        public boolean hasData() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public byte[] getData() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            Data value = Data.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<BlobsSerializableRowResult, byte[]> getDataFun() {
            return new Function<BlobsSerializableRowResult, byte[]>() {
                @Override
                public byte[] apply(BlobsSerializableRowResult rowResult) {
                    return rowResult.getData();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Data", getData())
                .toString();
        }
    }

    public enum BlobsSerializableNamedColumn {
        DATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        };

        public abstract byte[] getShortName();

        public static Function<BlobsSerializableNamedColumn, byte[]> toShortName() {
            return new Function<BlobsSerializableNamedColumn, byte[]>() {
                @Override
                public byte[] apply(BlobsSerializableNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<BlobsSerializableNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, BlobsSerializableNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(BlobsSerializableNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends BlobsSerializableNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends BlobsSerializableNamedColumnValue<?>>>builder()
                .put("d", Data.BYTES_HYDRATOR)
                .build();

    public Map<BlobsSerializableRow, byte[]> getDatas(Collection<BlobsSerializableRow> rows) {
        Map<Cell, BlobsSerializableRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (BlobsSerializableRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<BlobsSerializableRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Data.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putData(BlobsSerializableRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Data.of(value)));
    }

    public void putData(Map<BlobsSerializableRow, byte[]> map) {
        Map<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<BlobsSerializableRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Data.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<BlobsSerializableRow, ? extends BlobsSerializableNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (BlobsSerializableTrigger trigger : triggers) {
            trigger.putBlobsSerializable(rows);
        }
    }

    public void deleteData(BlobsSerializableRow row) {
        deleteData(ImmutableSet.of(row));
    }

    public void deleteData(Iterable<BlobsSerializableRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(BlobsSerializableRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<BlobsSerializableRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        t.delete(tableRef, cells);
    }

    public Optional<BlobsSerializableRowResult> getRow(BlobsSerializableRow row) {
        return getRow(row, allColumns);
    }

    public Optional<BlobsSerializableRowResult> getRow(BlobsSerializableRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(BlobsSerializableRowResult.of(rowResult));
        }
    }

    @Override
    public List<BlobsSerializableRowResult> getRows(Iterable<BlobsSerializableRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<BlobsSerializableRowResult> getRows(Iterable<BlobsSerializableRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<BlobsSerializableRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(BlobsSerializableRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<BlobsSerializableNamedColumnValue<?>> getRowColumns(BlobsSerializableRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<BlobsSerializableNamedColumnValue<?>> getRowColumns(BlobsSerializableRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<BlobsSerializableNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> getRowsMultimap(Iterable<BlobsSerializableRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> getRowsMultimap(Iterable<BlobsSerializableRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> getRowsMultimapInternal(Iterable<BlobsSerializableRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            BlobsSerializableRow row = BlobsSerializableRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<BlobsSerializableRow, BatchingVisitable<BlobsSerializableNamedColumnValue<?>>> getRowsColumnRange(Iterable<BlobsSerializableRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<BlobsSerializableRow, BatchingVisitable<BlobsSerializableNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            BlobsSerializableRow row = BlobsSerializableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<BlobsSerializableNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<BlobsSerializableRow, BlobsSerializableNamedColumnValue<?>>> getRowsColumnRange(Iterable<BlobsSerializableRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            BlobsSerializableRow row = BlobsSerializableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            BlobsSerializableNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<BlobsSerializableRow, Iterator<BlobsSerializableNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<BlobsSerializableRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<BlobsSerializableRow, Iterator<BlobsSerializableNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            BlobsSerializableRow row = BlobsSerializableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<BlobsSerializableNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
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

    public BatchingVisitableView<BlobsSerializableRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<BlobsSerializableRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, BlobsSerializableRowResult>() {
            @Override
            public BlobsSerializableRowResult apply(RowResult<byte[]> input) {
                return BlobsSerializableRowResult.of(input);
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
    static String __CLASS_HASH = "kEu4r6ZeyQGSln9x7e/bKg==";
}
