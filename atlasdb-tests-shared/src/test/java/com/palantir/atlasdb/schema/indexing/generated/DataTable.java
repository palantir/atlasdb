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
public final class DataTable implements
        AtlasDbMutablePersistentTable<DataTable.DataRow,
                                         DataTable.DataNamedColumnValue<?>,
                                         DataTable.DataRowResult>,
        AtlasDbNamedMutableTable<DataTable.DataRow,
                                    DataTable.DataNamedColumnValue<?>,
                                    DataTable.DataRowResult> {
    private final Transaction t;
    private final List<DataTrigger> triggers;
    private final static String rawTableName = "data";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(DataNamedColumn.values());

    static DataTable of(Transaction t, Namespace namespace) {
        return new DataTable(t, namespace, ImmutableList.<DataTrigger>of());
    }

    static DataTable of(Transaction t, Namespace namespace, DataTrigger trigger, DataTrigger... triggers) {
        return new DataTable(t, namespace, ImmutableList.<DataTrigger>builder().add(trigger).add(triggers).build());
    }

    static DataTable of(Transaction t, Namespace namespace, List<DataTrigger> triggers) {
        return new DataTable(t, namespace, triggers);
    }

    private DataTable(Transaction t, Namespace namespace, List<DataTrigger> triggers) {
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
     * DataRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class DataRow implements Persistable, Comparable<DataRow> {
        private final long id;

        public static DataRow of(long id) {
            return new DataRow(id);
        }

        private DataRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<DataRow, Long> getIdFun() {
            return new Function<DataRow, Long>() {
                @Override
                public Long apply(DataRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, DataRow> fromIdFun() {
            return new Function<Long, DataRow>() {
                @Override
                public DataRow apply(Long row) {
                    return DataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<DataRow> BYTES_HYDRATOR = new Hydrator<DataRow>() {
            @Override
            public DataRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                _index += 8;
                return new DataRow(id);
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
            DataRow other = (DataRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(DataRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface DataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Value implements DataNamedColumnValue<Long> {
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

    public interface DataTrigger {
        public void putData(Multimap<DataRow, ? extends DataNamedColumnValue<?>> newRows);
    }

    public static final class DataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static DataRowResult of(RowResult<byte[]> row) {
            return new DataRowResult(row);
        }

        private DataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public DataRow getRowName() {
            return DataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<DataRowResult, DataRow> getRowNameFun() {
            return new Function<DataRowResult, DataRow>() {
                @Override
                public DataRow apply(DataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, DataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, DataRowResult>() {
                @Override
                public DataRowResult apply(RowResult<byte[]> rowResult) {
                    return new DataRowResult(rowResult);
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

        public static Function<DataRowResult, Long> getValueFun() {
            return new Function<DataRowResult, Long>() {
                @Override
                public Long apply(DataRowResult rowResult) {
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

    public enum DataNamedColumn {
        VALUE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("v");
            }
        };

        public abstract byte[] getShortName();

        public static Function<DataNamedColumn, byte[]> toShortName() {
            return new Function<DataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(DataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<DataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, DataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(DataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends DataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends DataNamedColumnValue<?>>>builder()
                .put("v", Value.BYTES_HYDRATOR)
                .build();

    public Map<DataRow, Long> getValues(Collection<DataRow> rows) {
        Map<Cell, DataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (DataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("v")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<DataRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Value.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putValue(DataRow row, Long value) {
        put(ImmutableMultimap.of(row, Value.of(value)));
    }

    public void putValue(Map<DataRow, Long> map) {
        Map<DataRow, DataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<DataRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Value.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<DataRow, ? extends DataNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        Multimap<DataRow, DataNamedColumnValue<?>> affectedCells = getAffectedCells(rows);
        deleteIndex1Idx(affectedCells);
        deleteIndex2Idx(affectedCells);
        deleteIndex3Idx(affectedCells);
        deleteIndex4Idx(affectedCells);
        for (Entry<DataRow, ? extends DataNamedColumnValue<?>> e : rows.entries()) {
            if (e.getValue() instanceof Value)
            {
                Value col = (Value) e.getValue();
                {
                    DataRow row = e.getKey();
                    Index1IdxTable table = Index1IdxTable.of(this);
                    long value = col.getValue();
                    long id = row.getId();
                    Index1IdxTable.Index1IdxRow indexRow = Index1IdxTable.Index1IdxRow.of(value);
                    Index1IdxTable.Index1IdxColumn indexCol = Index1IdxTable.Index1IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    Index1IdxTable.Index1IdxColumnValue indexColVal = Index1IdxTable.Index1IdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
            if (e.getValue() instanceof Value)
            {
                Value col = (Value) e.getValue();
                {
                    DataRow row = e.getKey();
                    Index2IdxTable table = Index2IdxTable.of(this);
                    long value = col.getValue();
                    long id = row.getId();
                    Index2IdxTable.Index2IdxRow indexRow = Index2IdxTable.Index2IdxRow.of(value, id);
                    Index2IdxTable.Index2IdxColumn indexCol = Index2IdxTable.Index2IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                    Index2IdxTable.Index2IdxColumnValue indexColVal = Index2IdxTable.Index2IdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
            if (e.getValue() instanceof Value)
            {
                Value col = (Value) e.getValue();
                {
                    DataRow row = e.getKey();
                    Index3IdxTable table = Index3IdxTable.of(this);
                    Iterable<Long> valueIterable = ImmutableList.of(col.getValue());
                    for (long value : valueIterable) {
                        Index3IdxTable.Index3IdxRow indexRow = Index3IdxTable.Index3IdxRow.of(value);
                        Index3IdxTable.Index3IdxColumn indexCol = Index3IdxTable.Index3IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                        Index3IdxTable.Index3IdxColumnValue indexColVal = Index3IdxTable.Index3IdxColumnValue.of(indexCol, 0L);
                        table.put(indexRow, indexColVal);
                    }
                }
            }
            if (e.getValue() instanceof Value)
            {
                Value col = (Value) e.getValue();
                {
                    DataRow row = e.getKey();
                    Index4IdxTable table = Index4IdxTable.of(this);
                    Iterable<Long> value1Iterable = ImmutableList.of(col.getValue());
                    Iterable<Long> value2Iterable = ImmutableList.of(col.getValue());
                    for (long value1 : value1Iterable) {
                        for (long value2 : value2Iterable) {
                            Index4IdxTable.Index4IdxRow indexRow = Index4IdxTable.Index4IdxRow.of(value1, value2);
                            Index4IdxTable.Index4IdxColumn indexCol = Index4IdxTable.Index4IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                            Index4IdxTable.Index4IdxColumnValue indexColVal = Index4IdxTable.Index4IdxColumnValue.of(indexCol, 0L);
                            table.put(indexRow, indexColVal);
                        }
                    }
                }
            }
        }
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (DataTrigger trigger : triggers) {
            trigger.putData(rows);
        }
    }

    public void deleteValue(DataRow row) {
        deleteValue(ImmutableSet.of(row));
    }

    public void deleteValue(Iterable<DataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("v");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        Map<Cell, byte[]> results = t.get(tableRef, cells);
        deleteIndex1IdxRaw(results);
        deleteIndex2IdxRaw(results);
        deleteIndex3IdxRaw(results);
        deleteIndex4IdxRaw(results);
        t.delete(tableRef, cells);
    }

    private void deleteIndex1IdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Value col = (Value) shortNameToHydrator.get("v").hydrateFromBytes(result.getValue());
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long value = col.getValue();
            long id = row.getId();
            Index1IdxTable.Index1IdxRow indexRow = Index1IdxTable.Index1IdxRow.of(value);
            Index1IdxTable.Index1IdxColumn indexCol = Index1IdxTable.Index1IdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index1_idx"), indexCells);
    }

    private void deleteIndex2IdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Value col = (Value) shortNameToHydrator.get("v").hydrateFromBytes(result.getValue());
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long value = col.getValue();
            long id = row.getId();
            Index2IdxTable.Index2IdxRow indexRow = Index2IdxTable.Index2IdxRow.of(value, id);
            Index2IdxTable.Index2IdxColumn indexCol = Index2IdxTable.Index2IdxColumn.of(row.persistToBytes(), col.persistColumnName());
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index2_idx"), indexCells);
    }

    private void deleteIndex3IdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Value col = (Value) shortNameToHydrator.get("v").hydrateFromBytes(result.getValue());
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            Iterable<Long> valueIterable = ImmutableList.of(col.getValue());
            for (long value : valueIterable) {
                Index3IdxTable.Index3IdxRow indexRow = Index3IdxTable.Index3IdxRow.of(value);
                Index3IdxTable.Index3IdxColumn indexCol = Index3IdxTable.Index3IdxColumn.of(row.persistToBytes(), col.persistColumnName());
                indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index3_idx"), indexCells);
    }

    private void deleteIndex4IdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Value col = (Value) shortNameToHydrator.get("v").hydrateFromBytes(result.getValue());
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            Iterable<Long> value1Iterable = ImmutableList.of(col.getValue());
            Iterable<Long> value2Iterable = ImmutableList.of(col.getValue());
            for (long value1 : value1Iterable) {
                for (long value2 : value2Iterable) {
                    Index4IdxTable.Index4IdxRow indexRow = Index4IdxTable.Index4IdxRow.of(value1, value2);
                    Index4IdxTable.Index4IdxColumn indexCol = Index4IdxTable.Index4IdxColumn.of(row.persistToBytes(), col.persistColumnName());
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index4_idx"), indexCells);
    }

    @Override
    public void delete(DataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<DataRow> rows) {
        Multimap<DataRow, DataNamedColumnValue<?>> result = getRowsMultimap(rows);
        deleteIndex1Idx(result);
        deleteIndex2Idx(result);
        deleteIndex3Idx(result);
        deleteIndex4Idx(result);
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("v")));
        t.delete(tableRef, cells);
    }

    public Optional<DataRowResult> getRow(DataRow row) {
        return getRow(row, allColumns);
    }

    public Optional<DataRowResult> getRow(DataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(DataRowResult.of(rowResult));
        }
    }

    @Override
    public List<DataRowResult> getRows(Iterable<DataRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<DataRowResult> getRows(Iterable<DataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<DataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(DataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<DataNamedColumnValue<?>> getRowColumns(DataRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<DataNamedColumnValue<?>> getRowColumns(DataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<DataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<DataRow, DataNamedColumnValue<?>> getRowsMultimap(Iterable<DataRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<DataRow, DataNamedColumnValue<?>> getRowsMultimap(Iterable<DataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<DataRow, DataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<DataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<DataRow, DataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<DataRow, DataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<DataRow, BatchingVisitable<DataNamedColumnValue<?>>> getRowsColumnRange(Iterable<DataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataRow, BatchingVisitable<DataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<DataNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<DataRow, DataNamedColumnValue<?>>> getRowsColumnRange(Iterable<DataRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            DataNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<DataRow, Iterator<DataNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<DataRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataRow, Iterator<DataNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataRow row = DataRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<DataNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private Multimap<DataRow, DataNamedColumnValue<?>> getAffectedCells(Multimap<DataRow, ? extends DataNamedColumnValue<?>> rows) {
        Multimap<DataRow, DataNamedColumnValue<?>> oldData = getRowsMultimap(rows.keySet());
        Multimap<DataRow, DataNamedColumnValue<?>> cellsAffected = ArrayListMultimap.create();
        for (DataRow row : oldData.keySet()) {
            Set<String> columns = new HashSet<String>();
            for (DataNamedColumnValue<?> v : rows.get(row)) {
                columns.add(v.getColumnName());
            }
            for (DataNamedColumnValue<?> v : oldData.get(row)) {
                if (columns.contains(v.getColumnName())) {
                    cellsAffected.put(row, v);
                }
            }
        }
        return cellsAffected;
    }

    private void deleteIndex1Idx(Multimap<DataRow, DataNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<DataRow, DataNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Value) {
                Value col = (Value) e.getValue();{
                    DataRow row = e.getKey();
                    long value = col.getValue();
                    long id = row.getId();
                    Index1IdxTable.Index1IdxRow indexRow = Index1IdxTable.Index1IdxRow.of(value);
                    Index1IdxTable.Index1IdxColumn indexCol = Index1IdxTable.Index1IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index1_idx"), indexCells.build());
    }

    private void deleteIndex2Idx(Multimap<DataRow, DataNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<DataRow, DataNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Value) {
                Value col = (Value) e.getValue();{
                    DataRow row = e.getKey();
                    long value = col.getValue();
                    long id = row.getId();
                    Index2IdxTable.Index2IdxRow indexRow = Index2IdxTable.Index2IdxRow.of(value, id);
                    Index2IdxTable.Index2IdxColumn indexCol = Index2IdxTable.Index2IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index2_idx"), indexCells.build());
    }

    private void deleteIndex3Idx(Multimap<DataRow, DataNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<DataRow, DataNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Value) {
                Value col = (Value) e.getValue();{
                    DataRow row = e.getKey();
                    Iterable<Long> valueIterable = ImmutableList.of(col.getValue());
                    for (long value : valueIterable) {
                        Index3IdxTable.Index3IdxRow indexRow = Index3IdxTable.Index3IdxRow.of(value);
                        Index3IdxTable.Index3IdxColumn indexCol = Index3IdxTable.Index3IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                        indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                    }
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index3_idx"), indexCells.build());
    }

    private void deleteIndex4Idx(Multimap<DataRow, DataNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<DataRow, DataNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Value) {
                Value col = (Value) e.getValue();{
                    DataRow row = e.getKey();
                    Iterable<Long> value1Iterable = ImmutableList.of(col.getValue());
                    Iterable<Long> value2Iterable = ImmutableList.of(col.getValue());
                    for (long value1 : value1Iterable) {
                        for (long value2 : value2Iterable) {
                            Index4IdxTable.Index4IdxRow indexRow = Index4IdxTable.Index4IdxRow.of(value1, value2);
                            Index4IdxTable.Index4IdxColumn indexCol = Index4IdxTable.Index4IdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName());
                            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                        }
                    }
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.index4_idx"), indexCells.build());
    }

    private ColumnSelection optimizeColumnSelection(ColumnSelection columns) {
        if (columns.allColumnsSelected()) {
            return allColumns;
        }
        return columns;
    }

    public BatchingVisitableView<DataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<DataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, DataRowResult>() {
            @Override
            public DataRowResult apply(RowResult<byte[]> input) {
                return DataRowResult.of(input);
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

    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class Index1IdxTable implements
            AtlasDbDynamicMutablePersistentTable<Index1IdxTable.Index1IdxRow,
                                                    Index1IdxTable.Index1IdxColumn,
                                                    Index1IdxTable.Index1IdxColumnValue,
                                                    Index1IdxTable.Index1IdxRowResult> {
        private final Transaction t;
        private final List<Index1IdxTrigger> triggers;
        private final static String rawTableName = "index1_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static Index1IdxTable of(DataTable table) {
            return new Index1IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index1IdxTrigger>of());
        }

        public static Index1IdxTable of(DataTable table, Index1IdxTrigger trigger, Index1IdxTrigger... triggers) {
            return new Index1IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index1IdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static Index1IdxTable of(DataTable table, List<Index1IdxTrigger> triggers) {
            return new Index1IdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private Index1IdxTable(Transaction t, Namespace namespace, List<Index1IdxTrigger> triggers) {
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
         * Index1IdxRow {
         *   {@literal Long value};
         * }
         * </pre>
         */
        public static final class Index1IdxRow implements Persistable, Comparable<Index1IdxRow> {
            private final long value;

            public static Index1IdxRow of(long value) {
                return new Index1IdxRow(value);
            }

            private Index1IdxRow(long value) {
                this.value = value;
            }

            public long getValue() {
                return value;
            }

            public static Function<Index1IdxRow, Long> getValueFun() {
                return new Function<Index1IdxRow, Long>() {
                    @Override
                    public Long apply(Index1IdxRow row) {
                        return row.value;
                    }
                };
            }

            public static Function<Long, Index1IdxRow> fromValueFun() {
                return new Function<Long, Index1IdxRow>() {
                    @Override
                    public Index1IdxRow apply(Long row) {
                        return Index1IdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] valueBytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
                return EncodingUtils.add(valueBytes);
            }

            public static final Hydrator<Index1IdxRow> BYTES_HYDRATOR = new Hydrator<Index1IdxRow>() {
                @Override
                public Index1IdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long value = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    return new Index1IdxRow(value);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("value", value)
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
                Index1IdxRow other = (Index1IdxRow) obj;
                return Objects.equals(value, other.value);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(value);
            }

            @Override
            public int compareTo(Index1IdxRow o) {
                return ComparisonChain.start()
                    .compare(this.value, o.value)
                    .result();
            }
        }

        /**
         * <pre>
         * Index1IdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal Long id};
         * }
         * </pre>
         */
        public static final class Index1IdxColumn implements Persistable, Comparable<Index1IdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final long id;

            public static Index1IdxColumn of(byte[] rowName, byte[] columnName, long id) {
                return new Index1IdxColumn(rowName, columnName, id);
            }

            private Index1IdxColumn(byte[] rowName, byte[] columnName, long id) {
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

            public static Function<Index1IdxColumn, byte[]> getRowNameFun() {
                return new Function<Index1IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index1IdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<Index1IdxColumn, byte[]> getColumnNameFun() {
                return new Function<Index1IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index1IdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<Index1IdxColumn, Long> getIdFun() {
                return new Function<Index1IdxColumn, Long>() {
                    @Override
                    public Long apply(Index1IdxColumn row) {
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

            public static final Hydrator<Index1IdxColumn> BYTES_HYDRATOR = new Hydrator<Index1IdxColumn>() {
                @Override
                public Index1IdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    Long id = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    return new Index1IdxColumn(rowName, columnName, id);
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
                Index1IdxColumn other = (Index1IdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(Index1IdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface Index1IdxTrigger {
            public void putIndex1Idx(Multimap<Index1IdxRow, ? extends Index1IdxColumnValue> newRows);
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
        public static final class Index1IdxColumnValue implements ColumnValue<Long> {
            private final Index1IdxColumn columnName;
            private final Long value;

            public static Index1IdxColumnValue of(Index1IdxColumn columnName, Long value) {
                return new Index1IdxColumnValue(columnName, value);
            }

            private Index1IdxColumnValue(Index1IdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public Index1IdxColumn getColumnName() {
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

            public static Function<Index1IdxColumnValue, Index1IdxColumn> getColumnNameFun() {
                return new Function<Index1IdxColumnValue, Index1IdxColumn>() {
                    @Override
                    public Index1IdxColumn apply(Index1IdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<Index1IdxColumnValue, Long> getValueFun() {
                return new Function<Index1IdxColumnValue, Long>() {
                    @Override
                    public Long apply(Index1IdxColumnValue columnValue) {
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

        public static final class Index1IdxRowResult implements TypedRowResult {
            private final Index1IdxRow rowName;
            private final ImmutableSet<Index1IdxColumnValue> columnValues;

            public static Index1IdxRowResult of(RowResult<byte[]> rowResult) {
                Index1IdxRow rowName = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<Index1IdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = Index1IdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(Index1IdxColumnValue.of(col, value));
                }
                return new Index1IdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private Index1IdxRowResult(Index1IdxRow rowName, ImmutableSet<Index1IdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public Index1IdxRow getRowName() {
                return rowName;
            }

            public Set<Index1IdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<Index1IdxRowResult, Index1IdxRow> getRowNameFun() {
                return new Function<Index1IdxRowResult, Index1IdxRow>() {
                    @Override
                    public Index1IdxRow apply(Index1IdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<Index1IdxRowResult, ImmutableSet<Index1IdxColumnValue>> getColumnValuesFun() {
                return new Function<Index1IdxRowResult, ImmutableSet<Index1IdxColumnValue>>() {
                    @Override
                    public ImmutableSet<Index1IdxColumnValue> apply(Index1IdxRowResult rowResult) {
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
        public void delete(Index1IdxRow row, Index1IdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<Index1IdxRow> rows) {
            Multimap<Index1IdxRow, Index1IdxColumn> toRemove = HashMultimap.create();
            Multimap<Index1IdxRow, Index1IdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<Index1IdxRow, Index1IdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<Index1IdxRow, Index1IdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(Index1IdxRow rowName, Iterable<Index1IdxColumnValue> values) {
            put(ImmutableMultimap.<Index1IdxRow, Index1IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Index1IdxRow rowName, Index1IdxColumnValue... values) {
            put(ImmutableMultimap.<Index1IdxRow, Index1IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<Index1IdxRow, ? extends Index1IdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (Index1IdxTrigger trigger : triggers) {
                trigger.putIndex1Idx(values);
            }
        }

        @Override
        public void touch(Multimap<Index1IdxRow, Index1IdxColumn> values) {
            Multimap<Index1IdxRow, Index1IdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<Index1IdxRow, Index1IdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<Index1IdxRow, Index1IdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<Index1IdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(Index1IdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<Index1IdxRow, Index1IdxColumnValue> get(Multimap<Index1IdxRow, Index1IdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<Index1IdxRow, Index1IdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    Index1IdxRow row = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = Index1IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index1IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<Index1IdxColumnValue> getRowColumns(Index1IdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<Index1IdxColumnValue> getRowColumns(Index1IdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<Index1IdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index1IdxColumnValue.hydrateValue(e.getValue());
                    ret.add(Index1IdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<Index1IdxRow, Index1IdxColumnValue> getRowsMultimap(Iterable<Index1IdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<Index1IdxRow, Index1IdxColumnValue> getRowsMultimap(Iterable<Index1IdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<Index1IdxRow, Index1IdxColumnValue> getRowsMultimapInternal(Iterable<Index1IdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<Index1IdxRow, Index1IdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<Index1IdxRow, Index1IdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                Index1IdxRow row = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index1IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index1IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<Index1IdxRow, BatchingVisitable<Index1IdxColumnValue>> getRowsColumnRange(Iterable<Index1IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index1IdxRow, BatchingVisitable<Index1IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index1IdxRow row = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<Index1IdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index1IdxColumnValue.hydrateValue(result.getValue());
                    return Index1IdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<Index1IdxRow, Index1IdxColumnValue>> getRowsColumnRange(Iterable<Index1IdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                Index1IdxRow row = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = Index1IdxColumnValue.hydrateValue(e.getValue());
                Index1IdxColumnValue colValue = Index1IdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<Index1IdxRow, Iterator<Index1IdxColumnValue>> getRowsColumnRangeIterator(Iterable<Index1IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index1IdxRow, Iterator<Index1IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index1IdxRow row = Index1IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<Index1IdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    Index1IdxColumn col = Index1IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index1IdxColumnValue.hydrateValue(result.getValue());
                    return Index1IdxColumnValue.of(col, val);
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

        public BatchingVisitableView<Index1IdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, Index1IdxRowResult>() {
                @Override
                public Index1IdxRowResult apply(RowResult<byte[]> input) {
                    return Index1IdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<Index1IdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<Index1IdxRowResult>>() {
                @Override
                public BatchingVisitable<Index1IdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, Index1IdxRowResult>() {
                        @Override
                        public Index1IdxRowResult apply(RowResult<byte[]> row) {
                            return Index1IdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<Index1IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index1IdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<Index1IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index1IdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<Index1IdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, Index1IdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<Index1IdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<Index1IdxRowResult> rowResults) {
                    Multimap<Index1IdxRow, Index1IdxColumn> toRemove = HashMultimap.create();
                    for (Index1IdxRowResult rowResult : rowResults) {
                        for (Index1IdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class Index2IdxTable implements
            AtlasDbDynamicMutablePersistentTable<Index2IdxTable.Index2IdxRow,
                                                    Index2IdxTable.Index2IdxColumn,
                                                    Index2IdxTable.Index2IdxColumnValue,
                                                    Index2IdxTable.Index2IdxRowResult> {
        private final Transaction t;
        private final List<Index2IdxTrigger> triggers;
        private final static String rawTableName = "index2_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static Index2IdxTable of(DataTable table) {
            return new Index2IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index2IdxTrigger>of());
        }

        public static Index2IdxTable of(DataTable table, Index2IdxTrigger trigger, Index2IdxTrigger... triggers) {
            return new Index2IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index2IdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static Index2IdxTable of(DataTable table, List<Index2IdxTrigger> triggers) {
            return new Index2IdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private Index2IdxTable(Transaction t, Namespace namespace, List<Index2IdxTrigger> triggers) {
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
         * Index2IdxRow {
         *   {@literal Long value};
         *   {@literal Long id};
         * }
         * </pre>
         */
        public static final class Index2IdxRow implements Persistable, Comparable<Index2IdxRow> {
            private final long value;
            private final long id;

            public static Index2IdxRow of(long value, long id) {
                return new Index2IdxRow(value, id);
            }

            private Index2IdxRow(long value, long id) {
                this.value = value;
                this.id = id;
            }

            public long getValue() {
                return value;
            }

            public long getId() {
                return id;
            }

            public static Function<Index2IdxRow, Long> getValueFun() {
                return new Function<Index2IdxRow, Long>() {
                    @Override
                    public Long apply(Index2IdxRow row) {
                        return row.value;
                    }
                };
            }

            public static Function<Index2IdxRow, Long> getIdFun() {
                return new Function<Index2IdxRow, Long>() {
                    @Override
                    public Long apply(Index2IdxRow row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] valueBytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
                byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
                return EncodingUtils.add(valueBytes, idBytes);
            }

            public static final Hydrator<Index2IdxRow> BYTES_HYDRATOR = new Hydrator<Index2IdxRow>() {
                @Override
                public Index2IdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long value = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    Long id = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    return new Index2IdxRow(value, id);
                }
            };

            public static RangeRequest.Builder createPrefixRange(long value) {
                byte[] valueBytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
                return RangeRequest.builder().prefixRange(EncodingUtils.add(valueBytes));
            }

            public static Prefix prefix(long value) {
                byte[] valueBytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
                return new Prefix(EncodingUtils.add(valueBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("value", value)
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
                Index2IdxRow other = (Index2IdxRow) obj;
                return Objects.equals(value, other.value) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ value, id });
            }

            @Override
            public int compareTo(Index2IdxRow o) {
                return ComparisonChain.start()
                    .compare(this.value, o.value)
                    .compare(this.id, o.id)
                    .result();
            }
        }

        /**
         * <pre>
         * Index2IdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * </pre>
         */
        public static final class Index2IdxColumn implements Persistable, Comparable<Index2IdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;

            public static Index2IdxColumn of(byte[] rowName, byte[] columnName) {
                return new Index2IdxColumn(rowName, columnName);
            }

            private Index2IdxColumn(byte[] rowName, byte[] columnName) {
                this.rowName = rowName;
                this.columnName = columnName;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public static Function<Index2IdxColumn, byte[]> getRowNameFun() {
                return new Function<Index2IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index2IdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<Index2IdxColumn, byte[]> getColumnNameFun() {
                return new Function<Index2IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index2IdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return EncodingUtils.add(rowNameBytes, columnNameBytes);
            }

            public static final Hydrator<Index2IdxColumn> BYTES_HYDRATOR = new Hydrator<Index2IdxColumn>() {
                @Override
                public Index2IdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    return new Index2IdxColumn(rowName, columnName);
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

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                Index2IdxColumn other = (Index2IdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName });
            }

            @Override
            public int compareTo(Index2IdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .result();
            }
        }

        public interface Index2IdxTrigger {
            public void putIndex2Idx(Multimap<Index2IdxRow, ? extends Index2IdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class Index2IdxColumnValue implements ColumnValue<Long> {
            private final Index2IdxColumn columnName;
            private final Long value;

            public static Index2IdxColumnValue of(Index2IdxColumn columnName, Long value) {
                return new Index2IdxColumnValue(columnName, value);
            }

            private Index2IdxColumnValue(Index2IdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public Index2IdxColumn getColumnName() {
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

            public static Function<Index2IdxColumnValue, Index2IdxColumn> getColumnNameFun() {
                return new Function<Index2IdxColumnValue, Index2IdxColumn>() {
                    @Override
                    public Index2IdxColumn apply(Index2IdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<Index2IdxColumnValue, Long> getValueFun() {
                return new Function<Index2IdxColumnValue, Long>() {
                    @Override
                    public Long apply(Index2IdxColumnValue columnValue) {
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

        public static final class Index2IdxRowResult implements TypedRowResult {
            private final Index2IdxRow rowName;
            private final ImmutableSet<Index2IdxColumnValue> columnValues;

            public static Index2IdxRowResult of(RowResult<byte[]> rowResult) {
                Index2IdxRow rowName = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<Index2IdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = Index2IdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(Index2IdxColumnValue.of(col, value));
                }
                return new Index2IdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private Index2IdxRowResult(Index2IdxRow rowName, ImmutableSet<Index2IdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public Index2IdxRow getRowName() {
                return rowName;
            }

            public Set<Index2IdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<Index2IdxRowResult, Index2IdxRow> getRowNameFun() {
                return new Function<Index2IdxRowResult, Index2IdxRow>() {
                    @Override
                    public Index2IdxRow apply(Index2IdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<Index2IdxRowResult, ImmutableSet<Index2IdxColumnValue>> getColumnValuesFun() {
                return new Function<Index2IdxRowResult, ImmutableSet<Index2IdxColumnValue>>() {
                    @Override
                    public ImmutableSet<Index2IdxColumnValue> apply(Index2IdxRowResult rowResult) {
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
        public void delete(Index2IdxRow row, Index2IdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<Index2IdxRow> rows) {
            Multimap<Index2IdxRow, Index2IdxColumn> toRemove = HashMultimap.create();
            Multimap<Index2IdxRow, Index2IdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<Index2IdxRow, Index2IdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<Index2IdxRow, Index2IdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(Index2IdxRow rowName, Iterable<Index2IdxColumnValue> values) {
            put(ImmutableMultimap.<Index2IdxRow, Index2IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Index2IdxRow rowName, Index2IdxColumnValue... values) {
            put(ImmutableMultimap.<Index2IdxRow, Index2IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<Index2IdxRow, ? extends Index2IdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (Index2IdxTrigger trigger : triggers) {
                trigger.putIndex2Idx(values);
            }
        }

        @Override
        public void touch(Multimap<Index2IdxRow, Index2IdxColumn> values) {
            Multimap<Index2IdxRow, Index2IdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<Index2IdxRow, Index2IdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<Index2IdxRow, Index2IdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<Index2IdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(Index2IdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<Index2IdxRow, Index2IdxColumnValue> get(Multimap<Index2IdxRow, Index2IdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<Index2IdxRow, Index2IdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    Index2IdxRow row = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = Index2IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index2IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<Index2IdxColumnValue> getRowColumns(Index2IdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<Index2IdxColumnValue> getRowColumns(Index2IdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<Index2IdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index2IdxColumnValue.hydrateValue(e.getValue());
                    ret.add(Index2IdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<Index2IdxRow, Index2IdxColumnValue> getRowsMultimap(Iterable<Index2IdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<Index2IdxRow, Index2IdxColumnValue> getRowsMultimap(Iterable<Index2IdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<Index2IdxRow, Index2IdxColumnValue> getRowsMultimapInternal(Iterable<Index2IdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<Index2IdxRow, Index2IdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<Index2IdxRow, Index2IdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                Index2IdxRow row = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index2IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index2IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<Index2IdxRow, BatchingVisitable<Index2IdxColumnValue>> getRowsColumnRange(Iterable<Index2IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index2IdxRow, BatchingVisitable<Index2IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index2IdxRow row = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<Index2IdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index2IdxColumnValue.hydrateValue(result.getValue());
                    return Index2IdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<Index2IdxRow, Index2IdxColumnValue>> getRowsColumnRange(Iterable<Index2IdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                Index2IdxRow row = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = Index2IdxColumnValue.hydrateValue(e.getValue());
                Index2IdxColumnValue colValue = Index2IdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<Index2IdxRow, Iterator<Index2IdxColumnValue>> getRowsColumnRangeIterator(Iterable<Index2IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index2IdxRow, Iterator<Index2IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index2IdxRow row = Index2IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<Index2IdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    Index2IdxColumn col = Index2IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index2IdxColumnValue.hydrateValue(result.getValue());
                    return Index2IdxColumnValue.of(col, val);
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

        public BatchingVisitableView<Index2IdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, Index2IdxRowResult>() {
                @Override
                public Index2IdxRowResult apply(RowResult<byte[]> input) {
                    return Index2IdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<Index2IdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<Index2IdxRowResult>>() {
                @Override
                public BatchingVisitable<Index2IdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, Index2IdxRowResult>() {
                        @Override
                        public Index2IdxRowResult apply(RowResult<byte[]> row) {
                            return Index2IdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<Index2IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index2IdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<Index2IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index2IdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<Index2IdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, Index2IdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<Index2IdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<Index2IdxRowResult> rowResults) {
                    Multimap<Index2IdxRow, Index2IdxColumn> toRemove = HashMultimap.create();
                    for (Index2IdxRowResult rowResult : rowResults) {
                        for (Index2IdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class Index3IdxTable implements
            AtlasDbDynamicMutablePersistentTable<Index3IdxTable.Index3IdxRow,
                                                    Index3IdxTable.Index3IdxColumn,
                                                    Index3IdxTable.Index3IdxColumnValue,
                                                    Index3IdxTable.Index3IdxRowResult> {
        private final Transaction t;
        private final List<Index3IdxTrigger> triggers;
        private final static String rawTableName = "index3_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static Index3IdxTable of(DataTable table) {
            return new Index3IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index3IdxTrigger>of());
        }

        public static Index3IdxTable of(DataTable table, Index3IdxTrigger trigger, Index3IdxTrigger... triggers) {
            return new Index3IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index3IdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static Index3IdxTable of(DataTable table, List<Index3IdxTrigger> triggers) {
            return new Index3IdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private Index3IdxTable(Transaction t, Namespace namespace, List<Index3IdxTrigger> triggers) {
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
         * Index3IdxRow {
         *   {@literal Long value};
         * }
         * </pre>
         */
        public static final class Index3IdxRow implements Persistable, Comparable<Index3IdxRow> {
            private final long value;

            public static Index3IdxRow of(long value) {
                return new Index3IdxRow(value);
            }

            private Index3IdxRow(long value) {
                this.value = value;
            }

            public long getValue() {
                return value;
            }

            public static Function<Index3IdxRow, Long> getValueFun() {
                return new Function<Index3IdxRow, Long>() {
                    @Override
                    public Long apply(Index3IdxRow row) {
                        return row.value;
                    }
                };
            }

            public static Function<Long, Index3IdxRow> fromValueFun() {
                return new Function<Long, Index3IdxRow>() {
                    @Override
                    public Index3IdxRow apply(Long row) {
                        return Index3IdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] valueBytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
                return EncodingUtils.add(valueBytes);
            }

            public static final Hydrator<Index3IdxRow> BYTES_HYDRATOR = new Hydrator<Index3IdxRow>() {
                @Override
                public Index3IdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long value = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    return new Index3IdxRow(value);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("value", value)
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
                Index3IdxRow other = (Index3IdxRow) obj;
                return Objects.equals(value, other.value);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(value);
            }

            @Override
            public int compareTo(Index3IdxRow o) {
                return ComparisonChain.start()
                    .compare(this.value, o.value)
                    .result();
            }
        }

        /**
         * <pre>
         * Index3IdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * </pre>
         */
        public static final class Index3IdxColumn implements Persistable, Comparable<Index3IdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;

            public static Index3IdxColumn of(byte[] rowName, byte[] columnName) {
                return new Index3IdxColumn(rowName, columnName);
            }

            private Index3IdxColumn(byte[] rowName, byte[] columnName) {
                this.rowName = rowName;
                this.columnName = columnName;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public static Function<Index3IdxColumn, byte[]> getRowNameFun() {
                return new Function<Index3IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index3IdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<Index3IdxColumn, byte[]> getColumnNameFun() {
                return new Function<Index3IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index3IdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return EncodingUtils.add(rowNameBytes, columnNameBytes);
            }

            public static final Hydrator<Index3IdxColumn> BYTES_HYDRATOR = new Hydrator<Index3IdxColumn>() {
                @Override
                public Index3IdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    return new Index3IdxColumn(rowName, columnName);
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

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                Index3IdxColumn other = (Index3IdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName });
            }

            @Override
            public int compareTo(Index3IdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .result();
            }
        }

        public interface Index3IdxTrigger {
            public void putIndex3Idx(Multimap<Index3IdxRow, ? extends Index3IdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class Index3IdxColumnValue implements ColumnValue<Long> {
            private final Index3IdxColumn columnName;
            private final Long value;

            public static Index3IdxColumnValue of(Index3IdxColumn columnName, Long value) {
                return new Index3IdxColumnValue(columnName, value);
            }

            private Index3IdxColumnValue(Index3IdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public Index3IdxColumn getColumnName() {
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

            public static Function<Index3IdxColumnValue, Index3IdxColumn> getColumnNameFun() {
                return new Function<Index3IdxColumnValue, Index3IdxColumn>() {
                    @Override
                    public Index3IdxColumn apply(Index3IdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<Index3IdxColumnValue, Long> getValueFun() {
                return new Function<Index3IdxColumnValue, Long>() {
                    @Override
                    public Long apply(Index3IdxColumnValue columnValue) {
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

        public static final class Index3IdxRowResult implements TypedRowResult {
            private final Index3IdxRow rowName;
            private final ImmutableSet<Index3IdxColumnValue> columnValues;

            public static Index3IdxRowResult of(RowResult<byte[]> rowResult) {
                Index3IdxRow rowName = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<Index3IdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = Index3IdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(Index3IdxColumnValue.of(col, value));
                }
                return new Index3IdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private Index3IdxRowResult(Index3IdxRow rowName, ImmutableSet<Index3IdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public Index3IdxRow getRowName() {
                return rowName;
            }

            public Set<Index3IdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<Index3IdxRowResult, Index3IdxRow> getRowNameFun() {
                return new Function<Index3IdxRowResult, Index3IdxRow>() {
                    @Override
                    public Index3IdxRow apply(Index3IdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<Index3IdxRowResult, ImmutableSet<Index3IdxColumnValue>> getColumnValuesFun() {
                return new Function<Index3IdxRowResult, ImmutableSet<Index3IdxColumnValue>>() {
                    @Override
                    public ImmutableSet<Index3IdxColumnValue> apply(Index3IdxRowResult rowResult) {
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
        public void delete(Index3IdxRow row, Index3IdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<Index3IdxRow> rows) {
            Multimap<Index3IdxRow, Index3IdxColumn> toRemove = HashMultimap.create();
            Multimap<Index3IdxRow, Index3IdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<Index3IdxRow, Index3IdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<Index3IdxRow, Index3IdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(Index3IdxRow rowName, Iterable<Index3IdxColumnValue> values) {
            put(ImmutableMultimap.<Index3IdxRow, Index3IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Index3IdxRow rowName, Index3IdxColumnValue... values) {
            put(ImmutableMultimap.<Index3IdxRow, Index3IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<Index3IdxRow, ? extends Index3IdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (Index3IdxTrigger trigger : triggers) {
                trigger.putIndex3Idx(values);
            }
        }

        @Override
        public void touch(Multimap<Index3IdxRow, Index3IdxColumn> values) {
            Multimap<Index3IdxRow, Index3IdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<Index3IdxRow, Index3IdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<Index3IdxRow, Index3IdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<Index3IdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(Index3IdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<Index3IdxRow, Index3IdxColumnValue> get(Multimap<Index3IdxRow, Index3IdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<Index3IdxRow, Index3IdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    Index3IdxRow row = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = Index3IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index3IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<Index3IdxColumnValue> getRowColumns(Index3IdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<Index3IdxColumnValue> getRowColumns(Index3IdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<Index3IdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index3IdxColumnValue.hydrateValue(e.getValue());
                    ret.add(Index3IdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<Index3IdxRow, Index3IdxColumnValue> getRowsMultimap(Iterable<Index3IdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<Index3IdxRow, Index3IdxColumnValue> getRowsMultimap(Iterable<Index3IdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<Index3IdxRow, Index3IdxColumnValue> getRowsMultimapInternal(Iterable<Index3IdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<Index3IdxRow, Index3IdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<Index3IdxRow, Index3IdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                Index3IdxRow row = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index3IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index3IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<Index3IdxRow, BatchingVisitable<Index3IdxColumnValue>> getRowsColumnRange(Iterable<Index3IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index3IdxRow, BatchingVisitable<Index3IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index3IdxRow row = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<Index3IdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index3IdxColumnValue.hydrateValue(result.getValue());
                    return Index3IdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<Index3IdxRow, Index3IdxColumnValue>> getRowsColumnRange(Iterable<Index3IdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                Index3IdxRow row = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = Index3IdxColumnValue.hydrateValue(e.getValue());
                Index3IdxColumnValue colValue = Index3IdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<Index3IdxRow, Iterator<Index3IdxColumnValue>> getRowsColumnRangeIterator(Iterable<Index3IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index3IdxRow, Iterator<Index3IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index3IdxRow row = Index3IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<Index3IdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    Index3IdxColumn col = Index3IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index3IdxColumnValue.hydrateValue(result.getValue());
                    return Index3IdxColumnValue.of(col, val);
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

        public BatchingVisitableView<Index3IdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, Index3IdxRowResult>() {
                @Override
                public Index3IdxRowResult apply(RowResult<byte[]> input) {
                    return Index3IdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<Index3IdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<Index3IdxRowResult>>() {
                @Override
                public BatchingVisitable<Index3IdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, Index3IdxRowResult>() {
                        @Override
                        public Index3IdxRowResult apply(RowResult<byte[]> row) {
                            return Index3IdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<Index3IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index3IdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<Index3IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index3IdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<Index3IdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, Index3IdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<Index3IdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<Index3IdxRowResult> rowResults) {
                    Multimap<Index3IdxRow, Index3IdxColumn> toRemove = HashMultimap.create();
                    for (Index3IdxRowResult rowResult : rowResults) {
                        for (Index3IdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class Index4IdxTable implements
            AtlasDbDynamicMutablePersistentTable<Index4IdxTable.Index4IdxRow,
                                                    Index4IdxTable.Index4IdxColumn,
                                                    Index4IdxTable.Index4IdxColumnValue,
                                                    Index4IdxTable.Index4IdxRowResult> {
        private final Transaction t;
        private final List<Index4IdxTrigger> triggers;
        private final static String rawTableName = "index4_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static Index4IdxTable of(DataTable table) {
            return new Index4IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index4IdxTrigger>of());
        }

        public static Index4IdxTable of(DataTable table, Index4IdxTrigger trigger, Index4IdxTrigger... triggers) {
            return new Index4IdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<Index4IdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static Index4IdxTable of(DataTable table, List<Index4IdxTrigger> triggers) {
            return new Index4IdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private Index4IdxTable(Transaction t, Namespace namespace, List<Index4IdxTrigger> triggers) {
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
         * Index4IdxRow {
         *   {@literal Long value1};
         *   {@literal Long value2};
         * }
         * </pre>
         */
        public static final class Index4IdxRow implements Persistable, Comparable<Index4IdxRow> {
            private final long value1;
            private final long value2;

            public static Index4IdxRow of(long value1, long value2) {
                return new Index4IdxRow(value1, value2);
            }

            private Index4IdxRow(long value1, long value2) {
                this.value1 = value1;
                this.value2 = value2;
            }

            public long getValue1() {
                return value1;
            }

            public long getValue2() {
                return value2;
            }

            public static Function<Index4IdxRow, Long> getValue1Fun() {
                return new Function<Index4IdxRow, Long>() {
                    @Override
                    public Long apply(Index4IdxRow row) {
                        return row.value1;
                    }
                };
            }

            public static Function<Index4IdxRow, Long> getValue2Fun() {
                return new Function<Index4IdxRow, Long>() {
                    @Override
                    public Long apply(Index4IdxRow row) {
                        return row.value2;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] value1Bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value1);
                byte[] value2Bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value2);
                return EncodingUtils.add(value1Bytes, value2Bytes);
            }

            public static final Hydrator<Index4IdxRow> BYTES_HYDRATOR = new Hydrator<Index4IdxRow>() {
                @Override
                public Index4IdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long value1 = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    Long value2 = Long.MIN_VALUE ^ PtBytes.toLong(_input, _index);
                    _index += 8;
                    return new Index4IdxRow(value1, value2);
                }
            };

            public static RangeRequest.Builder createPrefixRange(long value1) {
                byte[] value1Bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value1);
                return RangeRequest.builder().prefixRange(EncodingUtils.add(value1Bytes));
            }

            public static Prefix prefix(long value1) {
                byte[] value1Bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value1);
                return new Prefix(EncodingUtils.add(value1Bytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("value1", value1)
                    .add("value2", value2)
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
                Index4IdxRow other = (Index4IdxRow) obj;
                return Objects.equals(value1, other.value1) && Objects.equals(value2, other.value2);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ value1, value2 });
            }

            @Override
            public int compareTo(Index4IdxRow o) {
                return ComparisonChain.start()
                    .compare(this.value1, o.value1)
                    .compare(this.value2, o.value2)
                    .result();
            }
        }

        /**
         * <pre>
         * Index4IdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * </pre>
         */
        public static final class Index4IdxColumn implements Persistable, Comparable<Index4IdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;

            public static Index4IdxColumn of(byte[] rowName, byte[] columnName) {
                return new Index4IdxColumn(rowName, columnName);
            }

            private Index4IdxColumn(byte[] rowName, byte[] columnName) {
                this.rowName = rowName;
                this.columnName = columnName;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public static Function<Index4IdxColumn, byte[]> getRowNameFun() {
                return new Function<Index4IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index4IdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<Index4IdxColumn, byte[]> getColumnNameFun() {
                return new Function<Index4IdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(Index4IdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return EncodingUtils.add(rowNameBytes, columnNameBytes);
            }

            public static final Hydrator<Index4IdxColumn> BYTES_HYDRATOR = new Hydrator<Index4IdxColumn>() {
                @Override
                public Index4IdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    return new Index4IdxColumn(rowName, columnName);
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

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                Index4IdxColumn other = (Index4IdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName });
            }

            @Override
            public int compareTo(Index4IdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .result();
            }
        }

        public interface Index4IdxTrigger {
            public void putIndex4Idx(Multimap<Index4IdxRow, ? extends Index4IdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class Index4IdxColumnValue implements ColumnValue<Long> {
            private final Index4IdxColumn columnName;
            private final Long value;

            public static Index4IdxColumnValue of(Index4IdxColumn columnName, Long value) {
                return new Index4IdxColumnValue(columnName, value);
            }

            private Index4IdxColumnValue(Index4IdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public Index4IdxColumn getColumnName() {
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

            public static Function<Index4IdxColumnValue, Index4IdxColumn> getColumnNameFun() {
                return new Function<Index4IdxColumnValue, Index4IdxColumn>() {
                    @Override
                    public Index4IdxColumn apply(Index4IdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<Index4IdxColumnValue, Long> getValueFun() {
                return new Function<Index4IdxColumnValue, Long>() {
                    @Override
                    public Long apply(Index4IdxColumnValue columnValue) {
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

        public static final class Index4IdxRowResult implements TypedRowResult {
            private final Index4IdxRow rowName;
            private final ImmutableSet<Index4IdxColumnValue> columnValues;

            public static Index4IdxRowResult of(RowResult<byte[]> rowResult) {
                Index4IdxRow rowName = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<Index4IdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = Index4IdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(Index4IdxColumnValue.of(col, value));
                }
                return new Index4IdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private Index4IdxRowResult(Index4IdxRow rowName, ImmutableSet<Index4IdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public Index4IdxRow getRowName() {
                return rowName;
            }

            public Set<Index4IdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<Index4IdxRowResult, Index4IdxRow> getRowNameFun() {
                return new Function<Index4IdxRowResult, Index4IdxRow>() {
                    @Override
                    public Index4IdxRow apply(Index4IdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<Index4IdxRowResult, ImmutableSet<Index4IdxColumnValue>> getColumnValuesFun() {
                return new Function<Index4IdxRowResult, ImmutableSet<Index4IdxColumnValue>>() {
                    @Override
                    public ImmutableSet<Index4IdxColumnValue> apply(Index4IdxRowResult rowResult) {
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
        public void delete(Index4IdxRow row, Index4IdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<Index4IdxRow> rows) {
            Multimap<Index4IdxRow, Index4IdxColumn> toRemove = HashMultimap.create();
            Multimap<Index4IdxRow, Index4IdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<Index4IdxRow, Index4IdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<Index4IdxRow, Index4IdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(Index4IdxRow rowName, Iterable<Index4IdxColumnValue> values) {
            put(ImmutableMultimap.<Index4IdxRow, Index4IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Index4IdxRow rowName, Index4IdxColumnValue... values) {
            put(ImmutableMultimap.<Index4IdxRow, Index4IdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<Index4IdxRow, ? extends Index4IdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (Index4IdxTrigger trigger : triggers) {
                trigger.putIndex4Idx(values);
            }
        }

        @Override
        public void touch(Multimap<Index4IdxRow, Index4IdxColumn> values) {
            Multimap<Index4IdxRow, Index4IdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<Index4IdxRow, Index4IdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<Index4IdxRow, Index4IdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<Index4IdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(Index4IdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<Index4IdxRow, Index4IdxColumnValue> get(Multimap<Index4IdxRow, Index4IdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<Index4IdxRow, Index4IdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    Index4IdxRow row = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = Index4IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index4IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<Index4IdxColumnValue> getRowColumns(Index4IdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<Index4IdxColumnValue> getRowColumns(Index4IdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<Index4IdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index4IdxColumnValue.hydrateValue(e.getValue());
                    ret.add(Index4IdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<Index4IdxRow, Index4IdxColumnValue> getRowsMultimap(Iterable<Index4IdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<Index4IdxRow, Index4IdxColumnValue> getRowsMultimap(Iterable<Index4IdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<Index4IdxRow, Index4IdxColumnValue> getRowsMultimapInternal(Iterable<Index4IdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<Index4IdxRow, Index4IdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<Index4IdxRow, Index4IdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                Index4IdxRow row = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = Index4IdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, Index4IdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<Index4IdxRow, BatchingVisitable<Index4IdxColumnValue>> getRowsColumnRange(Iterable<Index4IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index4IdxRow, BatchingVisitable<Index4IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index4IdxRow row = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<Index4IdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index4IdxColumnValue.hydrateValue(result.getValue());
                    return Index4IdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<Index4IdxRow, Index4IdxColumnValue>> getRowsColumnRange(Iterable<Index4IdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                Index4IdxRow row = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = Index4IdxColumnValue.hydrateValue(e.getValue());
                Index4IdxColumnValue colValue = Index4IdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<Index4IdxRow, Iterator<Index4IdxColumnValue>> getRowsColumnRangeIterator(Iterable<Index4IdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<Index4IdxRow, Iterator<Index4IdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                Index4IdxRow row = Index4IdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<Index4IdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    Index4IdxColumn col = Index4IdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = Index4IdxColumnValue.hydrateValue(result.getValue());
                    return Index4IdxColumnValue.of(col, val);
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

        public BatchingVisitableView<Index4IdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, Index4IdxRowResult>() {
                @Override
                public Index4IdxRowResult apply(RowResult<byte[]> input) {
                    return Index4IdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<Index4IdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<Index4IdxRowResult>>() {
                @Override
                public BatchingVisitable<Index4IdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, Index4IdxRowResult>() {
                        @Override
                        public Index4IdxRowResult apply(RowResult<byte[]> row) {
                            return Index4IdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<Index4IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index4IdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<Index4IdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, Index4IdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<Index4IdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, Index4IdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<Index4IdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<Index4IdxRowResult> rowResults) {
                    Multimap<Index4IdxRow, Index4IdxColumn> toRemove = HashMultimap.create();
                    for (Index4IdxRowResult rowResult : rowResults) {
                        for (Index4IdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    static String __CLASS_HASH = "5smQlgPEeoFu1azUh8FBpg==";
}
