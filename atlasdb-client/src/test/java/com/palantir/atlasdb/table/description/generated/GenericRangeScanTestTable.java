package com.palantir.atlasdb.table.description.generated;

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
public final class GenericRangeScanTestTable implements
        AtlasDbDynamicMutablePersistentTable<GenericRangeScanTestTable.GenericRangeScanTestRow,
                                                GenericRangeScanTestTable.GenericRangeScanTestColumn,
                                                GenericRangeScanTestTable.GenericRangeScanTestColumnValue,
                                                GenericRangeScanTestTable.GenericRangeScanTestRowResult> {
    private final Transaction t;
    private final List<GenericRangeScanTestTrigger> triggers;
    private final static String rawTableName = "genericRangeScanTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static GenericRangeScanTestTable of(Transaction t, Namespace namespace) {
        return new GenericRangeScanTestTable(t, namespace, ImmutableList.<GenericRangeScanTestTrigger>of());
    }

    static GenericRangeScanTestTable of(Transaction t, Namespace namespace, GenericRangeScanTestTrigger trigger, GenericRangeScanTestTrigger... triggers) {
        return new GenericRangeScanTestTable(t, namespace, ImmutableList.<GenericRangeScanTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static GenericRangeScanTestTable of(Transaction t, Namespace namespace, List<GenericRangeScanTestTrigger> triggers) {
        return new GenericRangeScanTestTable(t, namespace, triggers);
    }

    private GenericRangeScanTestTable(Transaction t, Namespace namespace, List<GenericRangeScanTestTrigger> triggers) {
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
     * GenericRangeScanTestRow {
     *   {@literal Sha256Hash component1};
     * }
     * </pre>
     */
    public static final class GenericRangeScanTestRow implements Persistable, Comparable<GenericRangeScanTestRow> {
        private final Sha256Hash component1;

        public static GenericRangeScanTestRow of(Sha256Hash component1) {
            return new GenericRangeScanTestRow(component1);
        }

        private GenericRangeScanTestRow(Sha256Hash component1) {
            this.component1 = component1;
        }

        public Sha256Hash getComponent1() {
            return component1;
        }

        public static Function<GenericRangeScanTestRow, Sha256Hash> getComponent1Fun() {
            return new Function<GenericRangeScanTestRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(GenericRangeScanTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<Sha256Hash, GenericRangeScanTestRow> fromComponent1Fun() {
            return new Function<Sha256Hash, GenericRangeScanTestRow>() {
                @Override
                public GenericRangeScanTestRow apply(Sha256Hash row) {
                    return GenericRangeScanTestRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component1Bytes = component1.getBytes();
            return EncodingUtils.add(component1Bytes);
        }

        public static final Hydrator<GenericRangeScanTestRow> BYTES_HYDRATOR = new Hydrator<GenericRangeScanTestRow>() {
            @Override
            public GenericRangeScanTestRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Sha256Hash component1 = new Sha256Hash(EncodingUtils.get32Bytes(_input, _index));
                _index += 32;
                return new GenericRangeScanTestRow(component1);
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
            GenericRangeScanTestRow other = (GenericRangeScanTestRow) obj;
            return Objects.equals(component1, other.component1);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(component1);
        }

        @Override
        public int compareTo(GenericRangeScanTestRow o) {
            return ComparisonChain.start()
                .compare(this.component1, o.component1)
                .result();
        }
    }

    /**
     * <pre>
     * GenericRangeScanTestColumn {
     *   {@literal String component2};
     * }
     * </pre>
     */
    public static final class GenericRangeScanTestColumn implements Persistable, Comparable<GenericRangeScanTestColumn> {
        private final String component2;

        public static GenericRangeScanTestColumn of(String component2) {
            return new GenericRangeScanTestColumn(component2);
        }

        private GenericRangeScanTestColumn(String component2) {
            this.component2 = component2;
        }

        public String getComponent2() {
            return component2;
        }

        public static Function<GenericRangeScanTestColumn, String> getComponent2Fun() {
            return new Function<GenericRangeScanTestColumn, String>() {
                @Override
                public String apply(GenericRangeScanTestColumn row) {
                    return row.component2;
                }
            };
        }

        public static Function<String, GenericRangeScanTestColumn> fromComponent2Fun() {
            return new Function<String, GenericRangeScanTestColumn>() {
                @Override
                public GenericRangeScanTestColumn apply(String row) {
                    return GenericRangeScanTestColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] component2Bytes = PtBytes.toBytes(component2);
            return EncodingUtils.add(component2Bytes);
        }

        public static final Hydrator<GenericRangeScanTestColumn> BYTES_HYDRATOR = new Hydrator<GenericRangeScanTestColumn>() {
            @Override
            public GenericRangeScanTestColumn hydrateFromBytes(byte[] _input) {
                int _index = 0;
                String component2 = PtBytes.toString(_input, _index, _input.length-_index);
                _index += 0;
                return new GenericRangeScanTestColumn(component2);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("component2", component2)
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
            GenericRangeScanTestColumn other = (GenericRangeScanTestColumn) obj;
            return Objects.equals(component2, other.component2);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(component2);
        }

        @Override
        public int compareTo(GenericRangeScanTestColumn o) {
            return ComparisonChain.start()
                .compare(this.component2, o.component2)
                .result();
        }
    }

    public interface GenericRangeScanTestTrigger {
        public void putGenericRangeScanTest(Multimap<GenericRangeScanTestRow, ? extends GenericRangeScanTestColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal String component2};
     * }
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class GenericRangeScanTestColumnValue implements ColumnValue<String> {
        private final GenericRangeScanTestColumn columnName;
        private final String value;

        public static GenericRangeScanTestColumnValue of(GenericRangeScanTestColumn columnName, String value) {
            return new GenericRangeScanTestColumnValue(columnName, value);
        }

        private GenericRangeScanTestColumnValue(GenericRangeScanTestColumn columnName, String value) {
            this.columnName = columnName;
            this.value = value;
        }

        public GenericRangeScanTestColumn getColumnName() {
            return columnName;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static String hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return PtBytes.toString(bytes, 0, bytes.length-0);
        }

        public static Function<GenericRangeScanTestColumnValue, GenericRangeScanTestColumn> getColumnNameFun() {
            return new Function<GenericRangeScanTestColumnValue, GenericRangeScanTestColumn>() {
                @Override
                public GenericRangeScanTestColumn apply(GenericRangeScanTestColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<GenericRangeScanTestColumnValue, String> getValueFun() {
            return new Function<GenericRangeScanTestColumnValue, String>() {
                @Override
                public String apply(GenericRangeScanTestColumnValue columnValue) {
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

    public static final class GenericRangeScanTestRowResult implements TypedRowResult {
        private final GenericRangeScanTestRow rowName;
        private final ImmutableSet<GenericRangeScanTestColumnValue> columnValues;

        public static GenericRangeScanTestRowResult of(RowResult<byte[]> rowResult) {
            GenericRangeScanTestRow rowName = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<GenericRangeScanTestColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String value = GenericRangeScanTestColumnValue.hydrateValue(e.getValue());
                columnValues.add(GenericRangeScanTestColumnValue.of(col, value));
            }
            return new GenericRangeScanTestRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private GenericRangeScanTestRowResult(GenericRangeScanTestRow rowName, ImmutableSet<GenericRangeScanTestColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public GenericRangeScanTestRow getRowName() {
            return rowName;
        }

        public Set<GenericRangeScanTestColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<GenericRangeScanTestRowResult, GenericRangeScanTestRow> getRowNameFun() {
            return new Function<GenericRangeScanTestRowResult, GenericRangeScanTestRow>() {
                @Override
                public GenericRangeScanTestRow apply(GenericRangeScanTestRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<GenericRangeScanTestRowResult, ImmutableSet<GenericRangeScanTestColumnValue>> getColumnValuesFun() {
            return new Function<GenericRangeScanTestRowResult, ImmutableSet<GenericRangeScanTestColumnValue>>() {
                @Override
                public ImmutableSet<GenericRangeScanTestColumnValue> apply(GenericRangeScanTestRowResult rowResult) {
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
    public void delete(GenericRangeScanTestRow row, GenericRangeScanTestColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<GenericRangeScanTestRow> rows) {
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> toRemove = HashMultimap.create();
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> result = getRowsMultimap(rows);
        for (Entry<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(GenericRangeScanTestRow rowName, Iterable<GenericRangeScanTestColumnValue> values) {
        put(ImmutableMultimap.<GenericRangeScanTestRow, GenericRangeScanTestColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(GenericRangeScanTestRow rowName, GenericRangeScanTestColumnValue... values) {
        put(ImmutableMultimap.<GenericRangeScanTestRow, GenericRangeScanTestColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<GenericRangeScanTestRow, ? extends GenericRangeScanTestColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (GenericRangeScanTestTrigger trigger : triggers) {
            trigger.putGenericRangeScanTest(values);
        }
    }

    @Override
    public void touch(Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> values) {
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<GenericRangeScanTestColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(GenericRangeScanTestColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> get(Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                GenericRangeScanTestRow row = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                String val = GenericRangeScanTestColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, GenericRangeScanTestColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<GenericRangeScanTestColumnValue> getRowColumns(GenericRangeScanTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<GenericRangeScanTestColumnValue> getRowColumns(GenericRangeScanTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<GenericRangeScanTestColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = GenericRangeScanTestColumnValue.hydrateValue(e.getValue());
                ret.add(GenericRangeScanTestColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> getRowsMultimap(Iterable<GenericRangeScanTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> getRowsMultimap(Iterable<GenericRangeScanTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> getRowsMultimapInternal(Iterable<GenericRangeScanTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            GenericRangeScanTestRow row = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                String val = GenericRangeScanTestColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, GenericRangeScanTestColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<GenericRangeScanTestRow, BatchingVisitable<GenericRangeScanTestColumnValue>> getRowsColumnRange(Iterable<GenericRangeScanTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<GenericRangeScanTestRow, BatchingVisitable<GenericRangeScanTestColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            GenericRangeScanTestRow row = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<GenericRangeScanTestColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = GenericRangeScanTestColumnValue.hydrateValue(result.getValue());
                return GenericRangeScanTestColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<GenericRangeScanTestRow, GenericRangeScanTestColumnValue>> getRowsColumnRange(Iterable<GenericRangeScanTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            GenericRangeScanTestRow row = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            String val = GenericRangeScanTestColumnValue.hydrateValue(e.getValue());
            GenericRangeScanTestColumnValue colValue = GenericRangeScanTestColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<GenericRangeScanTestRow, Iterator<GenericRangeScanTestColumnValue>> getRowsColumnRangeIterator(Iterable<GenericRangeScanTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<GenericRangeScanTestRow, Iterator<GenericRangeScanTestColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            GenericRangeScanTestRow row = GenericRangeScanTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<GenericRangeScanTestColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                GenericRangeScanTestColumn col = GenericRangeScanTestColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                String val = GenericRangeScanTestColumnValue.hydrateValue(result.getValue());
                return GenericRangeScanTestColumnValue.of(col, val);
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

    public BatchingVisitableView<GenericRangeScanTestRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, GenericRangeScanTestRowResult>() {
            @Override
            public GenericRangeScanTestRowResult apply(RowResult<byte[]> input) {
                return GenericRangeScanTestRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<GenericRangeScanTestRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<GenericRangeScanTestRowResult>>() {
            @Override
            public BatchingVisitable<GenericRangeScanTestRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, GenericRangeScanTestRowResult>() {
                    @Override
                    public GenericRangeScanTestRowResult apply(RowResult<byte[]> row) {
                        return GenericRangeScanTestRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<GenericRangeScanTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, GenericRangeScanTestRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<GenericRangeScanTestRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, GenericRangeScanTestRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<GenericRangeScanTestRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, GenericRangeScanTestRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<GenericRangeScanTestRowResult>, RuntimeException>() {
            @Override
            public boolean visit(List<GenericRangeScanTestRowResult> rowResults) {
                Multimap<GenericRangeScanTestRow, GenericRangeScanTestColumn> toRemove = HashMultimap.create();
                for (GenericRangeScanTestRowResult rowResult : rowResults) {
                    for (GenericRangeScanTestColumnValue columnValue : rowResult.getColumnValues()) {
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
    static String __CLASS_HASH = "MUMDhUC8qX2VcMbAP/ldog==";
}
