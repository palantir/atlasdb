package com.palantir.atlasdb.schema.stream.generated;

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
public final class StreamTestStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestStreamIdxTable.StreamTestStreamIdxRow,
                                                StreamTestStreamIdxTable.StreamTestStreamIdxColumn,
                                                StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue,
                                                StreamTestStreamIdxTable.StreamTestStreamIdxRowResult> {
    private final Transaction t;
    private final List<StreamTestStreamIdxTrigger> triggers;
    private final static String rawTableName = "stream_test_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestStreamIdxTable of(Transaction t, Namespace namespace) {
        return new StreamTestStreamIdxTable(t, namespace, ImmutableList.<StreamTestStreamIdxTrigger>of());
    }

    static StreamTestStreamIdxTable of(Transaction t, Namespace namespace, StreamTestStreamIdxTrigger trigger, StreamTestStreamIdxTrigger... triggers) {
        return new StreamTestStreamIdxTable(t, namespace, ImmutableList.<StreamTestStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestStreamIdxTable of(Transaction t, Namespace namespace, List<StreamTestStreamIdxTrigger> triggers) {
        return new StreamTestStreamIdxTable(t, namespace, triggers);
    }

    private StreamTestStreamIdxTable(Transaction t, Namespace namespace, List<StreamTestStreamIdxTrigger> triggers) {
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
     * StreamTestStreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTestStreamIdxRow implements Persistable, Comparable<StreamTestStreamIdxRow> {
        private final long id;

        public static StreamTestStreamIdxRow of(long id) {
            return new StreamTestStreamIdxRow(id);
        }

        private StreamTestStreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTestStreamIdxRow, Long> getIdFun() {
            return new Function<StreamTestStreamIdxRow, Long>() {
                @Override
                public Long apply(StreamTestStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTestStreamIdxRow> fromIdFun() {
            return new Function<Long, StreamTestStreamIdxRow>() {
                @Override
                public StreamTestStreamIdxRow apply(Long row) {
                    return StreamTestStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<StreamTestStreamIdxRow> BYTES_HYDRATOR = new Hydrator<StreamTestStreamIdxRow>() {
            @Override
            public StreamTestStreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTestStreamIdxRow(id);
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
            StreamTestStreamIdxRow other = (StreamTestStreamIdxRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(StreamTestStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class StreamTestStreamIdxColumn implements Persistable, Comparable<StreamTestStreamIdxColumn> {
        private final byte[] reference;

        public static StreamTestStreamIdxColumn of(byte[] reference) {
            return new StreamTestStreamIdxColumn(reference);
        }

        private StreamTestStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<StreamTestStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<StreamTestStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], StreamTestStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], StreamTestStreamIdxColumn>() {
                @Override
                public StreamTestStreamIdxColumn apply(byte[] row) {
                    return StreamTestStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<StreamTestStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestStreamIdxColumn>() {
            @Override
            public StreamTestStreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return new StreamTestStreamIdxColumn(reference);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("reference", reference)
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
            StreamTestStreamIdxColumn other = (StreamTestStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(StreamTestStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface StreamTestStreamIdxTrigger {
        public void putStreamTestStreamIdx(Multimap<StreamTestStreamIdxRow, ? extends StreamTestStreamIdxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal byte[] reference};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class StreamTestStreamIdxColumnValue implements ColumnValue<Long> {
        private final StreamTestStreamIdxColumn columnName;
        private final Long value;

        public static StreamTestStreamIdxColumnValue of(StreamTestStreamIdxColumn columnName, Long value) {
            return new StreamTestStreamIdxColumnValue(columnName, value);
        }

        private StreamTestStreamIdxColumnValue(StreamTestStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestStreamIdxColumn getColumnName() {
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

        public static Function<StreamTestStreamIdxColumnValue, StreamTestStreamIdxColumn> getColumnNameFun() {
            return new Function<StreamTestStreamIdxColumnValue, StreamTestStreamIdxColumn>() {
                @Override
                public StreamTestStreamIdxColumn apply(StreamTestStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestStreamIdxColumnValue, Long> getValueFun() {
            return new Function<StreamTestStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestStreamIdxColumnValue columnValue) {
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

    public static final class StreamTestStreamIdxRowResult implements TypedRowResult {
        private final StreamTestStreamIdxRow rowName;
        private final ImmutableSet<StreamTestStreamIdxColumnValue> columnValues;

        public static StreamTestStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestStreamIdxRow rowName = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestStreamIdxColumnValue.of(col, value));
            }
            return new StreamTestStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestStreamIdxRowResult(StreamTestStreamIdxRow rowName, ImmutableSet<StreamTestStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestStreamIdxRowResult, StreamTestStreamIdxRow> getRowNameFun() {
            return new Function<StreamTestStreamIdxRowResult, StreamTestStreamIdxRow>() {
                @Override
                public StreamTestStreamIdxRow apply(StreamTestStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestStreamIdxRowResult, ImmutableSet<StreamTestStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestStreamIdxRowResult, ImmutableSet<StreamTestStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestStreamIdxColumnValue> apply(StreamTestStreamIdxRowResult rowResult) {
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
    public void delete(StreamTestStreamIdxRow row, StreamTestStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestStreamIdxRow> rows) {
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestStreamIdxRow rowName, Iterable<StreamTestStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestStreamIdxRow rowName, StreamTestStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestStreamIdxRow, ? extends StreamTestStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestStreamIdxTrigger trigger : triggers) {
            trigger.putStreamTestStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumn> values) {
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> get(Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestStreamIdxRow row = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestStreamIdxColumnValue> getRowColumns(StreamTestStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestStreamIdxColumnValue> getRowColumns(StreamTestStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> getRowsMultimapInternal(Iterable<StreamTestStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestStreamIdxRow row = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestStreamIdxRow, BatchingVisitable<StreamTestStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestStreamIdxRow, BatchingVisitable<StreamTestStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestStreamIdxRow row = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestStreamIdxColumnValue.hydrateValue(result.getValue());
                return StreamTestStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestStreamIdxRow, StreamTestStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestStreamIdxRow row = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestStreamIdxColumnValue.hydrateValue(e.getValue());
            StreamTestStreamIdxColumnValue colValue = StreamTestStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<StreamTestStreamIdxRow, Iterator<StreamTestStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<StreamTestStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestStreamIdxRow, Iterator<StreamTestStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestStreamIdxRow row = StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<StreamTestStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                StreamTestStreamIdxColumn col = StreamTestStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestStreamIdxColumnValue.hydrateValue(result.getValue());
                return StreamTestStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<StreamTestStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, StreamTestStreamIdxRowResult>() {
            @Override
            public StreamTestStreamIdxRowResult apply(RowResult<byte[]> input) {
                return StreamTestStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "UpLQoZwCK/Ov1pclyB+iSA==";
}
