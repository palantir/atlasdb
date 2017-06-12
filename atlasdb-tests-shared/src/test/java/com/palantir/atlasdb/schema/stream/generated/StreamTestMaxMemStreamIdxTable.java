package com.palantir.atlasdb.schema.stream.generated;

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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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
public final class StreamTestMaxMemStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow,
                                                StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumn,
                                                StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue,
                                                StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRowResult> {
    private final Transaction t;
    private final List<StreamTestMaxMemStreamIdxTrigger> triggers;
    private final static String rawTableName = "stream_test_max_mem_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestMaxMemStreamIdxTable of(Transaction t, Namespace namespace) {
        return new StreamTestMaxMemStreamIdxTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamIdxTrigger>of());
    }

    static StreamTestMaxMemStreamIdxTable of(Transaction t, Namespace namespace, StreamTestMaxMemStreamIdxTrigger trigger, StreamTestMaxMemStreamIdxTrigger... triggers) {
        return new StreamTestMaxMemStreamIdxTable(t, namespace, ImmutableList.<StreamTestMaxMemStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestMaxMemStreamIdxTable of(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamIdxTrigger> triggers) {
        return new StreamTestMaxMemStreamIdxTable(t, namespace, triggers);
    }

    private StreamTestMaxMemStreamIdxTable(Transaction t, Namespace namespace, List<StreamTestMaxMemStreamIdxTrigger> triggers) {
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
     * StreamTestMaxMemStreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTestMaxMemStreamIdxRow implements Persistable, Comparable<StreamTestMaxMemStreamIdxRow> {
        private final long id;

        public static StreamTestMaxMemStreamIdxRow of(long id) {
            return new StreamTestMaxMemStreamIdxRow(id);
        }

        private StreamTestMaxMemStreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTestMaxMemStreamIdxRow, Long> getIdFun() {
            return new Function<StreamTestMaxMemStreamIdxRow, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTestMaxMemStreamIdxRow> fromIdFun() {
            return new Function<Long, StreamTestMaxMemStreamIdxRow>() {
                @Override
                public StreamTestMaxMemStreamIdxRow apply(Long row) {
                    return StreamTestMaxMemStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<StreamTestMaxMemStreamIdxRow> BYTES_HYDRATOR = new Hydrator<StreamTestMaxMemStreamIdxRow>() {
            @Override
            public StreamTestMaxMemStreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTestMaxMemStreamIdxRow(id);
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
            StreamTestMaxMemStreamIdxRow other = (StreamTestMaxMemStreamIdxRow) obj;
            return Objects.equal(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(StreamTestMaxMemStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestMaxMemStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class StreamTestMaxMemStreamIdxColumn implements Persistable, Comparable<StreamTestMaxMemStreamIdxColumn> {
        private final byte[] reference;

        public static StreamTestMaxMemStreamIdxColumn of(byte[] reference) {
            return new StreamTestMaxMemStreamIdxColumn(reference);
        }

        private StreamTestMaxMemStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<StreamTestMaxMemStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<StreamTestMaxMemStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestMaxMemStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], StreamTestMaxMemStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], StreamTestMaxMemStreamIdxColumn>() {
                @Override
                public StreamTestMaxMemStreamIdxColumn apply(byte[] row) {
                    return StreamTestMaxMemStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<StreamTestMaxMemStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestMaxMemStreamIdxColumn>() {
            @Override
            public StreamTestMaxMemStreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return new StreamTestMaxMemStreamIdxColumn(reference);
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
            StreamTestMaxMemStreamIdxColumn other = (StreamTestMaxMemStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(StreamTestMaxMemStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface StreamTestMaxMemStreamIdxTrigger {
        public void putStreamTestMaxMemStreamIdx(Multimap<StreamTestMaxMemStreamIdxRow, ? extends StreamTestMaxMemStreamIdxColumnValue> newRows);
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
    public static final class StreamTestMaxMemStreamIdxColumnValue implements ColumnValue<Long> {
        private final StreamTestMaxMemStreamIdxColumn columnName;
        private final Long value;

        public static StreamTestMaxMemStreamIdxColumnValue of(StreamTestMaxMemStreamIdxColumn columnName, Long value) {
            return new StreamTestMaxMemStreamIdxColumnValue(columnName, value);
        }

        private StreamTestMaxMemStreamIdxColumnValue(StreamTestMaxMemStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestMaxMemStreamIdxColumn getColumnName() {
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

        public static Function<StreamTestMaxMemStreamIdxColumnValue, StreamTestMaxMemStreamIdxColumn> getColumnNameFun() {
            return new Function<StreamTestMaxMemStreamIdxColumnValue, StreamTestMaxMemStreamIdxColumn>() {
                @Override
                public StreamTestMaxMemStreamIdxColumn apply(StreamTestMaxMemStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestMaxMemStreamIdxColumnValue, Long> getValueFun() {
            return new Function<StreamTestMaxMemStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestMaxMemStreamIdxColumnValue columnValue) {
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

    public static final class StreamTestMaxMemStreamIdxRowResult implements TypedRowResult {
        private final StreamTestMaxMemStreamIdxRow rowName;
        private final ImmutableSet<StreamTestMaxMemStreamIdxColumnValue> columnValues;

        public static StreamTestMaxMemStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestMaxMemStreamIdxRow rowName = StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestMaxMemStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestMaxMemStreamIdxColumnValue.of(col, value));
            }
            return new StreamTestMaxMemStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestMaxMemStreamIdxRowResult(StreamTestMaxMemStreamIdxRow rowName, ImmutableSet<StreamTestMaxMemStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestMaxMemStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestMaxMemStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestMaxMemStreamIdxRowResult, StreamTestMaxMemStreamIdxRow> getRowNameFun() {
            return new Function<StreamTestMaxMemStreamIdxRowResult, StreamTestMaxMemStreamIdxRow>() {
                @Override
                public StreamTestMaxMemStreamIdxRow apply(StreamTestMaxMemStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestMaxMemStreamIdxRowResult, ImmutableSet<StreamTestMaxMemStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestMaxMemStreamIdxRowResult, ImmutableSet<StreamTestMaxMemStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestMaxMemStreamIdxColumnValue> apply(StreamTestMaxMemStreamIdxRowResult rowResult) {
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
    public void delete(StreamTestMaxMemStreamIdxRow row, StreamTestMaxMemStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestMaxMemStreamIdxRow> rows) {
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestMaxMemStreamIdxRow rowName, Iterable<StreamTestMaxMemStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestMaxMemStreamIdxRow rowName, StreamTestMaxMemStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestMaxMemStreamIdxRow, ? extends StreamTestMaxMemStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestMaxMemStreamIdxTrigger trigger : triggers) {
            trigger.putStreamTestMaxMemStreamIdx(values);
        }
    }

    @Override
    public void putUnlessExists(StreamTestMaxMemStreamIdxRow rowName, Iterable<StreamTestMaxMemStreamIdxColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(StreamTestMaxMemStreamIdxRow rowName, StreamTestMaxMemStreamIdxColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(Multimap<StreamTestMaxMemStreamIdxRow, ? extends StreamTestMaxMemStreamIdxColumnValue> rows) {
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> toGet = Multimaps.transformValues(rows, StreamTestMaxMemStreamIdxColumnValue.getColumnNameFun());
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> existing = get(toGet);
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> toPut = HashMultimap.create();
        for (Entry<StreamTestMaxMemStreamIdxRow, ? extends StreamTestMaxMemStreamIdxColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> values) {
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestMaxMemStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestMaxMemStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> get(Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestMaxMemStreamIdxRow row = StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestMaxMemStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestMaxMemStreamIdxColumnValue> getRowColumns(StreamTestMaxMemStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestMaxMemStreamIdxColumnValue> getRowColumns(StreamTestMaxMemStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestMaxMemStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestMaxMemStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestMaxMemStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestMaxMemStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> getRowsMultimapInternal(Iterable<StreamTestMaxMemStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestMaxMemStreamIdxRow row = StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestMaxMemStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestMaxMemStreamIdxRow, BatchingVisitable<StreamTestMaxMemStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestMaxMemStreamIdxRow, BatchingVisitable<StreamTestMaxMemStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestMaxMemStreamIdxRow row = StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestMaxMemStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(result.getValue());
                return StreamTestMaxMemStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestMaxMemStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestMaxMemStreamIdxRow row = StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestMaxMemStreamIdxColumn col = StreamTestMaxMemStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestMaxMemStreamIdxColumnValue.hydrateValue(e.getValue());
            StreamTestMaxMemStreamIdxColumnValue colValue = StreamTestMaxMemStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<StreamTestMaxMemStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestMaxMemStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestMaxMemStreamIdxRowResult>() {
            @Override
            public StreamTestMaxMemStreamIdxRowResult apply(RowResult<byte[]> input) {
                return StreamTestMaxMemStreamIdxRowResult.of(input);
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
     * {@link Supplier}
     * {@link TableReference}
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "nYbolFkQFApw7mj7fX894A==";
}
