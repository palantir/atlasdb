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
public final class StreamTestWithHashStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow,
                                                StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumn,
                                                StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue,
                                                StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRowResult> {
    private final Transaction t;
    private final List<StreamTestWithHashStreamIdxTrigger> triggers;
    private final static String rawTableName = "stream_test_with_hash_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static StreamTestWithHashStreamIdxTable of(Transaction t, Namespace namespace) {
        return new StreamTestWithHashStreamIdxTable(t, namespace, ImmutableList.<StreamTestWithHashStreamIdxTrigger>of());
    }

    static StreamTestWithHashStreamIdxTable of(Transaction t, Namespace namespace, StreamTestWithHashStreamIdxTrigger trigger, StreamTestWithHashStreamIdxTrigger... triggers) {
        return new StreamTestWithHashStreamIdxTable(t, namespace, ImmutableList.<StreamTestWithHashStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTestWithHashStreamIdxTable of(Transaction t, Namespace namespace, List<StreamTestWithHashStreamIdxTrigger> triggers) {
        return new StreamTestWithHashStreamIdxTable(t, namespace, triggers);
    }

    private StreamTestWithHashStreamIdxTable(Transaction t, Namespace namespace, List<StreamTestWithHashStreamIdxTrigger> triggers) {
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
     * StreamTestWithHashStreamIdxRow {
     *   {@literal Long firstComponentHash};
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamIdxRow implements Persistable, Comparable<StreamTestWithHashStreamIdxRow> {
        private final long firstComponentHash;
        private final long id;

        public static StreamTestWithHashStreamIdxRow of(long id) {
            long firstComponentHash = Hashing.murmur3_128().hashBytes(ValueType.VAR_LONG.convertFromJava(id)).asLong();
            return new StreamTestWithHashStreamIdxRow(firstComponentHash, id);
        }

        private StreamTestWithHashStreamIdxRow(long firstComponentHash, long id) {
            this.firstComponentHash = firstComponentHash;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTestWithHashStreamIdxRow, Long> getIdFun() {
            return new Function<StreamTestWithHashStreamIdxRow, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTestWithHashStreamIdxRow> fromIdFun() {
            return new Function<Long, StreamTestWithHashStreamIdxRow>() {
                @Override
                public StreamTestWithHashStreamIdxRow apply(Long row) {
                    return StreamTestWithHashStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] firstComponentHashBytes = PtBytes.toBytes(Long.MIN_VALUE ^ firstComponentHash);
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(firstComponentHashBytes, idBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamIdxRow> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamIdxRow>() {
            @Override
            public StreamTestWithHashStreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long firstComponentHash = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTestWithHashStreamIdxRow(firstComponentHash, id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("firstComponentHash", firstComponentHash)
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
            StreamTestWithHashStreamIdxRow other = (StreamTestWithHashStreamIdxRow) obj;
            return Objects.equal(firstComponentHash, other.firstComponentHash) && Objects.equal(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ firstComponentHash, id });
        }

        @Override
        public int compareTo(StreamTestWithHashStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.firstComponentHash, o.firstComponentHash)
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTestWithHashStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class StreamTestWithHashStreamIdxColumn implements Persistable, Comparable<StreamTestWithHashStreamIdxColumn> {
        private final byte[] reference;

        public static StreamTestWithHashStreamIdxColumn of(byte[] reference) {
            return new StreamTestWithHashStreamIdxColumn(reference);
        }

        private StreamTestWithHashStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<StreamTestWithHashStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<StreamTestWithHashStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTestWithHashStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], StreamTestWithHashStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], StreamTestWithHashStreamIdxColumn>() {
                @Override
                public StreamTestWithHashStreamIdxColumn apply(byte[] row) {
                    return StreamTestWithHashStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<StreamTestWithHashStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<StreamTestWithHashStreamIdxColumn>() {
            @Override
            public StreamTestWithHashStreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return new StreamTestWithHashStreamIdxColumn(reference);
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
            StreamTestWithHashStreamIdxColumn other = (StreamTestWithHashStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(StreamTestWithHashStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface StreamTestWithHashStreamIdxTrigger {
        public void putStreamTestWithHashStreamIdx(Multimap<StreamTestWithHashStreamIdxRow, ? extends StreamTestWithHashStreamIdxColumnValue> newRows);
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
    public static final class StreamTestWithHashStreamIdxColumnValue implements ColumnValue<Long> {
        private final StreamTestWithHashStreamIdxColumn columnName;
        private final Long value;

        public static StreamTestWithHashStreamIdxColumnValue of(StreamTestWithHashStreamIdxColumn columnName, Long value) {
            return new StreamTestWithHashStreamIdxColumnValue(columnName, value);
        }

        private StreamTestWithHashStreamIdxColumnValue(StreamTestWithHashStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTestWithHashStreamIdxColumn getColumnName() {
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

        public static Function<StreamTestWithHashStreamIdxColumnValue, StreamTestWithHashStreamIdxColumn> getColumnNameFun() {
            return new Function<StreamTestWithHashStreamIdxColumnValue, StreamTestWithHashStreamIdxColumn>() {
                @Override
                public StreamTestWithHashStreamIdxColumn apply(StreamTestWithHashStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTestWithHashStreamIdxColumnValue, Long> getValueFun() {
            return new Function<StreamTestWithHashStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(StreamTestWithHashStreamIdxColumnValue columnValue) {
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

    public static final class StreamTestWithHashStreamIdxRowResult implements TypedRowResult {
        private final StreamTestWithHashStreamIdxRow rowName;
        private final ImmutableSet<StreamTestWithHashStreamIdxColumnValue> columnValues;

        public static StreamTestWithHashStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            StreamTestWithHashStreamIdxRow rowName = StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTestWithHashStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTestWithHashStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTestWithHashStreamIdxColumnValue.of(col, value));
            }
            return new StreamTestWithHashStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTestWithHashStreamIdxRowResult(StreamTestWithHashStreamIdxRow rowName, ImmutableSet<StreamTestWithHashStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTestWithHashStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<StreamTestWithHashStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTestWithHashStreamIdxRowResult, StreamTestWithHashStreamIdxRow> getRowNameFun() {
            return new Function<StreamTestWithHashStreamIdxRowResult, StreamTestWithHashStreamIdxRow>() {
                @Override
                public StreamTestWithHashStreamIdxRow apply(StreamTestWithHashStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTestWithHashStreamIdxRowResult, ImmutableSet<StreamTestWithHashStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTestWithHashStreamIdxRowResult, ImmutableSet<StreamTestWithHashStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTestWithHashStreamIdxColumnValue> apply(StreamTestWithHashStreamIdxRowResult rowResult) {
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
    public void delete(StreamTestWithHashStreamIdxRow row, StreamTestWithHashStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTestWithHashStreamIdxRow> rows) {
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTestWithHashStreamIdxRow rowName, Iterable<StreamTestWithHashStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(StreamTestWithHashStreamIdxRow rowName, StreamTestWithHashStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<StreamTestWithHashStreamIdxRow, ? extends StreamTestWithHashStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (StreamTestWithHashStreamIdxTrigger trigger : triggers) {
            trigger.putStreamTestWithHashStreamIdx(values);
        }
    }

    @Override
    public void putUnlessExists(StreamTestWithHashStreamIdxRow rowName, Iterable<StreamTestWithHashStreamIdxColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(StreamTestWithHashStreamIdxRow rowName, StreamTestWithHashStreamIdxColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(Multimap<StreamTestWithHashStreamIdxRow, ? extends StreamTestWithHashStreamIdxColumnValue> rows) {
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> toGet = Multimaps.transformValues(rows, StreamTestWithHashStreamIdxColumnValue.getColumnNameFun());
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> existing = get(toGet);
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> toPut = HashMultimap.create();
        for (Entry<StreamTestWithHashStreamIdxRow, ? extends StreamTestWithHashStreamIdxColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> values) {
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTestWithHashStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTestWithHashStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> get(Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTestWithHashStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestWithHashStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<StreamTestWithHashStreamIdxColumnValue> getRowColumns(StreamTestWithHashStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<StreamTestWithHashStreamIdxColumnValue> getRowColumns(StreamTestWithHashStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTestWithHashStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestWithHashStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTestWithHashStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestWithHashStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> getRowsMultimap(Iterable<StreamTestWithHashStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> getRowsMultimapInternal(Iterable<StreamTestWithHashStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTestWithHashStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTestWithHashStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<StreamTestWithHashStreamIdxRow, BatchingVisitable<StreamTestWithHashStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestWithHashStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<StreamTestWithHashStreamIdxRow, BatchingVisitable<StreamTestWithHashStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<StreamTestWithHashStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = StreamTestWithHashStreamIdxColumnValue.hydrateValue(result.getValue());
                return StreamTestWithHashStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxColumnValue>> getRowsColumnRange(Iterable<StreamTestWithHashStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            StreamTestWithHashStreamIdxColumn col = StreamTestWithHashStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = StreamTestWithHashStreamIdxColumnValue.hydrateValue(e.getValue());
            StreamTestWithHashStreamIdxColumnValue colValue = StreamTestWithHashStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<StreamTestWithHashStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<StreamTestWithHashStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTestWithHashStreamIdxRowResult>() {
            @Override
            public StreamTestWithHashStreamIdxRowResult apply(RowResult<byte[]> input) {
                return StreamTestWithHashStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "P2jOITY87SvZmGEA7Bgc2g==";
}
