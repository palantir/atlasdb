package com.palantir.atlasdb.schema.stream.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutableExpiringTable;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.atlasdb.table.api.TypedRowResult;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.persist.Persistable;
import com.palantir.common.persist.Persistables;
import com.palantir.common.proxy.AsyncProxy;


public final class StreamTest2StreamIdxTable implements
        AtlasDbDynamicMutableExpiringTable<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxColumn,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxColumnValue,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxRowResult> {
    private final Transaction t;
    private final List<StreamTest2StreamIdxTrigger> triggers;
    private final static String rawTableName = "stream_test_2_stream_idx";
    private final String tableName;
    private final Namespace namespace;

    static StreamTest2StreamIdxTable of(Transaction t, Namespace namespace) {
        return new StreamTest2StreamIdxTable(t, namespace, ImmutableList.<StreamTest2StreamIdxTrigger>of());
    }

    static StreamTest2StreamIdxTable of(Transaction t, Namespace namespace, StreamTest2StreamIdxTrigger trigger, StreamTest2StreamIdxTrigger... triggers) {
        return new StreamTest2StreamIdxTable(t, namespace, ImmutableList.<StreamTest2StreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTest2StreamIdxTable of(Transaction t, Namespace namespace, List<StreamTest2StreamIdxTrigger> triggers) {
        return new StreamTest2StreamIdxTable(t, namespace, triggers);
    }

    private StreamTest2StreamIdxTable(Transaction t, Namespace namespace, List<StreamTest2StreamIdxTrigger> triggers) {
        this.t = t;
        this.tableName = namespace.getName() + "." + rawTableName;
        this.triggers = triggers;
        this.namespace = namespace;
    }

    public static String getRawTableName() {
        return rawTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    /**
     * <pre>
     * StreamTest2StreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamIdxRow implements Persistable, Comparable<StreamTest2StreamIdxRow> {
        private final long id;

        public static StreamTest2StreamIdxRow of(long id) {
            return new StreamTest2StreamIdxRow(id);
        }

        private StreamTest2StreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTest2StreamIdxRow, Long> getIdFun() {
            return new Function<StreamTest2StreamIdxRow, Long>() {
                @Override
                public Long apply(StreamTest2StreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTest2StreamIdxRow> fromIdFun() {
            return new Function<Long, StreamTest2StreamIdxRow>() {
                @Override
                public StreamTest2StreamIdxRow apply(Long row) {
                    return StreamTest2StreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<StreamTest2StreamIdxRow> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamIdxRow>() {
            @Override
            public StreamTest2StreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new StreamTest2StreamIdxRow(id);
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
            StreamTest2StreamIdxRow other = (StreamTest2StreamIdxRow) obj;
            return Objects.equal(id, other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(StreamTest2StreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTest2StreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamIdxColumn implements Persistable, Comparable<StreamTest2StreamIdxColumn> {
        private final byte[] reference;

        public static StreamTest2StreamIdxColumn of(byte[] reference) {
            return new StreamTest2StreamIdxColumn(reference);
        }

        private StreamTest2StreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<StreamTest2StreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<StreamTest2StreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTest2StreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], StreamTest2StreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], StreamTest2StreamIdxColumn>() {
                @Override
                public StreamTest2StreamIdxColumn apply(byte[] row) {
                    return StreamTest2StreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<StreamTest2StreamIdxColumn> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamIdxColumn>() {
            @Override
            public StreamTest2StreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return new StreamTest2StreamIdxColumn(reference);
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
            StreamTest2StreamIdxColumn other = (StreamTest2StreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(StreamTest2StreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface StreamTest2StreamIdxTrigger {
        public void putStreamTest2StreamIdx(Multimap<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> newRows);
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
    public static final class StreamTest2StreamIdxColumnValue implements ColumnValue<Long> {
        private final StreamTest2StreamIdxColumn columnName;
        private final Long value;

        public static StreamTest2StreamIdxColumnValue of(StreamTest2StreamIdxColumn columnName, Long value) {
            return new StreamTest2StreamIdxColumnValue(columnName, value);
        }

        private StreamTest2StreamIdxColumnValue(StreamTest2StreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTest2StreamIdxColumn getColumnName() {
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

        public static Function<StreamTest2StreamIdxColumnValue, StreamTest2StreamIdxColumn> getColumnNameFun() {
            return new Function<StreamTest2StreamIdxColumnValue, StreamTest2StreamIdxColumn>() {
                @Override
                public StreamTest2StreamIdxColumn apply(StreamTest2StreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTest2StreamIdxColumnValue, Long> getValueFun() {
            return new Function<StreamTest2StreamIdxColumnValue, Long>() {
                @Override
                public Long apply(StreamTest2StreamIdxColumnValue columnValue) {
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

    public static final class StreamTest2StreamIdxRowResult implements TypedRowResult {
        private final StreamTest2StreamIdxRow rowName;
        private final ImmutableSet<StreamTest2StreamIdxColumnValue> columnValues;

        public static StreamTest2StreamIdxRowResult of(RowResult<byte[]> rowResult) {
            StreamTest2StreamIdxRow rowName = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTest2StreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTest2StreamIdxColumnValue.of(col, value));
            }
            return new StreamTest2StreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTest2StreamIdxRowResult(StreamTest2StreamIdxRow rowName, ImmutableSet<StreamTest2StreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTest2StreamIdxRow getRowName() {
            return rowName;
        }

        public Set<StreamTest2StreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTest2StreamIdxRowResult, StreamTest2StreamIdxRow> getRowNameFun() {
            return new Function<StreamTest2StreamIdxRowResult, StreamTest2StreamIdxRow>() {
                @Override
                public StreamTest2StreamIdxRow apply(StreamTest2StreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTest2StreamIdxRowResult, ImmutableSet<StreamTest2StreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTest2StreamIdxRowResult, ImmutableSet<StreamTest2StreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTest2StreamIdxColumnValue> apply(StreamTest2StreamIdxRowResult rowResult) {
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
    public void delete(StreamTest2StreamIdxRow row, StreamTest2StreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTest2StreamIdxRow> rows) {
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> values) {
        t.delete(tableName, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTest2StreamIdxRow rowName, Iterable<StreamTest2StreamIdxColumnValue> values, long duration, TimeUnit unit) {
        put(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(long duration, TimeUnit unit, StreamTest2StreamIdxRow rowName, StreamTest2StreamIdxColumnValue... values) {
        put(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(Multimap<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> values, long duration, TimeUnit unit) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(values, duration, unit));
        for (StreamTest2StreamIdxTrigger trigger : triggers) {
            trigger.putStreamTest2StreamIdx(values);
        }
    }

    @Override
    public void putUnlessExists(StreamTest2StreamIdxRow rowName, Iterable<StreamTest2StreamIdxColumnValue> values, long duration, TimeUnit unit) {
        putUnlessExists(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void putUnlessExists(long duration, TimeUnit unit, StreamTest2StreamIdxRow rowName, StreamTest2StreamIdxColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void putUnlessExists(Multimap<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> rows, long duration, TimeUnit unit) {
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> toGet = Multimaps.transformValues(rows, StreamTest2StreamIdxColumnValue.getColumnNameFun());
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> existing = get(toGet);
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> toPut = HashMultimap.create();
        for (Entry<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut, duration, unit);
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTest2StreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTest2StreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> get(Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableName, rawCells);
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTest2StreamIdxRow row = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsync(final Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> cells, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> call() {
                return get(cells);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    @Override
    public List<StreamTest2StreamIdxColumnValue> getRowColumns(StreamTest2StreamIdxRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamIdxColumnValue> getRowColumns(StreamTest2StreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTest2StreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTest2StreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsyncRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsyncRowsMultimap(final Iterable<StreamTest2StreamIdxRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimapInternal(Iterable<StreamTest2StreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTest2StreamIdxRow row = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTest2StreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTest2StreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTest2StreamIdxRowResult>() {
            @Override
            public StreamTest2StreamIdxRowResult apply(RowResult<byte[]> input) {
                return StreamTest2StreamIdxRowResult.of(input);
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

    static String __CLASS_HASH = "8G1RcLlO7SjAppVbR6lM8A==";
}
