package com.palantir.atlasdb.schema.cleanup;

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
public final class CleanupQueueTable implements
        AtlasDbDynamicMutablePersistentTable<CleanupQueueTable.CleanupQueueRow,
                                                CleanupQueueTable.CleanupQueueColumn,
                                                CleanupQueueTable.CleanupQueueColumnValue,
                                                CleanupQueueTable.CleanupQueueRowResult> {
    private final Transaction t;
    private final List<CleanupQueueTrigger> triggers;
    private final static String rawTableName = "cleanup_queue";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static CleanupQueueTable of(Transaction t, Namespace namespace) {
        return new CleanupQueueTable(t, namespace, ImmutableList.<CleanupQueueTrigger>of());
    }

    static CleanupQueueTable of(Transaction t, Namespace namespace, CleanupQueueTrigger trigger, CleanupQueueTrigger... triggers) {
        return new CleanupQueueTable(t, namespace, ImmutableList.<CleanupQueueTrigger>builder().add(trigger).add(triggers).build());
    }

    static CleanupQueueTable of(Transaction t, Namespace namespace, List<CleanupQueueTrigger> triggers) {
        return new CleanupQueueTable(t, namespace, triggers);
    }

    private CleanupQueueTable(Transaction t, Namespace namespace, List<CleanupQueueTrigger> triggers) {
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
     * CleanupQueueRow {
     *   {@literal byte[] queueKey};
     * }
     * </pre>
     */
    public static final class CleanupQueueRow implements Persistable, Comparable<CleanupQueueRow> {
        private final byte[] queueKey;

        public static CleanupQueueRow of(byte[] queueKey) {
            return new CleanupQueueRow(queueKey);
        }

        private CleanupQueueRow(byte[] queueKey) {
            this.queueKey = queueKey;
        }

        public byte[] getQueueKey() {
            return queueKey;
        }

        public static Function<CleanupQueueRow, byte[]> getQueueKeyFun() {
            return new Function<CleanupQueueRow, byte[]>() {
                @Override
                public byte[] apply(CleanupQueueRow row) {
                    return row.queueKey;
                }
            };
        }

        public static Function<byte[], CleanupQueueRow> fromQueueKeyFun() {
            return new Function<byte[], CleanupQueueRow>() {
                @Override
                public CleanupQueueRow apply(byte[] row) {
                    return CleanupQueueRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] queueKeyBytes = queueKey;
            return EncodingUtils.add(queueKeyBytes);
        }

        public static final Hydrator<CleanupQueueRow> BYTES_HYDRATOR = new Hydrator<CleanupQueueRow>() {
            @Override
            public CleanupQueueRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] queueKey = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new CleanupQueueRow(queueKey);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("queueKey", queueKey)
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
            CleanupQueueRow other = (CleanupQueueRow) obj;
            return Arrays.equals(queueKey, other.queueKey);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(queueKey);
        }

        @Override
        public int compareTo(CleanupQueueRow o) {
            return ComparisonChain.start()
                .compare(this.queueKey, o.queueKey, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    /**
     * <pre>
     * CleanupQueueColumn {
     *   {@literal Long offset};
     * }
     * </pre>
     */
    public static final class CleanupQueueColumn implements Persistable, Comparable<CleanupQueueColumn> {
        private final long offset;

        public static CleanupQueueColumn of(long offset) {
            return new CleanupQueueColumn(offset);
        }

        private CleanupQueueColumn(long offset) {
            this.offset = offset;
        }

        public long getOffset() {
            return offset;
        }

        public static Function<CleanupQueueColumn, Long> getOffsetFun() {
            return new Function<CleanupQueueColumn, Long>() {
                @Override
                public Long apply(CleanupQueueColumn row) {
                    return row.offset;
                }
            };
        }

        public static Function<Long, CleanupQueueColumn> fromOffsetFun() {
            return new Function<Long, CleanupQueueColumn>() {
                @Override
                public CleanupQueueColumn apply(Long row) {
                    return CleanupQueueColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] offsetBytes = EncodingUtils.encodeUnsignedVarLong(offset);
            return EncodingUtils.add(offsetBytes);
        }

        public static final Hydrator<CleanupQueueColumn> BYTES_HYDRATOR = new Hydrator<CleanupQueueColumn>() {
            @Override
            public CleanupQueueColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long offset = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(offset);
                return new CleanupQueueColumn(offset);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("offset", offset)
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
            CleanupQueueColumn other = (CleanupQueueColumn) obj;
            return Objects.equal(offset, other.offset);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(offset);
        }

        @Override
        public int compareTo(CleanupQueueColumn o) {
            return ComparisonChain.start()
                .compare(this.offset, o.offset)
                .result();
        }
    }

    public interface CleanupQueueTrigger {
        public void putCleanupQueue(Multimap<CleanupQueueRow, ? extends CleanupQueueColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long offset};
     * }
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class CleanupQueueColumnValue implements ColumnValue<byte[]> {
        private final CleanupQueueColumn columnName;
        private final byte[] value;

        public static CleanupQueueColumnValue of(CleanupQueueColumn columnName, byte[] value) {
            return new CleanupQueueColumnValue(columnName, value);
        }

        private CleanupQueueColumnValue(CleanupQueueColumn columnName, byte[] value) {
            this.columnName = columnName;
            this.value = value;
        }

        public CleanupQueueColumn getColumnName() {
            return columnName;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value;
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static byte[] hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            return EncodingUtils.getBytesFromOffsetToEnd(bytes, 0);
        }

        public static Function<CleanupQueueColumnValue, CleanupQueueColumn> getColumnNameFun() {
            return new Function<CleanupQueueColumnValue, CleanupQueueColumn>() {
                @Override
                public CleanupQueueColumn apply(CleanupQueueColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<CleanupQueueColumnValue, byte[]> getValueFun() {
            return new Function<CleanupQueueColumnValue, byte[]>() {
                @Override
                public byte[] apply(CleanupQueueColumnValue columnValue) {
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

    public static final class CleanupQueueRowResult implements TypedRowResult {
        private final CleanupQueueRow rowName;
        private final ImmutableSet<CleanupQueueColumnValue> columnValues;

        public static CleanupQueueRowResult of(RowResult<byte[]> rowResult) {
            CleanupQueueRow rowName = CleanupQueueRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<CleanupQueueColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] value = CleanupQueueColumnValue.hydrateValue(e.getValue());
                columnValues.add(CleanupQueueColumnValue.of(col, value));
            }
            return new CleanupQueueRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private CleanupQueueRowResult(CleanupQueueRow rowName, ImmutableSet<CleanupQueueColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public CleanupQueueRow getRowName() {
            return rowName;
        }

        public Set<CleanupQueueColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<CleanupQueueRowResult, CleanupQueueRow> getRowNameFun() {
            return new Function<CleanupQueueRowResult, CleanupQueueRow>() {
                @Override
                public CleanupQueueRow apply(CleanupQueueRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<CleanupQueueRowResult, ImmutableSet<CleanupQueueColumnValue>> getColumnValuesFun() {
            return new Function<CleanupQueueRowResult, ImmutableSet<CleanupQueueColumnValue>>() {
                @Override
                public ImmutableSet<CleanupQueueColumnValue> apply(CleanupQueueRowResult rowResult) {
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
    public void delete(CleanupQueueRow row, CleanupQueueColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<CleanupQueueRow> rows) {
        Multimap<CleanupQueueRow, CleanupQueueColumn> toRemove = HashMultimap.create();
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> result = getRowsMultimap(rows);
        for (Entry<CleanupQueueRow, CleanupQueueColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<CleanupQueueRow, CleanupQueueColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(CleanupQueueRow rowName, Iterable<CleanupQueueColumnValue> values) {
        put(ImmutableMultimap.<CleanupQueueRow, CleanupQueueColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(CleanupQueueRow rowName, CleanupQueueColumnValue... values) {
        put(ImmutableMultimap.<CleanupQueueRow, CleanupQueueColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<CleanupQueueRow, ? extends CleanupQueueColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (CleanupQueueTrigger trigger : triggers) {
            trigger.putCleanupQueue(values);
        }
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(CleanupQueueRow rowName, Iterable<CleanupQueueColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<CleanupQueueRow, CleanupQueueColumnValue>builder().putAll(rowName, values).build());
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(CleanupQueueRow rowName, CleanupQueueColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<CleanupQueueRow, CleanupQueueColumnValue>builder().putAll(rowName, values).build());
    }

    /** @deprecated Use separate read and write in a single transaction instead. */
    @Deprecated
    @Override
    public void putUnlessExists(Multimap<CleanupQueueRow, ? extends CleanupQueueColumnValue> rows) {
        Multimap<CleanupQueueRow, CleanupQueueColumn> toGet = Multimaps.transformValues(rows, CleanupQueueColumnValue.getColumnNameFun());
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> existing = get(toGet);
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> toPut = HashMultimap.create();
        for (Entry<CleanupQueueRow, ? extends CleanupQueueColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<CleanupQueueRow, CleanupQueueColumn> values) {
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<CleanupQueueRow, CleanupQueueColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<CleanupQueueRow, CleanupQueueColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<CleanupQueueColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(CleanupQueueColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<CleanupQueueRow, CleanupQueueColumnValue> get(Multimap<CleanupQueueRow, CleanupQueueColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                CleanupQueueRow row = CleanupQueueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                byte[] val = CleanupQueueColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, CleanupQueueColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<CleanupQueueColumnValue> getRowColumns(CleanupQueueRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<CleanupQueueColumnValue> getRowColumns(CleanupQueueRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<CleanupQueueColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = CleanupQueueColumnValue.hydrateValue(e.getValue());
                ret.add(CleanupQueueColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<CleanupQueueRow, CleanupQueueColumnValue> getRowsMultimap(Iterable<CleanupQueueRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<CleanupQueueRow, CleanupQueueColumnValue> getRowsMultimap(Iterable<CleanupQueueRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<CleanupQueueRow, CleanupQueueColumnValue> getRowsMultimapInternal(Iterable<CleanupQueueRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<CleanupQueueRow, CleanupQueueColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<CleanupQueueRow, CleanupQueueColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            CleanupQueueRow row = CleanupQueueRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                byte[] val = CleanupQueueColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, CleanupQueueColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<CleanupQueueRow, BatchingVisitable<CleanupQueueColumnValue>> getRowsColumnRange(Iterable<CleanupQueueRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<CleanupQueueRow, BatchingVisitable<CleanupQueueColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            CleanupQueueRow row = CleanupQueueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<CleanupQueueColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                byte[] val = CleanupQueueColumnValue.hydrateValue(result.getValue());
                return CleanupQueueColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<CleanupQueueRow, CleanupQueueColumnValue>> getRowsColumnRange(Iterable<CleanupQueueRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            CleanupQueueRow row = CleanupQueueRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            CleanupQueueColumn col = CleanupQueueColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            byte[] val = CleanupQueueColumnValue.hydrateValue(e.getValue());
            CleanupQueueColumnValue colValue = CleanupQueueColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<CleanupQueueRowResult> getRange(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            range = range.getBuilder().retainColumns(allColumns).build();
        }
        return BatchingVisitables.transform(t.getRange(tableRef, range), new Function<RowResult<byte[]>, CleanupQueueRowResult>() {
            @Override
            public CleanupQueueRowResult apply(RowResult<byte[]> input) {
                return CleanupQueueRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<CleanupQueueRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, ranges);
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<CleanupQueueRowResult>>() {
            @Override
            public BatchingVisitable<CleanupQueueRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, CleanupQueueRowResult>() {
                    @Override
                    public CleanupQueueRowResult apply(RowResult<byte[]> row) {
                        return CleanupQueueRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<CleanupQueueRowResult>, T> visitableProcessor) {
        return t.getRanges(tableRef, ranges, concurrencyLevel,
                (rangeRequest, visitable) -> visitableProcessor.apply(rangeRequest, BatchingVisitables.transform(visitable, CleanupQueueRowResult::of)));
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<CleanupQueueRowResult>, T> visitableProcessor) {
        return t.getRanges(tableRef, ranges,
                (rangeRequest, visitable) -> visitableProcessor.apply(rangeRequest, BatchingVisitables.transform(visitable, CleanupQueueRowResult::of)));
    }

    public Stream<BatchingVisitable<CleanupQueueRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, ranges);
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, CleanupQueueRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<CleanupQueueRowResult>, RuntimeException>() {
            @Override
            public boolean visit(List<CleanupQueueRowResult> rowResults) {
                Multimap<CleanupQueueRow, CleanupQueueColumn> toRemove = HashMultimap.create();
                for (CleanupQueueRowResult rowResult : rowResults) {
                    for (CleanupQueueColumnValue columnValue : rowResult.getColumnValues()) {
                        toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                    }
                }
                delete(toRemove);
                return true;
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
    static String __CLASS_HASH = "54NtSCMiDzP9YNeQ3OhayA==";
}
