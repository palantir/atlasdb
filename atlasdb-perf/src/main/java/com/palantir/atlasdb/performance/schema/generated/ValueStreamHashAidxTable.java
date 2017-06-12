package com.palantir.atlasdb.performance.schema.generated;

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
public final class ValueStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<ValueStreamHashAidxTable.ValueStreamHashAidxRow,
                                                ValueStreamHashAidxTable.ValueStreamHashAidxColumn,
                                                ValueStreamHashAidxTable.ValueStreamHashAidxColumnValue,
                                                ValueStreamHashAidxTable.ValueStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<ValueStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "blob_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static ValueStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new ValueStreamHashAidxTable(t, namespace, ImmutableList.<ValueStreamHashAidxTrigger>of());
    }

    static ValueStreamHashAidxTable of(Transaction t, Namespace namespace, ValueStreamHashAidxTrigger trigger, ValueStreamHashAidxTrigger... triggers) {
        return new ValueStreamHashAidxTable(t, namespace, ImmutableList.<ValueStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static ValueStreamHashAidxTable of(Transaction t, Namespace namespace, List<ValueStreamHashAidxTrigger> triggers) {
        return new ValueStreamHashAidxTable(t, namespace, triggers);
    }

    private ValueStreamHashAidxTable(Transaction t, Namespace namespace, List<ValueStreamHashAidxTrigger> triggers) {
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
     * ValueStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class ValueStreamHashAidxRow implements Persistable, Comparable<ValueStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static ValueStreamHashAidxRow of(Sha256Hash hash) {
            return new ValueStreamHashAidxRow(hash);
        }

        private ValueStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<ValueStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<ValueStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(ValueStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, ValueStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, ValueStreamHashAidxRow>() {
                @Override
                public ValueStreamHashAidxRow apply(Sha256Hash row) {
                    return ValueStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<ValueStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<ValueStreamHashAidxRow>() {
            @Override
            public ValueStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new ValueStreamHashAidxRow(hash);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hash", hash)
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
            ValueStreamHashAidxRow other = (ValueStreamHashAidxRow) obj;
            return Objects.equal(hash, other.hash);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(ValueStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * ValueStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class ValueStreamHashAidxColumn implements Persistable, Comparable<ValueStreamHashAidxColumn> {
        private final long streamId;

        public static ValueStreamHashAidxColumn of(long streamId) {
            return new ValueStreamHashAidxColumn(streamId);
        }

        private ValueStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<ValueStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<ValueStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(ValueStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, ValueStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, ValueStreamHashAidxColumn>() {
                @Override
                public ValueStreamHashAidxColumn apply(Long row) {
                    return ValueStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<ValueStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<ValueStreamHashAidxColumn>() {
            @Override
            public ValueStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new ValueStreamHashAidxColumn(streamId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("streamId", streamId)
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
            ValueStreamHashAidxColumn other = (ValueStreamHashAidxColumn) obj;
            return Objects.equal(streamId, other.streamId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(ValueStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface ValueStreamHashAidxTrigger {
        public void putValueStreamHashAidx(Multimap<ValueStreamHashAidxRow, ? extends ValueStreamHashAidxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long streamId};
     * }
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class ValueStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final ValueStreamHashAidxColumn columnName;
        private final Long value;

        public static ValueStreamHashAidxColumnValue of(ValueStreamHashAidxColumn columnName, Long value) {
            return new ValueStreamHashAidxColumnValue(columnName, value);
        }

        private ValueStreamHashAidxColumnValue(ValueStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public ValueStreamHashAidxColumn getColumnName() {
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

        public static Function<ValueStreamHashAidxColumnValue, ValueStreamHashAidxColumn> getColumnNameFun() {
            return new Function<ValueStreamHashAidxColumnValue, ValueStreamHashAidxColumn>() {
                @Override
                public ValueStreamHashAidxColumn apply(ValueStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<ValueStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<ValueStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(ValueStreamHashAidxColumnValue columnValue) {
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

    public static final class ValueStreamHashAidxRowResult implements TypedRowResult {
        private final ValueStreamHashAidxRow rowName;
        private final ImmutableSet<ValueStreamHashAidxColumnValue> columnValues;

        public static ValueStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            ValueStreamHashAidxRow rowName = ValueStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<ValueStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = ValueStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(ValueStreamHashAidxColumnValue.of(col, value));
            }
            return new ValueStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private ValueStreamHashAidxRowResult(ValueStreamHashAidxRow rowName, ImmutableSet<ValueStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public ValueStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<ValueStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<ValueStreamHashAidxRowResult, ValueStreamHashAidxRow> getRowNameFun() {
            return new Function<ValueStreamHashAidxRowResult, ValueStreamHashAidxRow>() {
                @Override
                public ValueStreamHashAidxRow apply(ValueStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<ValueStreamHashAidxRowResult, ImmutableSet<ValueStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<ValueStreamHashAidxRowResult, ImmutableSet<ValueStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<ValueStreamHashAidxColumnValue> apply(ValueStreamHashAidxRowResult rowResult) {
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
    public void delete(ValueStreamHashAidxRow row, ValueStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<ValueStreamHashAidxRow> rows) {
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(ValueStreamHashAidxRow rowName, Iterable<ValueStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(ValueStreamHashAidxRow rowName, ValueStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<ValueStreamHashAidxRow, ? extends ValueStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (ValueStreamHashAidxTrigger trigger : triggers) {
            trigger.putValueStreamHashAidx(values);
        }
    }

    @Override
    public void putUnlessExists(ValueStreamHashAidxRow rowName, Iterable<ValueStreamHashAidxColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(ValueStreamHashAidxRow rowName, ValueStreamHashAidxColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(Multimap<ValueStreamHashAidxRow, ? extends ValueStreamHashAidxColumnValue> rows) {
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> toGet = Multimaps.transformValues(rows, ValueStreamHashAidxColumnValue.getColumnNameFun());
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> existing = get(toGet);
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> toPut = HashMultimap.create();
        for (Entry<ValueStreamHashAidxRow, ? extends ValueStreamHashAidxColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> values) {
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<ValueStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(ValueStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> get(Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                ValueStreamHashAidxRow row = ValueStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = ValueStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, ValueStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<ValueStreamHashAidxColumnValue> getRowColumns(ValueStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<ValueStreamHashAidxColumnValue> getRowColumns(ValueStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<ValueStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = ValueStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(ValueStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> getRowsMultimap(Iterable<ValueStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> getRowsMultimap(Iterable<ValueStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<ValueStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            ValueStreamHashAidxRow row = ValueStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = ValueStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, ValueStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<ValueStreamHashAidxRow, BatchingVisitable<ValueStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<ValueStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<ValueStreamHashAidxRow, BatchingVisitable<ValueStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            ValueStreamHashAidxRow row = ValueStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<ValueStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = ValueStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return ValueStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<ValueStreamHashAidxRow, ValueStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<ValueStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            ValueStreamHashAidxRow row = ValueStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            ValueStreamHashAidxColumn col = ValueStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = ValueStreamHashAidxColumnValue.hydrateValue(e.getValue());
            ValueStreamHashAidxColumnValue colValue = ValueStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<ValueStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<ValueStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, ValueStreamHashAidxRowResult>() {
            @Override
            public ValueStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return ValueStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "6fowX/ukMQ6UNoGKcOPGFg==";
}
