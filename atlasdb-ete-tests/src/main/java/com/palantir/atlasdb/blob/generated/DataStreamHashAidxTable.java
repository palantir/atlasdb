package com.palantir.atlasdb.blob.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import com.google.common.base.Optional;
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
public final class DataStreamHashAidxTable implements
        AtlasDbDynamicMutablePersistentTable<DataStreamHashAidxTable.DataStreamHashAidxRow,
                                                DataStreamHashAidxTable.DataStreamHashAidxColumn,
                                                DataStreamHashAidxTable.DataStreamHashAidxColumnValue,
                                                DataStreamHashAidxTable.DataStreamHashAidxRowResult> {
    private final Transaction t;
    private final List<DataStreamHashAidxTrigger> triggers;
    private final static String rawTableName = "data_stream_hash_aidx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static DataStreamHashAidxTable of(Transaction t, Namespace namespace) {
        return new DataStreamHashAidxTable(t, namespace, ImmutableList.<DataStreamHashAidxTrigger>of());
    }

    static DataStreamHashAidxTable of(Transaction t, Namespace namespace, DataStreamHashAidxTrigger trigger, DataStreamHashAidxTrigger... triggers) {
        return new DataStreamHashAidxTable(t, namespace, ImmutableList.<DataStreamHashAidxTrigger>builder().add(trigger).add(triggers).build());
    }

    static DataStreamHashAidxTable of(Transaction t, Namespace namespace, List<DataStreamHashAidxTrigger> triggers) {
        return new DataStreamHashAidxTable(t, namespace, triggers);
    }

    private DataStreamHashAidxTable(Transaction t, Namespace namespace, List<DataStreamHashAidxTrigger> triggers) {
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
     * DataStreamHashAidxRow {
     *   {@literal Sha256Hash hash};
     * }
     * </pre>
     */
    public static final class DataStreamHashAidxRow implements Persistable, Comparable<DataStreamHashAidxRow> {
        private final Sha256Hash hash;

        public static DataStreamHashAidxRow of(Sha256Hash hash) {
            return new DataStreamHashAidxRow(hash);
        }

        private DataStreamHashAidxRow(Sha256Hash hash) {
            this.hash = hash;
        }

        public Sha256Hash getHash() {
            return hash;
        }

        public static Function<DataStreamHashAidxRow, Sha256Hash> getHashFun() {
            return new Function<DataStreamHashAidxRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(DataStreamHashAidxRow row) {
                    return row.hash;
                }
            };
        }

        public static Function<Sha256Hash, DataStreamHashAidxRow> fromHashFun() {
            return new Function<Sha256Hash, DataStreamHashAidxRow>() {
                @Override
                public DataStreamHashAidxRow apply(Sha256Hash row) {
                    return DataStreamHashAidxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] hashBytes = hash.getBytes();
            return EncodingUtils.add(hashBytes);
        }

        public static final Hydrator<DataStreamHashAidxRow> BYTES_HYDRATOR = new Hydrator<DataStreamHashAidxRow>() {
            @Override
            public DataStreamHashAidxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Sha256Hash hash = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                return new DataStreamHashAidxRow(hash);
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
            DataStreamHashAidxRow other = (DataStreamHashAidxRow) obj;
            return Objects.equals(hash, other.hash);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(hash);
        }

        @Override
        public int compareTo(DataStreamHashAidxRow o) {
            return ComparisonChain.start()
                .compare(this.hash, o.hash)
                .result();
        }
    }

    /**
     * <pre>
     * DataStreamHashAidxColumn {
     *   {@literal Long streamId};
     * }
     * </pre>
     */
    public static final class DataStreamHashAidxColumn implements Persistable, Comparable<DataStreamHashAidxColumn> {
        private final long streamId;

        public static DataStreamHashAidxColumn of(long streamId) {
            return new DataStreamHashAidxColumn(streamId);
        }

        private DataStreamHashAidxColumn(long streamId) {
            this.streamId = streamId;
        }

        public long getStreamId() {
            return streamId;
        }

        public static Function<DataStreamHashAidxColumn, Long> getStreamIdFun() {
            return new Function<DataStreamHashAidxColumn, Long>() {
                @Override
                public Long apply(DataStreamHashAidxColumn row) {
                    return row.streamId;
                }
            };
        }

        public static Function<Long, DataStreamHashAidxColumn> fromStreamIdFun() {
            return new Function<Long, DataStreamHashAidxColumn>() {
                @Override
                public DataStreamHashAidxColumn apply(Long row) {
                    return DataStreamHashAidxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] streamIdBytes = EncodingUtils.encodeUnsignedVarLong(streamId);
            return EncodingUtils.add(streamIdBytes);
        }

        public static final Hydrator<DataStreamHashAidxColumn> BYTES_HYDRATOR = new Hydrator<DataStreamHashAidxColumn>() {
            @Override
            public DataStreamHashAidxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long streamId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(streamId);
                return new DataStreamHashAidxColumn(streamId);
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
            DataStreamHashAidxColumn other = (DataStreamHashAidxColumn) obj;
            return Objects.equals(streamId, other.streamId);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(streamId);
        }

        @Override
        public int compareTo(DataStreamHashAidxColumn o) {
            return ComparisonChain.start()
                .compare(this.streamId, o.streamId)
                .result();
        }
    }

    public interface DataStreamHashAidxTrigger {
        public void putDataStreamHashAidx(Multimap<DataStreamHashAidxRow, ? extends DataStreamHashAidxColumnValue> newRows);
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
    public static final class DataStreamHashAidxColumnValue implements ColumnValue<Long> {
        private final DataStreamHashAidxColumn columnName;
        private final Long value;

        public static DataStreamHashAidxColumnValue of(DataStreamHashAidxColumn columnName, Long value) {
            return new DataStreamHashAidxColumnValue(columnName, value);
        }

        private DataStreamHashAidxColumnValue(DataStreamHashAidxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public DataStreamHashAidxColumn getColumnName() {
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

        public static Function<DataStreamHashAidxColumnValue, DataStreamHashAidxColumn> getColumnNameFun() {
            return new Function<DataStreamHashAidxColumnValue, DataStreamHashAidxColumn>() {
                @Override
                public DataStreamHashAidxColumn apply(DataStreamHashAidxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<DataStreamHashAidxColumnValue, Long> getValueFun() {
            return new Function<DataStreamHashAidxColumnValue, Long>() {
                @Override
                public Long apply(DataStreamHashAidxColumnValue columnValue) {
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

    public static final class DataStreamHashAidxRowResult implements TypedRowResult {
        private final DataStreamHashAidxRow rowName;
        private final ImmutableSet<DataStreamHashAidxColumnValue> columnValues;

        public static DataStreamHashAidxRowResult of(RowResult<byte[]> rowResult) {
            DataStreamHashAidxRow rowName = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<DataStreamHashAidxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = DataStreamHashAidxColumnValue.hydrateValue(e.getValue());
                columnValues.add(DataStreamHashAidxColumnValue.of(col, value));
            }
            return new DataStreamHashAidxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private DataStreamHashAidxRowResult(DataStreamHashAidxRow rowName, ImmutableSet<DataStreamHashAidxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public DataStreamHashAidxRow getRowName() {
            return rowName;
        }

        public Set<DataStreamHashAidxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<DataStreamHashAidxRowResult, DataStreamHashAidxRow> getRowNameFun() {
            return new Function<DataStreamHashAidxRowResult, DataStreamHashAidxRow>() {
                @Override
                public DataStreamHashAidxRow apply(DataStreamHashAidxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<DataStreamHashAidxRowResult, ImmutableSet<DataStreamHashAidxColumnValue>> getColumnValuesFun() {
            return new Function<DataStreamHashAidxRowResult, ImmutableSet<DataStreamHashAidxColumnValue>>() {
                @Override
                public ImmutableSet<DataStreamHashAidxColumnValue> apply(DataStreamHashAidxRowResult rowResult) {
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
    public void delete(DataStreamHashAidxRow row, DataStreamHashAidxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<DataStreamHashAidxRow> rows) {
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumn> toRemove = HashMultimap.create();
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> result = getRowsMultimap(rows);
        for (Entry<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(DataStreamHashAidxRow rowName, Iterable<DataStreamHashAidxColumnValue> values) {
        put(ImmutableMultimap.<DataStreamHashAidxRow, DataStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(DataStreamHashAidxRow rowName, DataStreamHashAidxColumnValue... values) {
        put(ImmutableMultimap.<DataStreamHashAidxRow, DataStreamHashAidxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<DataStreamHashAidxRow, ? extends DataStreamHashAidxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (DataStreamHashAidxTrigger trigger : triggers) {
            trigger.putDataStreamHashAidx(values);
        }
    }

    @Override
    public void touch(Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumn> values) {
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<DataStreamHashAidxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(DataStreamHashAidxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> get(Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> rowMap = ArrayListMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                DataStreamHashAidxRow row = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = DataStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DataStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<DataStreamHashAidxColumnValue> getRowColumns(DataStreamHashAidxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<DataStreamHashAidxColumnValue> getRowColumns(DataStreamHashAidxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<DataStreamHashAidxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = DataStreamHashAidxColumnValue.hydrateValue(e.getValue());
                ret.add(DataStreamHashAidxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> getRowsMultimap(Iterable<DataStreamHashAidxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> getRowsMultimap(Iterable<DataStreamHashAidxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> getRowsMultimapInternal(Iterable<DataStreamHashAidxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<DataStreamHashAidxRow, DataStreamHashAidxColumnValue> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            DataStreamHashAidxRow row = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = DataStreamHashAidxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DataStreamHashAidxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<DataStreamHashAidxRow, BatchingVisitable<DataStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<DataStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataStreamHashAidxRow, BatchingVisitable<DataStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataStreamHashAidxRow row = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<DataStreamHashAidxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = DataStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return DataStreamHashAidxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<DataStreamHashAidxRow, DataStreamHashAidxColumnValue>> getRowsColumnRange(Iterable<DataStreamHashAidxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            DataStreamHashAidxRow row = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = DataStreamHashAidxColumnValue.hydrateValue(e.getValue());
            DataStreamHashAidxColumnValue colValue = DataStreamHashAidxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<DataStreamHashAidxRow, Iterator<DataStreamHashAidxColumnValue>> getRowsColumnRangeIterator(Iterable<DataStreamHashAidxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DataStreamHashAidxRow, Iterator<DataStreamHashAidxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DataStreamHashAidxRow row = DataStreamHashAidxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<DataStreamHashAidxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                DataStreamHashAidxColumn col = DataStreamHashAidxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = DataStreamHashAidxColumnValue.hydrateValue(result.getValue());
                return DataStreamHashAidxColumnValue.of(col, val);
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

    public BatchingVisitableView<DataStreamHashAidxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<DataStreamHashAidxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, DataStreamHashAidxRowResult>() {
            @Override
            public DataStreamHashAidxRowResult apply(RowResult<byte[]> input) {
                return DataStreamHashAidxRowResult.of(input);
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
    static String __CLASS_HASH = "2DA26oucpzo61Q1TGNErzg==";
}
