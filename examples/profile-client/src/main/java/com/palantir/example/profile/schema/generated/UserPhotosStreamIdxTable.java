package com.palantir.example.profile.schema.generated;

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
public final class UserPhotosStreamIdxTable implements
        AtlasDbDynamicMutablePersistentTable<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow,
                                                UserPhotosStreamIdxTable.UserPhotosStreamIdxColumn,
                                                UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue,
                                                UserPhotosStreamIdxTable.UserPhotosStreamIdxRowResult> {
    private final Transaction t;
    private final List<UserPhotosStreamIdxTrigger> triggers;
    private final static String rawTableName = "user_photos_stream_idx";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = ColumnSelection.all();

    static UserPhotosStreamIdxTable of(Transaction t, Namespace namespace) {
        return new UserPhotosStreamIdxTable(t, namespace, ImmutableList.<UserPhotosStreamIdxTrigger>of());
    }

    static UserPhotosStreamIdxTable of(Transaction t, Namespace namespace, UserPhotosStreamIdxTrigger trigger, UserPhotosStreamIdxTrigger... triggers) {
        return new UserPhotosStreamIdxTable(t, namespace, ImmutableList.<UserPhotosStreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static UserPhotosStreamIdxTable of(Transaction t, Namespace namespace, List<UserPhotosStreamIdxTrigger> triggers) {
        return new UserPhotosStreamIdxTable(t, namespace, triggers);
    }

    private UserPhotosStreamIdxTable(Transaction t, Namespace namespace, List<UserPhotosStreamIdxTrigger> triggers) {
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
     * UserPhotosStreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class UserPhotosStreamIdxRow implements Persistable, Comparable<UserPhotosStreamIdxRow> {
        private final long id;

        public static UserPhotosStreamIdxRow of(long id) {
            return new UserPhotosStreamIdxRow(id);
        }

        private UserPhotosStreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<UserPhotosStreamIdxRow, Long> getIdFun() {
            return new Function<UserPhotosStreamIdxRow, Long>() {
                @Override
                public Long apply(UserPhotosStreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, UserPhotosStreamIdxRow> fromIdFun() {
            return new Function<Long, UserPhotosStreamIdxRow>() {
                @Override
                public UserPhotosStreamIdxRow apply(Long row) {
                    return UserPhotosStreamIdxRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<UserPhotosStreamIdxRow> BYTES_HYDRATOR = new Hydrator<UserPhotosStreamIdxRow>() {
            @Override
            public UserPhotosStreamIdxRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                _index += EncodingUtils.sizeOfUnsignedVarLong(id);
                return new UserPhotosStreamIdxRow(id);
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
            UserPhotosStreamIdxRow other = (UserPhotosStreamIdxRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(UserPhotosStreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * UserPhotosStreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class UserPhotosStreamIdxColumn implements Persistable, Comparable<UserPhotosStreamIdxColumn> {
        private final byte[] reference;

        public static UserPhotosStreamIdxColumn of(byte[] reference) {
            return new UserPhotosStreamIdxColumn(reference);
        }

        private UserPhotosStreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<UserPhotosStreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<UserPhotosStreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(UserPhotosStreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], UserPhotosStreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], UserPhotosStreamIdxColumn>() {
                @Override
                public UserPhotosStreamIdxColumn apply(byte[] row) {
                    return UserPhotosStreamIdxColumn.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<UserPhotosStreamIdxColumn> BYTES_HYDRATOR = new Hydrator<UserPhotosStreamIdxColumn>() {
            @Override
            public UserPhotosStreamIdxColumn hydrateFromBytes(byte[] _input) {
                int _index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(_input, _index);
                _index += EncodingUtils.sizeOfSizedBytes(reference);
                return new UserPhotosStreamIdxColumn(reference);
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
            UserPhotosStreamIdxColumn other = (UserPhotosStreamIdxColumn) obj;
            return Arrays.equals(reference, other.reference);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(UserPhotosStreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface UserPhotosStreamIdxTrigger {
        public void putUserPhotosStreamIdx(Multimap<UserPhotosStreamIdxRow, ? extends UserPhotosStreamIdxColumnValue> newRows);
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
    public static final class UserPhotosStreamIdxColumnValue implements ColumnValue<Long> {
        private final UserPhotosStreamIdxColumn columnName;
        private final Long value;

        public static UserPhotosStreamIdxColumnValue of(UserPhotosStreamIdxColumn columnName, Long value) {
            return new UserPhotosStreamIdxColumnValue(columnName, value);
        }

        private UserPhotosStreamIdxColumnValue(UserPhotosStreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public UserPhotosStreamIdxColumn getColumnName() {
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

        public static Function<UserPhotosStreamIdxColumnValue, UserPhotosStreamIdxColumn> getColumnNameFun() {
            return new Function<UserPhotosStreamIdxColumnValue, UserPhotosStreamIdxColumn>() {
                @Override
                public UserPhotosStreamIdxColumn apply(UserPhotosStreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<UserPhotosStreamIdxColumnValue, Long> getValueFun() {
            return new Function<UserPhotosStreamIdxColumnValue, Long>() {
                @Override
                public Long apply(UserPhotosStreamIdxColumnValue columnValue) {
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

    public static final class UserPhotosStreamIdxRowResult implements TypedRowResult {
        private final UserPhotosStreamIdxRow rowName;
        private final ImmutableSet<UserPhotosStreamIdxColumnValue> columnValues;

        public static UserPhotosStreamIdxRowResult of(RowResult<byte[]> rowResult) {
            UserPhotosStreamIdxRow rowName = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<UserPhotosStreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = UserPhotosStreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(UserPhotosStreamIdxColumnValue.of(col, value));
            }
            return new UserPhotosStreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private UserPhotosStreamIdxRowResult(UserPhotosStreamIdxRow rowName, ImmutableSet<UserPhotosStreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public UserPhotosStreamIdxRow getRowName() {
            return rowName;
        }

        public Set<UserPhotosStreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<UserPhotosStreamIdxRowResult, UserPhotosStreamIdxRow> getRowNameFun() {
            return new Function<UserPhotosStreamIdxRowResult, UserPhotosStreamIdxRow>() {
                @Override
                public UserPhotosStreamIdxRow apply(UserPhotosStreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<UserPhotosStreamIdxRowResult, ImmutableSet<UserPhotosStreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<UserPhotosStreamIdxRowResult, ImmutableSet<UserPhotosStreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<UserPhotosStreamIdxColumnValue> apply(UserPhotosStreamIdxRowResult rowResult) {
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
    public void delete(UserPhotosStreamIdxRow row, UserPhotosStreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<UserPhotosStreamIdxRow> rows) {
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(UserPhotosStreamIdxRow rowName, Iterable<UserPhotosStreamIdxColumnValue> values) {
        put(ImmutableMultimap.<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(UserPhotosStreamIdxRow rowName, UserPhotosStreamIdxColumnValue... values) {
        put(ImmutableMultimap.<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<UserPhotosStreamIdxRow, ? extends UserPhotosStreamIdxColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (UserPhotosStreamIdxTrigger trigger : triggers) {
            trigger.putUserPhotosStreamIdx(values);
        }
    }

    @Override
    public void touch(Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumn> values) {
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<UserPhotosStreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(UserPhotosStreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> get(Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                UserPhotosStreamIdxRow row = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = UserPhotosStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, UserPhotosStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public List<UserPhotosStreamIdxColumnValue> getRowColumns(UserPhotosStreamIdxRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<UserPhotosStreamIdxColumnValue> getRowColumns(UserPhotosStreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<UserPhotosStreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = UserPhotosStreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(UserPhotosStreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> getRowsMultimap(Iterable<UserPhotosStreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> getRowsMultimap(Iterable<UserPhotosStreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> getRowsMultimapInternal(Iterable<UserPhotosStreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            UserPhotosStreamIdxRow row = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = UserPhotosStreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, UserPhotosStreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<UserPhotosStreamIdxRow, BatchingVisitable<UserPhotosStreamIdxColumnValue>> getRowsColumnRange(Iterable<UserPhotosStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<UserPhotosStreamIdxRow, BatchingVisitable<UserPhotosStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            UserPhotosStreamIdxRow row = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<UserPhotosStreamIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = UserPhotosStreamIdxColumnValue.hydrateValue(result.getValue());
                return UserPhotosStreamIdxColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<UserPhotosStreamIdxRow, UserPhotosStreamIdxColumnValue>> getRowsColumnRange(Iterable<UserPhotosStreamIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            UserPhotosStreamIdxRow row = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
            Long val = UserPhotosStreamIdxColumnValue.hydrateValue(e.getValue());
            UserPhotosStreamIdxColumnValue colValue = UserPhotosStreamIdxColumnValue.of(col, val);
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<UserPhotosStreamIdxRow, Iterator<UserPhotosStreamIdxColumnValue>> getRowsColumnRangeIterator(Iterable<UserPhotosStreamIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<UserPhotosStreamIdxRow, Iterator<UserPhotosStreamIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            UserPhotosStreamIdxRow row = UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<UserPhotosStreamIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                UserPhotosStreamIdxColumn col = UserPhotosStreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                Long val = UserPhotosStreamIdxColumnValue.hydrateValue(result.getValue());
                return UserPhotosStreamIdxColumnValue.of(col, val);
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

    public BatchingVisitableView<UserPhotosStreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<UserPhotosStreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, UserPhotosStreamIdxRowResult>() {
            @Override
            public UserPhotosStreamIdxRowResult apply(RowResult<byte[]> input) {
                return UserPhotosStreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "xkzH3ePg9bEd2qkecxe+6Q==";
}
