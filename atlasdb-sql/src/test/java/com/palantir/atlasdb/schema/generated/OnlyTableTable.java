package com.palantir.atlasdb.schema.generated;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Generated;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
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
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;


@Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
public final class OnlyTableTable implements
        AtlasDbMutablePersistentTable<OnlyTableTable.OnlyTableRow,
                                         OnlyTableTable.OnlyTableNamedColumnValue<?>,
                                         OnlyTableTable.OnlyTableRowResult>,
        AtlasDbNamedMutableTable<OnlyTableTable.OnlyTableRow,
                                    OnlyTableTable.OnlyTableNamedColumnValue<?>,
                                    OnlyTableTable.OnlyTableRowResult> {
    private final Transaction t;
    private final List<OnlyTableTrigger> triggers;
    private final static String rawTableName = "only_table";
    private final TableReference tableRef;

    static OnlyTableTable of(Transaction t, Namespace namespace) {
        return new OnlyTableTable(t, namespace, ImmutableList.<OnlyTableTrigger>of());
    }

    static OnlyTableTable of(Transaction t, Namespace namespace, OnlyTableTrigger trigger, OnlyTableTrigger... triggers) {
        return new OnlyTableTable(t, namespace, ImmutableList.<OnlyTableTrigger>builder().add(trigger).add(triggers).build());
    }

    static OnlyTableTable of(Transaction t, Namespace namespace, List<OnlyTableTrigger> triggers) {
        return new OnlyTableTable(t, namespace, triggers);
    }

    private OnlyTableTable(Transaction t, Namespace namespace, List<OnlyTableTrigger> triggers) {
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
     * OnlyTableRow {
     *   {@literal String objectId};
     * }
     * </pre>
     */
    public static final class OnlyTableRow implements Persistable, Comparable<OnlyTableRow> {
        private final String objectId;

        public static OnlyTableRow of(String objectId) {
            return new OnlyTableRow(objectId);
        }

        private OnlyTableRow(String objectId) {
            this.objectId = objectId;
        }

        public String getObjectId() {
            return objectId;
        }

        public static Function<OnlyTableRow, String> getObjectIdFun() {
            return new Function<OnlyTableRow, String>() {
                @Override
                public String apply(OnlyTableRow row) {
                    return row.objectId;
                }
            };
        }

        public static Function<String, OnlyTableRow> fromObjectIdFun() {
            return new Function<String, OnlyTableRow>() {
                @Override
                public OnlyTableRow apply(String row) {
                    return OnlyTableRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] objectIdBytes = PtBytes.toBytes(objectId);
            return EncodingUtils.add(objectIdBytes);
        }

        public static final Hydrator<OnlyTableRow> BYTES_HYDRATOR = new Hydrator<OnlyTableRow>() {
            @Override
            public OnlyTableRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String objectId = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new OnlyTableRow(objectId);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("objectId", objectId)
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
            OnlyTableRow other = (OnlyTableRow) obj;
            return Objects.equal(objectId, other.objectId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(objectId);
        }

        @Override
        public int compareTo(OnlyTableRow o) {
            return ComparisonChain.start()
                .compare(this.objectId, o.objectId)
                .result();
        }
    }

    public interface OnlyTableNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.TestPersistence.TestObject;
     *   name: "TestObject"
     *   field {
     *     name: "id"
     *     number: 1
     *     label: LABEL_REQUIRED
     *     type: TYPE_FIXED64
     *   }
     *   field {
     *     name: "type"
     *     number: 2
     *     label: LABEL_REQUIRED
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "is_group"
     *     number: 3
     *     label: LABEL_REQUIRED
     *     type: TYPE_BOOL
     *   }
     *   field {
     *     name: "deleted"
     *     number: 4
     *     label: LABEL_REQUIRED
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "data_event_id"
     *     number: 5
     *     label: LABEL_OPTIONAL
     *     type: TYPE_INT64
     *   }
     * }
     * </pre>
     */
    public static final class BaseObject implements OnlyTableNamedColumnValue<com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> {
        private final com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value;

        public static BaseObject of(com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
            return new BaseObject(value);
        }

        private BaseObject(com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "base_object";
        }

        @Override
        public String getShortColumnName() {
            return "b";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.TestPersistence.TestObject getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("b");
        }

        public static final Hydrator<BaseObject> BYTES_HYDRATOR = new Hydrator<BaseObject>() {
            @Override
            public BaseObject hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.TestPersistence.TestObject.parseFrom(bytes));
                } catch (InvalidProtocolBufferException e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface OnlyTableTrigger {
        public void putOnlyTable(Multimap<OnlyTableRow, ? extends OnlyTableNamedColumnValue<?>> newRows);
    }

    public static final class OnlyTableRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static OnlyTableRowResult of(RowResult<byte[]> row) {
            return new OnlyTableRowResult(row);
        }

        private OnlyTableRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public OnlyTableRow getRowName() {
            return OnlyTableRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<OnlyTableRowResult, OnlyTableRow> getRowNameFun() {
            return new Function<OnlyTableRowResult, OnlyTableRow>() {
                @Override
                public OnlyTableRow apply(OnlyTableRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, OnlyTableRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, OnlyTableRowResult>() {
                @Override
                public OnlyTableRowResult apply(RowResult<byte[]> rowResult) {
                    return new OnlyTableRowResult(rowResult);
                }
            };
        }

        public boolean hasBaseObject() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("b"));
        }

        public com.palantir.atlasdb.protos.generated.TestPersistence.TestObject getBaseObject() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("b"));
            if (bytes == null) {
                return null;
            }
            BaseObject value = BaseObject.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<OnlyTableRowResult, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> getBaseObjectFun() {
            return new Function<OnlyTableRowResult, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject>() {
                @Override
                public com.palantir.atlasdb.protos.generated.TestPersistence.TestObject apply(OnlyTableRowResult rowResult) {
                    return rowResult.getBaseObject();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("BaseObject", getBaseObject())
                .toString();
        }
    }

    public enum OnlyTableNamedColumn {
        BASE_OBJECT {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("b");
            }
        };

        public abstract byte[] getShortName();

        public static Function<OnlyTableNamedColumn, byte[]> toShortName() {
            return new Function<OnlyTableNamedColumn, byte[]>() {
                @Override
                public byte[] apply(OnlyTableNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<OnlyTableNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, OnlyTableNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(OnlyTableNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends OnlyTableNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends OnlyTableNamedColumnValue<?>>>builder()
                .put("b", BaseObject.BYTES_HYDRATOR)
                .build();

    public Map<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> getBaseObjects(Collection<OnlyTableRow> rows) {
        Map<Cell, OnlyTableRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (OnlyTableRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("b")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.TestPersistence.TestObject val = BaseObject.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putBaseObject(OnlyTableRow row, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
        put(ImmutableMultimap.of(row, BaseObject.of(value)));
    }

    public void putBaseObject(Map<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> map) {
        Map<OnlyTableRow, OnlyTableNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> e : map.entrySet()) {
            toPut.put(e.getKey(), BaseObject.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putBaseObjectUnlessExists(OnlyTableRow row, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
        putUnlessExists(ImmutableMultimap.of(row, BaseObject.of(value)));
    }

    public void putBaseObjectUnlessExists(Map<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> map) {
        Map<OnlyTableRow, OnlyTableNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<OnlyTableRow, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> e : map.entrySet()) {
            toPut.put(e.getKey(), BaseObject.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<OnlyTableRow, ? extends OnlyTableNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (OnlyTableTrigger trigger : triggers) {
            trigger.putOnlyTable(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<OnlyTableRow, ? extends OnlyTableNamedColumnValue<?>> rows) {
        Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<OnlyTableRow, ? extends OnlyTableNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteBaseObject(OnlyTableRow row) {
        deleteBaseObject(ImmutableSet.of(row));
    }

    public void deleteBaseObject(Iterable<OnlyTableRow> rows) {
        byte[] col = PtBytes.toCachedBytes("b");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(OnlyTableRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<OnlyTableRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("b")));
        t.delete(tableRef, cells);
    }

    @Override
    public Optional<OnlyTableRowResult> getRow(OnlyTableRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<OnlyTableRowResult> getRow(OnlyTableRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(OnlyTableRowResult.of(rowResult));
        }
    }

    @Override
    public List<OnlyTableRowResult> getRows(Iterable<OnlyTableRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<OnlyTableRowResult> getRows(Iterable<OnlyTableRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<OnlyTableRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(OnlyTableRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<OnlyTableRowResult> getAsyncRows(Iterable<OnlyTableRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<OnlyTableRowResult> getAsyncRows(final Iterable<OnlyTableRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<OnlyTableRowResult>> c =
                new Callable<List<OnlyTableRowResult>>() {
            @Override
            public List<OnlyTableRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<OnlyTableNamedColumnValue<?>> getRowColumns(OnlyTableRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<OnlyTableNamedColumnValue<?>> getRowColumns(OnlyTableRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<OnlyTableNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getRowsMultimap(Iterable<OnlyTableRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getRowsMultimap(Iterable<OnlyTableRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<OnlyTableRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<OnlyTableRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>>> c =
                new Callable<Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>>>() {
            @Override
            public Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getRowsMultimapInternal(Iterable<OnlyTableRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<OnlyTableRow, OnlyTableNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            OnlyTableRow row = OnlyTableRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<OnlyTableRow, BatchingVisitable<OnlyTableNamedColumnValue<?>>> getRowsColumnRange(Iterable<OnlyTableRow> rows, ColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<OnlyTableRow, BatchingVisitable<OnlyTableNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            OnlyTableRow row = OnlyTableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<OnlyTableNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    public BatchingVisitableView<OnlyTableRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<OnlyTableRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, OnlyTableRowResult>() {
            @Override
            public OnlyTableRowResult apply(RowResult<byte[]> input) {
                return OnlyTableRowResult.of(input);
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
     * {@link AsyncProxy}
     * {@link AtlasDbConstraintCheckingMode}
     * {@link AtlasDbDynamicMutableExpiringTable}
     * {@link AtlasDbDynamicMutablePersistentTable}
     * {@link AtlasDbMutableExpiringTable}
     * {@link AtlasDbMutablePersistentTable}
     * {@link AtlasDbNamedExpiringSet}
     * {@link AtlasDbNamedMutableTable}
     * {@link AtlasDbNamedPersistentSet}
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
     * {@link ExecutorService}
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
    static String __CLASS_HASH = "fTMBX1HIWnYUtedsda5N3A==";
}
