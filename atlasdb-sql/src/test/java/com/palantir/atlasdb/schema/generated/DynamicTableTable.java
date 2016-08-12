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
public final class DynamicTableTable implements
        AtlasDbDynamicMutablePersistentTable<DynamicTableTable.DynamicTableRow,
                                                DynamicTableTable.DynamicTableColumn,
                                                DynamicTableTable.DynamicTableColumnValue,
                                                DynamicTableTable.DynamicTableRowResult> {
    private final Transaction t;
    private final List<DynamicTableTrigger> triggers;
    private final static String rawTableName = "dynamic_table";
    private final TableReference tableRef;

    static DynamicTableTable of(Transaction t, Namespace namespace) {
        return new DynamicTableTable(t, namespace, ImmutableList.<DynamicTableTrigger>of());
    }

    static DynamicTableTable of(Transaction t, Namespace namespace, DynamicTableTrigger trigger, DynamicTableTrigger... triggers) {
        return new DynamicTableTable(t, namespace, ImmutableList.<DynamicTableTrigger>builder().add(trigger).add(triggers).build());
    }

    static DynamicTableTable of(Transaction t, Namespace namespace, List<DynamicTableTrigger> triggers) {
        return new DynamicTableTable(t, namespace, triggers);
    }

    private DynamicTableTable(Transaction t, Namespace namespace, List<DynamicTableTrigger> triggers) {
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
     * DynamicTableRow {
     *   {@literal Long resyncContextId};
     *   {@literal String extOrResolvesToKey};
     * }
     * </pre>
     */
    public static final class DynamicTableRow implements Persistable, Comparable<DynamicTableRow> {
        private final long resyncContextId;
        private final String extOrResolvesToKey;

        public static DynamicTableRow of(long resyncContextId, String extOrResolvesToKey) {
            return new DynamicTableRow(resyncContextId, extOrResolvesToKey);
        }

        private DynamicTableRow(long resyncContextId, String extOrResolvesToKey) {
            this.resyncContextId = resyncContextId;
            this.extOrResolvesToKey = extOrResolvesToKey;
        }

        public long getResyncContextId() {
            return resyncContextId;
        }

        public String getExtOrResolvesToKey() {
            return extOrResolvesToKey;
        }

        public static Function<DynamicTableRow, Long> getResyncContextIdFun() {
            return new Function<DynamicTableRow, Long>() {
                @Override
                public Long apply(DynamicTableRow row) {
                    return row.resyncContextId;
                }
            };
        }

        public static Function<DynamicTableRow, String> getExtOrResolvesToKeyFun() {
            return new Function<DynamicTableRow, String>() {
                @Override
                public String apply(DynamicTableRow row) {
                    return row.extOrResolvesToKey;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] resyncContextIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ resyncContextId);
            byte[] extOrResolvesToKeyBytes = PtBytes.toBytes(extOrResolvesToKey);
            return EncodingUtils.add(resyncContextIdBytes, extOrResolvesToKeyBytes);
        }

        public static final Hydrator<DynamicTableRow> BYTES_HYDRATOR = new Hydrator<DynamicTableRow>() {
            @Override
            public DynamicTableRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long resyncContextId = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String extOrResolvesToKey = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new DynamicTableRow(resyncContextId, extOrResolvesToKey);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("resyncContextId", resyncContextId)
                .add("extOrResolvesToKey", extOrResolvesToKey)
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
            DynamicTableRow other = (DynamicTableRow) obj;
            return Objects.equal(resyncContextId, other.resyncContextId) && Objects.equal(extOrResolvesToKey, other.extOrResolvesToKey);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ resyncContextId, extOrResolvesToKey });
        }

        @Override
        public int compareTo(DynamicTableRow o) {
            return ComparisonChain.start()
                .compare(this.resyncContextId, o.resyncContextId)
                .compare(this.extOrResolvesToKey, o.extOrResolvesToKey)
                .result();
        }
    }

    /**
     * <pre>
     * DynamicTableColumn {
     *   {@literal Long dataChunkId};
     *   {@literal String externalKey};
     * }
     * </pre>
     */
    public static final class DynamicTableColumn implements Persistable, Comparable<DynamicTableColumn> {
        private final long dataChunkId;
        private final String externalKey;

        public static DynamicTableColumn of(long dataChunkId, String externalKey) {
            return new DynamicTableColumn(dataChunkId, externalKey);
        }

        private DynamicTableColumn(long dataChunkId, String externalKey) {
            this.dataChunkId = dataChunkId;
            this.externalKey = externalKey;
        }

        public long getDataChunkId() {
            return dataChunkId;
        }

        public String getExternalKey() {
            return externalKey;
        }

        public static Function<DynamicTableColumn, Long> getDataChunkIdFun() {
            return new Function<DynamicTableColumn, Long>() {
                @Override
                public Long apply(DynamicTableColumn row) {
                    return row.dataChunkId;
                }
            };
        }

        public static Function<DynamicTableColumn, String> getExternalKeyFun() {
            return new Function<DynamicTableColumn, String>() {
                @Override
                public String apply(DynamicTableColumn row) {
                    return row.externalKey;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] dataChunkIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ dataChunkId);
            byte[] externalKeyBytes = PtBytes.toBytes(externalKey);
            return EncodingUtils.add(dataChunkIdBytes, externalKeyBytes);
        }

        public static final Hydrator<DynamicTableColumn> BYTES_HYDRATOR = new Hydrator<DynamicTableColumn>() {
            @Override
            public DynamicTableColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long dataChunkId = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                String externalKey = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new DynamicTableColumn(dataChunkId, externalKey);
            }
        };

        public static ColumnRangeSelection createPrefixRange(long dataChunkId, int batchSize) {
            byte[] dataChunkIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ dataChunkId);
            return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(dataChunkIdBytes), batchSize);
        }

        public static Prefix prefix(long dataChunkId) {
            byte[] dataChunkIdBytes = PtBytes.toBytes(Long.MIN_VALUE ^ dataChunkId);
            return new Prefix(EncodingUtils.add(dataChunkIdBytes));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("dataChunkId", dataChunkId)
                .add("externalKey", externalKey)
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
            DynamicTableColumn other = (DynamicTableColumn) obj;
            return Objects.equal(dataChunkId, other.dataChunkId) && Objects.equal(externalKey, other.externalKey);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ dataChunkId, externalKey });
        }

        @Override
        public int compareTo(DynamicTableColumn o) {
            return ComparisonChain.start()
                .compare(this.dataChunkId, o.dataChunkId)
                .compare(this.externalKey, o.externalKey)
                .result();
        }
    }

    public interface DynamicTableTrigger {
        public void putDynamicTable(Multimap<DynamicTableRow, ? extends DynamicTableColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal Long dataChunkId};
     *   {@literal String externalKey};
     * }
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
    public static final class DynamicTableColumnValue implements ColumnValue<com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> {
        private final DynamicTableColumn columnName;
        private final com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value;

        public static DynamicTableColumnValue of(DynamicTableColumn columnName, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
            return new DynamicTableColumnValue(columnName, value);
        }

        private DynamicTableColumnValue(DynamicTableColumn columnName, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value) {
            this.columnName = columnName;
            this.value = value;
        }

        public DynamicTableColumn getColumnName() {
            return columnName;
        }

        @Override
        public com.palantir.atlasdb.protos.generated.TestPersistence.TestObject getValue() {
            return value;
        }

        @Override
        public byte[] persistColumnName() {
            return columnName.persistToBytes();
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        public static com.palantir.atlasdb.protos.generated.TestPersistence.TestObject hydrateValue(byte[] bytes) {
            bytes = CompressionUtils.decompress(bytes, Compression.NONE);
            try {
                return com.palantir.atlasdb.protos.generated.TestPersistence.TestObject.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        public static Function<DynamicTableColumnValue, DynamicTableColumn> getColumnNameFun() {
            return new Function<DynamicTableColumnValue, DynamicTableColumn>() {
                @Override
                public DynamicTableColumn apply(DynamicTableColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<DynamicTableColumnValue, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject> getValueFun() {
            return new Function<DynamicTableColumnValue, com.palantir.atlasdb.protos.generated.TestPersistence.TestObject>() {
                @Override
                public com.palantir.atlasdb.protos.generated.TestPersistence.TestObject apply(DynamicTableColumnValue columnValue) {
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

    public static final class DynamicTableRowResult implements TypedRowResult {
        private final DynamicTableRow rowName;
        private final ImmutableSet<DynamicTableColumnValue> columnValues;

        public static DynamicTableRowResult of(RowResult<byte[]> rowResult) {
            DynamicTableRow rowName = DynamicTableRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<DynamicTableColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DynamicTableColumn col = DynamicTableColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.protos.generated.TestPersistence.TestObject value = DynamicTableColumnValue.hydrateValue(e.getValue());
                columnValues.add(DynamicTableColumnValue.of(col, value));
            }
            return new DynamicTableRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private DynamicTableRowResult(DynamicTableRow rowName, ImmutableSet<DynamicTableColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public DynamicTableRow getRowName() {
            return rowName;
        }

        public Set<DynamicTableColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<DynamicTableRowResult, DynamicTableRow> getRowNameFun() {
            return new Function<DynamicTableRowResult, DynamicTableRow>() {
                @Override
                public DynamicTableRow apply(DynamicTableRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<DynamicTableRowResult, ImmutableSet<DynamicTableColumnValue>> getColumnValuesFun() {
            return new Function<DynamicTableRowResult, ImmutableSet<DynamicTableColumnValue>>() {
                @Override
                public ImmutableSet<DynamicTableColumnValue> apply(DynamicTableRowResult rowResult) {
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
    public void delete(DynamicTableRow row, DynamicTableColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<DynamicTableRow> rows) {
        Multimap<DynamicTableRow, DynamicTableColumn> toRemove = HashMultimap.create();
        Multimap<DynamicTableRow, DynamicTableColumnValue> result = getRowsMultimap(rows);
        for (Entry<DynamicTableRow, DynamicTableColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<DynamicTableRow, DynamicTableColumn> values) {
        t.delete(tableRef, ColumnValues.toCells(values));
    }

    @Override
    public void put(DynamicTableRow rowName, Iterable<DynamicTableColumnValue> values) {
        put(ImmutableMultimap.<DynamicTableRow, DynamicTableColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(DynamicTableRow rowName, DynamicTableColumnValue... values) {
        put(ImmutableMultimap.<DynamicTableRow, DynamicTableColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void put(Multimap<DynamicTableRow, ? extends DynamicTableColumnValue> values) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(values));
        for (DynamicTableTrigger trigger : triggers) {
            trigger.putDynamicTable(values);
        }
    }

    @Override
    public void putUnlessExists(DynamicTableRow rowName, Iterable<DynamicTableColumnValue> values) {
        putUnlessExists(ImmutableMultimap.<DynamicTableRow, DynamicTableColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(DynamicTableRow rowName, DynamicTableColumnValue... values) {
        putUnlessExists(ImmutableMultimap.<DynamicTableRow, DynamicTableColumnValue>builder().putAll(rowName, values).build());
    }

    @Override
    public void putUnlessExists(Multimap<DynamicTableRow, ? extends DynamicTableColumnValue> rows) {
        Multimap<DynamicTableRow, DynamicTableColumn> toGet = Multimaps.transformValues(rows, DynamicTableColumnValue.getColumnNameFun());
        Multimap<DynamicTableRow, DynamicTableColumnValue> existing = get(toGet);
        Multimap<DynamicTableRow, DynamicTableColumnValue> toPut = HashMultimap.create();
        for (Entry<DynamicTableRow, ? extends DynamicTableColumnValue> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    @Override
    public void touch(Multimap<DynamicTableRow, DynamicTableColumn> values) {
        Multimap<DynamicTableRow, DynamicTableColumnValue> currentValues = get(values);
        put(currentValues);
        Multimap<DynamicTableRow, DynamicTableColumn> toDelete = HashMultimap.create(values);
        for (Map.Entry<DynamicTableRow, DynamicTableColumnValue> e : currentValues.entries()) {
            toDelete.remove(e.getKey(), e.getValue().getColumnName());
        }
        delete(toDelete);
    }

    public static ColumnSelection getColumnSelection(Collection<DynamicTableColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(DynamicTableColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> get(Multimap<DynamicTableRow, DynamicTableColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
        Multimap<DynamicTableRow, DynamicTableColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                DynamicTableRow row = DynamicTableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                DynamicTableColumn col = DynamicTableColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                com.palantir.atlasdb.protos.generated.TestPersistence.TestObject val = DynamicTableColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DynamicTableColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> getAsync(final Multimap<DynamicTableRow, DynamicTableColumn> cells, ExecutorService exec) {
        Callable<Multimap<DynamicTableRow, DynamicTableColumnValue>> c =
                new Callable<Multimap<DynamicTableRow, DynamicTableColumnValue>>() {
            @Override
            public Multimap<DynamicTableRow, DynamicTableColumnValue> call() {
                return get(cells);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    @Override
    public List<DynamicTableColumnValue> getRowColumns(DynamicTableRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<DynamicTableColumnValue> getRowColumns(DynamicTableRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<DynamicTableColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                DynamicTableColumn col = DynamicTableColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.protos.generated.TestPersistence.TestObject val = DynamicTableColumnValue.hydrateValue(e.getValue());
                ret.add(DynamicTableColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> getRowsMultimap(Iterable<DynamicTableRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> getRowsMultimap(Iterable<DynamicTableRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> getAsyncRowsMultimap(Iterable<DynamicTableRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<DynamicTableRow, DynamicTableColumnValue> getAsyncRowsMultimap(final Iterable<DynamicTableRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<DynamicTableRow, DynamicTableColumnValue>> c =
                new Callable<Multimap<DynamicTableRow, DynamicTableColumnValue>>() {
            @Override
            public Multimap<DynamicTableRow, DynamicTableColumnValue> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<DynamicTableRow, DynamicTableColumnValue> getRowsMultimapInternal(Iterable<DynamicTableRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<DynamicTableRow, DynamicTableColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<DynamicTableRow, DynamicTableColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            DynamicTableRow row = DynamicTableRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                DynamicTableColumn col = DynamicTableColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                com.palantir.atlasdb.protos.generated.TestPersistence.TestObject val = DynamicTableColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, DynamicTableColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Map<DynamicTableRow, BatchingVisitable<DynamicTableColumnValue>> getRowsColumnRange(Iterable<DynamicTableRow> rows, ColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<DynamicTableRow, BatchingVisitable<DynamicTableColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            DynamicTableRow row = DynamicTableRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<DynamicTableColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                DynamicTableColumn col = DynamicTableColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                com.palantir.atlasdb.protos.generated.TestPersistence.TestObject val = DynamicTableColumnValue.hydrateValue(result.getValue());
                return DynamicTableColumnValue.of(col, val);
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    public BatchingVisitableView<DynamicTableRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<DynamicTableRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, DynamicTableRowResult>() {
            @Override
            public DynamicTableRowResult apply(RowResult<byte[]> input) {
                return DynamicTableRowResult.of(input);
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
    static String __CLASS_HASH = "3+5J0ZAj4vnQbkRgvqPNMw==";
}
