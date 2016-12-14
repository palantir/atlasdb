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
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;

@Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
public final class TransactionSweepTable implements
        AtlasDbMutablePersistentTable<TransactionSweepTable.TransactionSweepRow,
                                         TransactionSweepTable.TransactionSweepNamedColumnValue<?>,
                                         TransactionSweepTable.TransactionSweepRowResult>,
        AtlasDbNamedMutableTable<TransactionSweepTable.TransactionSweepRow,
                                    TransactionSweepTable.TransactionSweepNamedColumnValue<?>,
                                    TransactionSweepTable.TransactionSweepRowResult> {
    private final Transaction t;
    private final List<TransactionSweepTrigger> triggers;
    private final static String rawTableName = "transactionSweep";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(TransactionSweepNamedColumn.values());

    static TransactionSweepTable of(Transaction t, Namespace namespace) {
        return new TransactionSweepTable(t, namespace, ImmutableList.<TransactionSweepTrigger>of());
    }

    static TransactionSweepTable of(Transaction t, Namespace namespace, TransactionSweepTrigger trigger, TransactionSweepTrigger... triggers) {
        return new TransactionSweepTable(t, namespace, ImmutableList.<TransactionSweepTrigger>builder().add(trigger).add(triggers).build());
    }

    static TransactionSweepTable of(Transaction t, Namespace namespace, List<TransactionSweepTrigger> triggers) {
        return new TransactionSweepTable(t, namespace, triggers);
    }

    private TransactionSweepTable(Transaction t, Namespace namespace, List<TransactionSweepTrigger> triggers) {
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
     * TransactionSweepRow {
     *   {@literal Long dummy};
     * }
     * </pre>
     */
    public static final class TransactionSweepRow implements Persistable, Comparable<TransactionSweepRow> {
        private final long dummy;

        public static TransactionSweepRow of(long dummy) {
            return new TransactionSweepRow(dummy);
        }

        private TransactionSweepRow(long dummy) {
            this.dummy = dummy;
        }

        public long getDummy() {
            return dummy;
        }

        public static Function<TransactionSweepRow, Long> getDummyFun() {
            return new Function<TransactionSweepRow, Long>() {
                @Override
                public Long apply(TransactionSweepRow row) {
                    return row.dummy;
                }
            };
        }

        public static Function<Long, TransactionSweepRow> fromDummyFun() {
            return new Function<Long, TransactionSweepRow>() {
                @Override
                public TransactionSweepRow apply(Long row) {
                    return TransactionSweepRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] dummyBytes = EncodingUtils.encodeUnsignedVarLong(dummy);
            return EncodingUtils.add(dummyBytes);
        }

        public static final Hydrator<TransactionSweepRow> BYTES_HYDRATOR = new Hydrator<TransactionSweepRow>() {
            @Override
            public TransactionSweepRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long dummy = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(dummy);
                return new TransactionSweepRow(dummy);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("dummy", dummy)
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
            TransactionSweepRow other = (TransactionSweepRow) obj;
            return Objects.equal(dummy, other.dummy);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dummy);
        }

        @Override
        public int compareTo(TransactionSweepRow o) {
            return ComparisonChain.start()
                .compare(this.dummy, o.dummy)
                .result();
        }
    }

    public interface TransactionSweepNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class AllTransactionsGoodBefore implements TransactionSweepNamedColumnValue<Long> {
        private final Long value;

        public static AllTransactionsGoodBefore of(Long value) {
            return new AllTransactionsGoodBefore(value);
        }

        private AllTransactionsGoodBefore(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "all_transactions_good_before";
        }

        @Override
        public String getShortColumnName() {
            return "a";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeSignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("a");
        }

        public static final Hydrator<AllTransactionsGoodBefore> BYTES_HYDRATOR = new Hydrator<AllTransactionsGoodBefore>() {
            @Override
            public AllTransactionsGoodBefore hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeSignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class CellsDeleted implements TransactionSweepNamedColumnValue<Long> {
        private final Long value;

        public static CellsDeleted of(Long value) {
            return new CellsDeleted(value);
        }

        private CellsDeleted(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "cells_deleted";
        }

        @Override
        public String getShortColumnName() {
            return "d";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("d");
        }

        public static final Hydrator<CellsDeleted> BYTES_HYDRATOR = new Hydrator<CellsDeleted>() {
            @Override
            public CellsDeleted hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeUnsignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class CurrentlySweptUpTo implements TransactionSweepNamedColumnValue<Long> {
        private final Long value;

        public static CurrentlySweptUpTo of(Long value) {
            return new CurrentlySweptUpTo(value);
        }

        private CurrentlySweptUpTo(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "currentlySweptUpTo";
        }

        @Override
        public String getShortColumnName() {
            return "c";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeSignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<CurrentlySweptUpTo> BYTES_HYDRATOR = new Hydrator<CurrentlySweptUpTo>() {
            @Override
            public CurrentlySweptUpTo hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeSignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class LastSweepTime implements TransactionSweepNamedColumnValue<Long> {
        private final Long value;

        public static LastSweepTime of(Long value) {
            return new LastSweepTime(value);
        }

        private LastSweepTime(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "last_sweep_time";
        }

        @Override
        public String getShortColumnName() {
            return "s";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUnsignedVarLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("s");
        }

        public static final Hydrator<LastSweepTime> BYTES_HYDRATOR = new Hydrator<LastSweepTime>() {
            @Override
            public LastSweepTime hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeUnsignedVarLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface TransactionSweepTrigger {
        public void putTransactionSweep(Multimap<TransactionSweepRow, ? extends TransactionSweepNamedColumnValue<?>> newRows);
    }

    public static final class TransactionSweepRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static TransactionSweepRowResult of(RowResult<byte[]> row) {
            return new TransactionSweepRowResult(row);
        }

        private TransactionSweepRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public TransactionSweepRow getRowName() {
            return TransactionSweepRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<TransactionSweepRowResult, TransactionSweepRow> getRowNameFun() {
            return new Function<TransactionSweepRowResult, TransactionSweepRow>() {
                @Override
                public TransactionSweepRow apply(TransactionSweepRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, TransactionSweepRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, TransactionSweepRowResult>() {
                @Override
                public TransactionSweepRowResult apply(RowResult<byte[]> rowResult) {
                    return new TransactionSweepRowResult(rowResult);
                }
            };
        }

        public boolean hasAllTransactionsGoodBefore() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("a"));
        }

        public boolean hasCellsDeleted() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public boolean hasCurrentlySweptUpTo() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public boolean hasLastSweepTime() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("s"));
        }

        public Long getAllTransactionsGoodBefore() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("a"));
            if (bytes == null) {
                return null;
            }
            AllTransactionsGoodBefore value = AllTransactionsGoodBefore.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getCellsDeleted() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            CellsDeleted value = CellsDeleted.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getCurrentlySweptUpTo() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            CurrentlySweptUpTo value = CurrentlySweptUpTo.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getLastSweepTime() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("s"));
            if (bytes == null) {
                return null;
            }
            LastSweepTime value = LastSweepTime.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<TransactionSweepRowResult, Long> getAllTransactionsGoodBeforeFun() {
            return new Function<TransactionSweepRowResult, Long>() {
                @Override
                public Long apply(TransactionSweepRowResult rowResult) {
                    return rowResult.getAllTransactionsGoodBefore();
                }
            };
        }

        public static Function<TransactionSweepRowResult, Long> getCellsDeletedFun() {
            return new Function<TransactionSweepRowResult, Long>() {
                @Override
                public Long apply(TransactionSweepRowResult rowResult) {
                    return rowResult.getCellsDeleted();
                }
            };
        }

        public static Function<TransactionSweepRowResult, Long> getCurrentlySweptUpToFun() {
            return new Function<TransactionSweepRowResult, Long>() {
                @Override
                public Long apply(TransactionSweepRowResult rowResult) {
                    return rowResult.getCurrentlySweptUpTo();
                }
            };
        }

        public static Function<TransactionSweepRowResult, Long> getLastSweepTimeFun() {
            return new Function<TransactionSweepRowResult, Long>() {
                @Override
                public Long apply(TransactionSweepRowResult rowResult) {
                    return rowResult.getLastSweepTime();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("AllTransactionsGoodBefore", getAllTransactionsGoodBefore())
                .add("CellsDeleted", getCellsDeleted())
                .add("CurrentlySweptUpTo", getCurrentlySweptUpTo())
                .add("LastSweepTime", getLastSweepTime())
                .toString();
        }
    }

    public enum TransactionSweepNamedColumn {
        ALL_TRANSACTIONS_GOOD_BEFORE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("a");
            }
        },
        CELLS_DELETED {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        },
        CURRENTLY_SWEPT_UP_TO {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        },
        LAST_SWEEP_TIME {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("s");
            }
        };

        public abstract byte[] getShortName();

        public static Function<TransactionSweepNamedColumn, byte[]> toShortName() {
            return new Function<TransactionSweepNamedColumn, byte[]>() {
                @Override
                public byte[] apply(TransactionSweepNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<TransactionSweepNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, TransactionSweepNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(TransactionSweepNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends TransactionSweepNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends TransactionSweepNamedColumnValue<?>>>builder()
                .put("a", AllTransactionsGoodBefore.BYTES_HYDRATOR)
                .put("s", LastSweepTime.BYTES_HYDRATOR)
                .put("d", CellsDeleted.BYTES_HYDRATOR)
                .put("c", CurrentlySweptUpTo.BYTES_HYDRATOR)
                .build();

    public Map<TransactionSweepRow, Long> getAllTransactionsGoodBefores(Collection<TransactionSweepRow> rows) {
        Map<Cell, TransactionSweepRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TransactionSweepRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("a")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TransactionSweepRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = AllTransactionsGoodBefore.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<TransactionSweepRow, Long> getLastSweepTimes(Collection<TransactionSweepRow> rows) {
        Map<Cell, TransactionSweepRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TransactionSweepRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("s")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TransactionSweepRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = LastSweepTime.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<TransactionSweepRow, Long> getCellsDeleteds(Collection<TransactionSweepRow> rows) {
        Map<Cell, TransactionSweepRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TransactionSweepRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TransactionSweepRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CellsDeleted.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<TransactionSweepRow, Long> getCurrentlySweptUpTos(Collection<TransactionSweepRow> rows) {
        Map<Cell, TransactionSweepRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (TransactionSweepRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<TransactionSweepRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CurrentlySweptUpTo.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putAllTransactionsGoodBefore(TransactionSweepRow row, Long value) {
        put(ImmutableMultimap.of(row, AllTransactionsGoodBefore.of(value)));
    }

    public void putAllTransactionsGoodBefore(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), AllTransactionsGoodBefore.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putAllTransactionsGoodBeforeUnlessExists(TransactionSweepRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, AllTransactionsGoodBefore.of(value)));
    }

    public void putAllTransactionsGoodBeforeUnlessExists(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), AllTransactionsGoodBefore.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putLastSweepTime(TransactionSweepRow row, Long value) {
        put(ImmutableMultimap.of(row, LastSweepTime.of(value)));
    }

    public void putLastSweepTime(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), LastSweepTime.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putLastSweepTimeUnlessExists(TransactionSweepRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, LastSweepTime.of(value)));
    }

    public void putLastSweepTimeUnlessExists(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), LastSweepTime.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putCellsDeleted(TransactionSweepRow row, Long value) {
        put(ImmutableMultimap.of(row, CellsDeleted.of(value)));
    }

    public void putCellsDeleted(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsDeleted.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCellsDeletedUnlessExists(TransactionSweepRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, CellsDeleted.of(value)));
    }

    public void putCellsDeletedUnlessExists(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsDeleted.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putCurrentlySweptUpTo(TransactionSweepRow row, Long value) {
        put(ImmutableMultimap.of(row, CurrentlySweptUpTo.of(value)));
    }

    public void putCurrentlySweptUpTo(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CurrentlySweptUpTo.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCurrentlySweptUpToUnlessExists(TransactionSweepRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, CurrentlySweptUpTo.of(value)));
    }

    public void putCurrentlySweptUpToUnlessExists(Map<TransactionSweepRow, Long> map) {
        Map<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<TransactionSweepRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CurrentlySweptUpTo.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<TransactionSweepRow, ? extends TransactionSweepNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (TransactionSweepTrigger trigger : triggers) {
            trigger.putTransactionSweep(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<TransactionSweepRow, ? extends TransactionSweepNamedColumnValue<?>> rows) {
        Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<TransactionSweepRow, ? extends TransactionSweepNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteAllTransactionsGoodBefore(TransactionSweepRow row) {
        deleteAllTransactionsGoodBefore(ImmutableSet.of(row));
    }

    public void deleteAllTransactionsGoodBefore(Iterable<TransactionSweepRow> rows) {
        byte[] col = PtBytes.toCachedBytes("a");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteLastSweepTime(TransactionSweepRow row) {
        deleteLastSweepTime(ImmutableSet.of(row));
    }

    public void deleteLastSweepTime(Iterable<TransactionSweepRow> rows) {
        byte[] col = PtBytes.toCachedBytes("s");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCellsDeleted(TransactionSweepRow row) {
        deleteCellsDeleted(ImmutableSet.of(row));
    }

    public void deleteCellsDeleted(Iterable<TransactionSweepRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCurrentlySweptUpTo(TransactionSweepRow row) {
        deleteCurrentlySweptUpTo(ImmutableSet.of(row));
    }

    public void deleteCurrentlySweptUpTo(Iterable<TransactionSweepRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(TransactionSweepRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<TransactionSweepRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 4);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("a")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("s")));
        t.delete(tableRef, cells);
    }

    @Override
    public Optional<TransactionSweepRowResult> getRow(TransactionSweepRow row) {
        return getRow(row, allColumns);
    }

    @Override
    public Optional<TransactionSweepRowResult> getRow(TransactionSweepRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(TransactionSweepRowResult.of(rowResult));
        }
    }

    @Override
    public List<TransactionSweepRowResult> getRows(Iterable<TransactionSweepRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<TransactionSweepRowResult> getRows(Iterable<TransactionSweepRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<TransactionSweepRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(TransactionSweepRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<TransactionSweepRowResult> getAsyncRows(Iterable<TransactionSweepRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, allColumns, exec);
    }

    @Override
    public List<TransactionSweepRowResult> getAsyncRows(final Iterable<TransactionSweepRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<TransactionSweepRowResult>> c =
                new Callable<List<TransactionSweepRowResult>>() {
            @Override
            public List<TransactionSweepRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<TransactionSweepNamedColumnValue<?>> getRowColumns(TransactionSweepRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<TransactionSweepNamedColumnValue<?>> getRowColumns(TransactionSweepRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<TransactionSweepNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getRowsMultimap(Iterable<TransactionSweepRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getRowsMultimap(Iterable<TransactionSweepRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<TransactionSweepRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, allColumns, exec);
    }

    @Override
    public Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<TransactionSweepRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>>> c =
                new Callable<Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>>>() {
            @Override
            public Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getRowsMultimapInternal(Iterable<TransactionSweepRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<TransactionSweepRow, TransactionSweepNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            TransactionSweepRow row = TransactionSweepRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<TransactionSweepRow, BatchingVisitable<TransactionSweepNamedColumnValue<?>>> getRowsColumnRange(Iterable<TransactionSweepRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<TransactionSweepRow, BatchingVisitable<TransactionSweepNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            TransactionSweepRow row = TransactionSweepRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<TransactionSweepNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<TransactionSweepRow, TransactionSweepNamedColumnValue<?>>> getRowsColumnRange(Iterable<TransactionSweepRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            TransactionSweepRow row = TransactionSweepRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            TransactionSweepNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    public BatchingVisitableView<TransactionSweepRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<TransactionSweepRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, TransactionSweepRowResult>() {
            @Override
            public TransactionSweepRowResult apply(RowResult<byte[]> input) {
                return TransactionSweepRowResult.of(input);
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
    static String __CLASS_HASH = "Tz8miErR4NCPOZuFO0IWUw==";
}
