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
public final class SweepProgressTable implements
        AtlasDbMutablePersistentTable<SweepProgressTable.SweepProgressRow,
                                         SweepProgressTable.SweepProgressNamedColumnValue<?>,
                                         SweepProgressTable.SweepProgressRowResult>,
        AtlasDbNamedMutableTable<SweepProgressTable.SweepProgressRow,
                                    SweepProgressTable.SweepProgressNamedColumnValue<?>,
                                    SweepProgressTable.SweepProgressRowResult> {
    private final Transaction t;
    private final List<SweepProgressTrigger> triggers;
    private final static String rawTableName = "progress";
    private final TableReference tableRef;

    static SweepProgressTable of(Transaction t, Namespace namespace) {
        return new SweepProgressTable(t, namespace, ImmutableList.<SweepProgressTrigger>of());
    }

    static SweepProgressTable of(Transaction t, Namespace namespace, SweepProgressTrigger trigger, SweepProgressTrigger... triggers) {
        return new SweepProgressTable(t, namespace, ImmutableList.<SweepProgressTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepProgressTable of(Transaction t, Namespace namespace, List<SweepProgressTrigger> triggers) {
        return new SweepProgressTable(t, namespace, triggers);
    }

    private SweepProgressTable(Transaction t, Namespace namespace, List<SweepProgressTrigger> triggers) {
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
     * SweepProgressRow {
     *   {@literal Long dummy};
     * }
     * </pre>
     */
    public static final class SweepProgressRow implements Persistable, Comparable<SweepProgressRow> {
        private final long dummy;

        public static SweepProgressRow of(long dummy) {
            return new SweepProgressRow(dummy);
        }

        private SweepProgressRow(long dummy) {
            this.dummy = dummy;
        }

        public long getDummy() {
            return dummy;
        }

        public static Function<SweepProgressRow, Long> getDummyFun() {
            return new Function<SweepProgressRow, Long>() {
                @Override
                public Long apply(SweepProgressRow row) {
                    return row.dummy;
                }
            };
        }

        public static Function<Long, SweepProgressRow> fromDummyFun() {
            return new Function<Long, SweepProgressRow>() {
                @Override
                public SweepProgressRow apply(Long row) {
                    return SweepProgressRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] dummyBytes = EncodingUtils.encodeUnsignedVarLong(dummy);
            return EncodingUtils.add(dummyBytes);
        }

        public static final Hydrator<SweepProgressRow> BYTES_HYDRATOR = new Hydrator<SweepProgressRow>() {
            @Override
            public SweepProgressRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long dummy = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(dummy);
                return new SweepProgressRow(dummy);
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
            SweepProgressRow other = (SweepProgressRow) obj;
            return Objects.equal(dummy, other.dummy);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dummy);
        }

        @Override
        public int compareTo(SweepProgressRow o) {
            return ComparisonChain.start()
                .compare(this.dummy, o.dummy)
                .result();
        }
    }

    public interface SweepProgressNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class CellsDeleted implements SweepProgressNamedColumnValue<Long> {
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
    public static final class CellsExamined implements SweepProgressNamedColumnValue<Long> {
        private final Long value;

        public static CellsExamined of(Long value) {
            return new CellsExamined(value);
        }

        private CellsExamined(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "cells_examined";
        }

        @Override
        public String getShortColumnName() {
            return "e";
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
            return PtBytes.toCachedBytes("e");
        }

        public static final Hydrator<CellsExamined> BYTES_HYDRATOR = new Hydrator<CellsExamined>() {
            @Override
            public CellsExamined hydrateFromBytes(byte[] bytes) {
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
     *   type: String;
     * }
     * </pre>
     */
    public static final class FullTableName implements SweepProgressNamedColumnValue<String> {
        private final String value;

        public static FullTableName of(String value) {
            return new FullTableName(value);
        }

        private FullTableName(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "full_table_name";
        }

        @Override
        public String getShortColumnName() {
            return "n";
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("n");
        }

        public static final Hydrator<FullTableName> BYTES_HYDRATOR = new Hydrator<FullTableName>() {
            @Override
            public FullTableName hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(PtBytes.toString(bytes, 0, bytes.length-0));
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
    public static final class MinimumSweptTimestamp implements SweepProgressNamedColumnValue<Long> {
        private final Long value;

        public static MinimumSweptTimestamp of(Long value) {
            return new MinimumSweptTimestamp(value);
        }

        private MinimumSweptTimestamp(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "minimum_swept_timestamp";
        }

        @Override
        public String getShortColumnName() {
            return "m";
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
            return PtBytes.toCachedBytes("m");
        }

        public static final Hydrator<MinimumSweptTimestamp> BYTES_HYDRATOR = new Hydrator<MinimumSweptTimestamp>() {
            @Override
            public MinimumSweptTimestamp hydrateFromBytes(byte[] bytes) {
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
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class StartRow implements SweepProgressNamedColumnValue<byte[]> {
        private final byte[] value;

        public static StartRow of(byte[] value) {
            return new StartRow(value);
        }

        private StartRow(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "start_row";
        }

        @Override
        public String getShortColumnName() {
            return "s";
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value;
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("s");
        }

        public static final Hydrator<StartRow> BYTES_HYDRATOR = new Hydrator<StartRow>() {
            @Override
            public StartRow hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.getBytesFromOffsetToEnd(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface SweepProgressTrigger {
        public void putSweepProgress(Multimap<SweepProgressRow, ? extends SweepProgressNamedColumnValue<?>> newRows);
    }

    public static final class SweepProgressRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SweepProgressRowResult of(RowResult<byte[]> row) {
            return new SweepProgressRowResult(row);
        }

        private SweepProgressRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SweepProgressRow getRowName() {
            return SweepProgressRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SweepProgressRowResult, SweepProgressRow> getRowNameFun() {
            return new Function<SweepProgressRowResult, SweepProgressRow>() {
                @Override
                public SweepProgressRow apply(SweepProgressRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SweepProgressRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SweepProgressRowResult>() {
                @Override
                public SweepProgressRowResult apply(RowResult<byte[]> rowResult) {
                    return new SweepProgressRowResult(rowResult);
                }
            };
        }

        public boolean hasCellsDeleted() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public boolean hasCellsExamined() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("e"));
        }

        public boolean hasFullTableName() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("n"));
        }

        public boolean hasMinimumSweptTimestamp() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("m"));
        }

        public boolean hasStartRow() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("s"));
        }

        public Long getCellsDeleted() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("d"));
            if (bytes == null) {
                return null;
            }
            CellsDeleted value = CellsDeleted.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getCellsExamined() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("e"));
            if (bytes == null) {
                return null;
            }
            CellsExamined value = CellsExamined.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public String getFullTableName() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("n"));
            if (bytes == null) {
                return null;
            }
            FullTableName value = FullTableName.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getMinimumSweptTimestamp() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("m"));
            if (bytes == null) {
                return null;
            }
            MinimumSweptTimestamp value = MinimumSweptTimestamp.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public byte[] getStartRow() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("s"));
            if (bytes == null) {
                return null;
            }
            StartRow value = StartRow.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SweepProgressRowResult, Long> getCellsDeletedFun() {
            return new Function<SweepProgressRowResult, Long>() {
                @Override
                public Long apply(SweepProgressRowResult rowResult) {
                    return rowResult.getCellsDeleted();
                }
            };
        }

        public static Function<SweepProgressRowResult, Long> getCellsExaminedFun() {
            return new Function<SweepProgressRowResult, Long>() {
                @Override
                public Long apply(SweepProgressRowResult rowResult) {
                    return rowResult.getCellsExamined();
                }
            };
        }

        public static Function<SweepProgressRowResult, String> getFullTableNameFun() {
            return new Function<SweepProgressRowResult, String>() {
                @Override
                public String apply(SweepProgressRowResult rowResult) {
                    return rowResult.getFullTableName();
                }
            };
        }

        public static Function<SweepProgressRowResult, Long> getMinimumSweptTimestampFun() {
            return new Function<SweepProgressRowResult, Long>() {
                @Override
                public Long apply(SweepProgressRowResult rowResult) {
                    return rowResult.getMinimumSweptTimestamp();
                }
            };
        }

        public static Function<SweepProgressRowResult, byte[]> getStartRowFun() {
            return new Function<SweepProgressRowResult, byte[]>() {
                @Override
                public byte[] apply(SweepProgressRowResult rowResult) {
                    return rowResult.getStartRow();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("CellsDeleted", getCellsDeleted())
                .add("CellsExamined", getCellsExamined())
                .add("FullTableName", getFullTableName())
                .add("MinimumSweptTimestamp", getMinimumSweptTimestamp())
                .add("StartRow", getStartRow())
                .toString();
        }
    }

    public enum SweepProgressNamedColumn {
        CELLS_DELETED {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("d");
            }
        },
        CELLS_EXAMINED {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("e");
            }
        },
        FULL_TABLE_NAME {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("n");
            }
        },
        MINIMUM_SWEPT_TIMESTAMP {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("m");
            }
        },
        START_ROW {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("s");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SweepProgressNamedColumn, byte[]> toShortName() {
            return new Function<SweepProgressNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SweepProgressNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SweepProgressNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SweepProgressNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SweepProgressNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SweepProgressNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SweepProgressNamedColumnValue<?>>>builder()
                .put("n", FullTableName.BYTES_HYDRATOR)
                .put("m", MinimumSweptTimestamp.BYTES_HYDRATOR)
                .put("s", StartRow.BYTES_HYDRATOR)
                .put("d", CellsDeleted.BYTES_HYDRATOR)
                .put("e", CellsExamined.BYTES_HYDRATOR)
                .build();

    public Map<SweepProgressRow, String> getFullTableNames(Collection<SweepProgressRow> rows) {
        Map<Cell, SweepProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("n")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepProgressRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = FullTableName.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepProgressRow, Long> getMinimumSweptTimestamps(Collection<SweepProgressRow> rows) {
        Map<Cell, SweepProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("m")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepProgressRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = MinimumSweptTimestamp.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepProgressRow, byte[]> getStartRows(Collection<SweepProgressRow> rows) {
        Map<Cell, SweepProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("s")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepProgressRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = StartRow.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepProgressRow, Long> getCellsDeleteds(Collection<SweepProgressRow> rows) {
        Map<Cell, SweepProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepProgressRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CellsDeleted.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepProgressRow, Long> getCellsExamineds(Collection<SweepProgressRow> rows) {
        Map<Cell, SweepProgressRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepProgressRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("e")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepProgressRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CellsExamined.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putFullTableName(SweepProgressRow row, String value) {
        put(ImmutableMultimap.of(row, FullTableName.of(value)));
    }

    public void putFullTableName(Map<SweepProgressRow, String> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), FullTableName.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putFullTableNameUnlessExists(SweepProgressRow row, String value) {
        putUnlessExists(ImmutableMultimap.of(row, FullTableName.of(value)));
    }

    public void putFullTableNameUnlessExists(Map<SweepProgressRow, String> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), FullTableName.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putMinimumSweptTimestamp(SweepProgressRow row, Long value) {
        put(ImmutableMultimap.of(row, MinimumSweptTimestamp.of(value)));
    }

    public void putMinimumSweptTimestamp(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), MinimumSweptTimestamp.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putMinimumSweptTimestampUnlessExists(SweepProgressRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, MinimumSweptTimestamp.of(value)));
    }

    public void putMinimumSweptTimestampUnlessExists(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), MinimumSweptTimestamp.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putStartRow(SweepProgressRow row, byte[] value) {
        put(ImmutableMultimap.of(row, StartRow.of(value)));
    }

    public void putStartRow(Map<SweepProgressRow, byte[]> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), StartRow.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putStartRowUnlessExists(SweepProgressRow row, byte[] value) {
        putUnlessExists(ImmutableMultimap.of(row, StartRow.of(value)));
    }

    public void putStartRowUnlessExists(Map<SweepProgressRow, byte[]> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), StartRow.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putCellsDeleted(SweepProgressRow row, Long value) {
        put(ImmutableMultimap.of(row, CellsDeleted.of(value)));
    }

    public void putCellsDeleted(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsDeleted.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCellsDeletedUnlessExists(SweepProgressRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, CellsDeleted.of(value)));
    }

    public void putCellsDeletedUnlessExists(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsDeleted.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putCellsExamined(SweepProgressRow row, Long value) {
        put(ImmutableMultimap.of(row, CellsExamined.of(value)));
    }

    public void putCellsExamined(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsExamined.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCellsExaminedUnlessExists(SweepProgressRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, CellsExamined.of(value)));
    }

    public void putCellsExaminedUnlessExists(Map<SweepProgressRow, Long> map) {
        Map<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepProgressRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsExamined.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SweepProgressRow, ? extends SweepProgressNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SweepProgressTrigger trigger : triggers) {
            trigger.putSweepProgress(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<SweepProgressRow, ? extends SweepProgressNamedColumnValue<?>> rows) {
        Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<SweepProgressRow, ? extends SweepProgressNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteFullTableName(SweepProgressRow row) {
        deleteFullTableName(ImmutableSet.of(row));
    }

    public void deleteFullTableName(Iterable<SweepProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("n");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteMinimumSweptTimestamp(SweepProgressRow row) {
        deleteMinimumSweptTimestamp(ImmutableSet.of(row));
    }

    public void deleteMinimumSweptTimestamp(Iterable<SweepProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("m");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteStartRow(SweepProgressRow row) {
        deleteStartRow(ImmutableSet.of(row));
    }

    public void deleteStartRow(Iterable<SweepProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("s");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCellsDeleted(SweepProgressRow row) {
        deleteCellsDeleted(ImmutableSet.of(row));
    }

    public void deleteCellsDeleted(Iterable<SweepProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCellsExamined(SweepProgressRow row) {
        deleteCellsExamined(ImmutableSet.of(row));
    }

    public void deleteCellsExamined(Iterable<SweepProgressRow> rows) {
        byte[] col = PtBytes.toCachedBytes("e");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SweepProgressRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SweepProgressRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 5);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("e")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("n")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("m")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("s")));
        t.delete(tableRef, cells);
    }

    @Override
    public Optional<SweepProgressRowResult> getRow(SweepProgressRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<SweepProgressRowResult> getRow(SweepProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(SweepProgressRowResult.of(rowResult));
        }
    }

    @Override
    public List<SweepProgressRowResult> getRows(Iterable<SweepProgressRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<SweepProgressRowResult> getRows(Iterable<SweepProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SweepProgressRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SweepProgressRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SweepProgressRowResult> getAsyncRows(Iterable<SweepProgressRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<SweepProgressRowResult> getAsyncRows(final Iterable<SweepProgressRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<SweepProgressRowResult>> c =
                new Callable<List<SweepProgressRowResult>>() {
            @Override
            public List<SweepProgressRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<SweepProgressNamedColumnValue<?>> getRowColumns(SweepProgressRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<SweepProgressNamedColumnValue<?>> getRowColumns(SweepProgressRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepProgressNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getRowsMultimap(Iterable<SweepProgressRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getRowsMultimap(Iterable<SweepProgressRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<SweepProgressRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<SweepProgressRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>>> c =
                new Callable<Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>>>() {
            @Override
            public Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SweepProgressRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepProgressRow, SweepProgressNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepProgressRow row = SweepProgressRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<SweepProgressRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<SweepProgressRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, SweepProgressRowResult>() {
            @Override
            public SweepProgressRowResult apply(RowResult<byte[]> input) {
                return SweepProgressRowResult.of(input);
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
    static String __CLASS_HASH = "JdYIsvXhQw3ZkZMVh3ypQA==";
}
