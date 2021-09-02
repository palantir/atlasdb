package com.palantir.atlasdb.schema.generated;

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
public final class SweepPriorityTable implements
        AtlasDbMutablePersistentTable<SweepPriorityTable.SweepPriorityRow,
                                         SweepPriorityTable.SweepPriorityNamedColumnValue<?>,
                                         SweepPriorityTable.SweepPriorityRowResult>,
        AtlasDbNamedMutableTable<SweepPriorityTable.SweepPriorityRow,
                                    SweepPriorityTable.SweepPriorityNamedColumnValue<?>,
                                    SweepPriorityTable.SweepPriorityRowResult> {
    private final Transaction t;
    private final List<SweepPriorityTrigger> triggers;
    private final static String rawTableName = "priority";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(SweepPriorityNamedColumn.values());

    static SweepPriorityTable of(Transaction t, Namespace namespace) {
        return new SweepPriorityTable(t, namespace, ImmutableList.<SweepPriorityTrigger>of());
    }

    static SweepPriorityTable of(Transaction t, Namespace namespace, SweepPriorityTrigger trigger, SweepPriorityTrigger... triggers) {
        return new SweepPriorityTable(t, namespace, ImmutableList.<SweepPriorityTrigger>builder().add(trigger).add(triggers).build());
    }

    static SweepPriorityTable of(Transaction t, Namespace namespace, List<SweepPriorityTrigger> triggers) {
        return new SweepPriorityTable(t, namespace, triggers);
    }

    private SweepPriorityTable(Transaction t, Namespace namespace, List<SweepPriorityTrigger> triggers) {
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
     * SweepPriorityRow {
     *   {@literal String fullTableName};
     * }
     * </pre>
     */
    public static final class SweepPriorityRow implements Persistable, Comparable<SweepPriorityRow> {
        private final String fullTableName;

        public static SweepPriorityRow of(String fullTableName) {
            return new SweepPriorityRow(fullTableName);
        }

        private SweepPriorityRow(String fullTableName) {
            this.fullTableName = fullTableName;
        }

        public String getFullTableName() {
            return fullTableName;
        }

        public static Function<SweepPriorityRow, String> getFullTableNameFun() {
            return new Function<SweepPriorityRow, String>() {
                @Override
                public String apply(SweepPriorityRow row) {
                    return row.fullTableName;
                }
            };
        }

        public static Function<String, SweepPriorityRow> fromFullTableNameFun() {
            return new Function<String, SweepPriorityRow>() {
                @Override
                public SweepPriorityRow apply(String row) {
                    return SweepPriorityRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] fullTableNameBytes = PtBytes.toBytes(fullTableName);
            return EncodingUtils.add(fullTableNameBytes);
        }

        public static final Hydrator<SweepPriorityRow> BYTES_HYDRATOR = new Hydrator<SweepPriorityRow>() {
            @Override
            public SweepPriorityRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String fullTableName = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return new SweepPriorityRow(fullTableName);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("fullTableName", fullTableName)
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
            SweepPriorityRow other = (SweepPriorityRow) obj;
            return Objects.equals(fullTableName, other.fullTableName);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(fullTableName);
        }

        @Override
        public int compareTo(SweepPriorityRow o) {
            return ComparisonChain.start()
                .compare(this.fullTableName, o.fullTableName)
                .result();
        }
    }

    public interface SweepPriorityNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class CellsDeleted implements SweepPriorityNamedColumnValue<Long> {
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
    public static final class CellsExamined implements SweepPriorityNamedColumnValue<Long> {
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
     *   type: Long;
     * }
     * </pre>
     */
    public static final class LastSweepTime implements SweepPriorityNamedColumnValue<Long> {
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
            return "t";
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
            return PtBytes.toCachedBytes("t");
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

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class MinimumSweptTimestamp implements SweepPriorityNamedColumnValue<Long> {
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
     *   type: Long;
     * }
     * </pre>
     */
    public static final class WriteCount implements SweepPriorityNamedColumnValue<Long> {
        private final Long value;

        public static WriteCount of(Long value) {
            return new WriteCount(value);
        }

        private WriteCount(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "write_count";
        }

        @Override
        public String getShortColumnName() {
            return "w";
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
            return PtBytes.toCachedBytes("w");
        }

        public static final Hydrator<WriteCount> BYTES_HYDRATOR = new Hydrator<WriteCount>() {
            @Override
            public WriteCount hydrateFromBytes(byte[] bytes) {
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

    public interface SweepPriorityTrigger {
        public void putSweepPriority(Multimap<SweepPriorityRow, ? extends SweepPriorityNamedColumnValue<?>> newRows);
    }

    public static final class SweepPriorityRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static SweepPriorityRowResult of(RowResult<byte[]> row) {
            return new SweepPriorityRowResult(row);
        }

        private SweepPriorityRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public SweepPriorityRow getRowName() {
            return SweepPriorityRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<SweepPriorityRowResult, SweepPriorityRow> getRowNameFun() {
            return new Function<SweepPriorityRowResult, SweepPriorityRow>() {
                @Override
                public SweepPriorityRow apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, SweepPriorityRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, SweepPriorityRowResult>() {
                @Override
                public SweepPriorityRowResult apply(RowResult<byte[]> rowResult) {
                    return new SweepPriorityRowResult(rowResult);
                }
            };
        }

        public boolean hasCellsDeleted() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("d"));
        }

        public boolean hasCellsExamined() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("e"));
        }

        public boolean hasLastSweepTime() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("t"));
        }

        public boolean hasMinimumSweptTimestamp() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("m"));
        }

        public boolean hasWriteCount() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("w"));
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

        public Long getLastSweepTime() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("t"));
            if (bytes == null) {
                return null;
            }
            LastSweepTime value = LastSweepTime.BYTES_HYDRATOR.hydrateFromBytes(bytes);
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

        public Long getWriteCount() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("w"));
            if (bytes == null) {
                return null;
            }
            WriteCount value = WriteCount.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<SweepPriorityRowResult, Long> getCellsDeletedFun() {
            return new Function<SweepPriorityRowResult, Long>() {
                @Override
                public Long apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getCellsDeleted();
                }
            };
        }

        public static Function<SweepPriorityRowResult, Long> getCellsExaminedFun() {
            return new Function<SweepPriorityRowResult, Long>() {
                @Override
                public Long apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getCellsExamined();
                }
            };
        }

        public static Function<SweepPriorityRowResult, Long> getLastSweepTimeFun() {
            return new Function<SweepPriorityRowResult, Long>() {
                @Override
                public Long apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getLastSweepTime();
                }
            };
        }

        public static Function<SweepPriorityRowResult, Long> getMinimumSweptTimestampFun() {
            return new Function<SweepPriorityRowResult, Long>() {
                @Override
                public Long apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getMinimumSweptTimestamp();
                }
            };
        }

        public static Function<SweepPriorityRowResult, Long> getWriteCountFun() {
            return new Function<SweepPriorityRowResult, Long>() {
                @Override
                public Long apply(SweepPriorityRowResult rowResult) {
                    return rowResult.getWriteCount();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("CellsDeleted", getCellsDeleted())
                .add("CellsExamined", getCellsExamined())
                .add("LastSweepTime", getLastSweepTime())
                .add("MinimumSweptTimestamp", getMinimumSweptTimestamp())
                .add("WriteCount", getWriteCount())
                .toString();
        }
    }

    public enum SweepPriorityNamedColumn {
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
        LAST_SWEEP_TIME {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("t");
            }
        },
        MINIMUM_SWEPT_TIMESTAMP {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("m");
            }
        },
        WRITE_COUNT {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("w");
            }
        };

        public abstract byte[] getShortName();

        public static Function<SweepPriorityNamedColumn, byte[]> toShortName() {
            return new Function<SweepPriorityNamedColumn, byte[]>() {
                @Override
                public byte[] apply(SweepPriorityNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<SweepPriorityNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, SweepPriorityNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(SweepPriorityNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends SweepPriorityNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends SweepPriorityNamedColumnValue<?>>>builder()
                .put("w", WriteCount.BYTES_HYDRATOR)
                .put("t", LastSweepTime.BYTES_HYDRATOR)
                .put("m", MinimumSweptTimestamp.BYTES_HYDRATOR)
                .put("d", CellsDeleted.BYTES_HYDRATOR)
                .put("e", CellsExamined.BYTES_HYDRATOR)
                .build();

    public Map<SweepPriorityRow, Long> getWriteCounts(Collection<SweepPriorityRow> rows) {
        Map<Cell, SweepPriorityRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepPriorityRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("w")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepPriorityRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = WriteCount.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepPriorityRow, Long> getLastSweepTimes(Collection<SweepPriorityRow> rows) {
        Map<Cell, SweepPriorityRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepPriorityRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("t")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepPriorityRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = LastSweepTime.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepPriorityRow, Long> getMinimumSweptTimestamps(Collection<SweepPriorityRow> rows) {
        Map<Cell, SweepPriorityRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepPriorityRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("m")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepPriorityRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = MinimumSweptTimestamp.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepPriorityRow, Long> getCellsDeleteds(Collection<SweepPriorityRow> rows) {
        Map<Cell, SweepPriorityRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepPriorityRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("d")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepPriorityRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CellsDeleted.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<SweepPriorityRow, Long> getCellsExamineds(Collection<SweepPriorityRow> rows) {
        Map<Cell, SweepPriorityRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (SweepPriorityRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("e")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<SweepPriorityRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = CellsExamined.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putWriteCount(SweepPriorityRow row, Long value) {
        put(ImmutableMultimap.of(row, WriteCount.of(value)));
    }

    public void putWriteCount(Map<SweepPriorityRow, Long> map) {
        Map<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepPriorityRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), WriteCount.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putLastSweepTime(SweepPriorityRow row, Long value) {
        put(ImmutableMultimap.of(row, LastSweepTime.of(value)));
    }

    public void putLastSweepTime(Map<SweepPriorityRow, Long> map) {
        Map<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepPriorityRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), LastSweepTime.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putMinimumSweptTimestamp(SweepPriorityRow row, Long value) {
        put(ImmutableMultimap.of(row, MinimumSweptTimestamp.of(value)));
    }

    public void putMinimumSweptTimestamp(Map<SweepPriorityRow, Long> map) {
        Map<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepPriorityRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), MinimumSweptTimestamp.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCellsDeleted(SweepPriorityRow row, Long value) {
        put(ImmutableMultimap.of(row, CellsDeleted.of(value)));
    }

    public void putCellsDeleted(Map<SweepPriorityRow, Long> map) {
        Map<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepPriorityRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsDeleted.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCellsExamined(SweepPriorityRow row, Long value) {
        put(ImmutableMultimap.of(row, CellsExamined.of(value)));
    }

    public void putCellsExamined(Map<SweepPriorityRow, Long> map) {
        Map<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<SweepPriorityRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), CellsExamined.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<SweepPriorityRow, ? extends SweepPriorityNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (SweepPriorityTrigger trigger : triggers) {
            trigger.putSweepPriority(rows);
        }
    }

    public void deleteWriteCount(SweepPriorityRow row) {
        deleteWriteCount(ImmutableSet.of(row));
    }

    public void deleteWriteCount(Iterable<SweepPriorityRow> rows) {
        byte[] col = PtBytes.toCachedBytes("w");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteLastSweepTime(SweepPriorityRow row) {
        deleteLastSweepTime(ImmutableSet.of(row));
    }

    public void deleteLastSweepTime(Iterable<SweepPriorityRow> rows) {
        byte[] col = PtBytes.toCachedBytes("t");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteMinimumSweptTimestamp(SweepPriorityRow row) {
        deleteMinimumSweptTimestamp(ImmutableSet.of(row));
    }

    public void deleteMinimumSweptTimestamp(Iterable<SweepPriorityRow> rows) {
        byte[] col = PtBytes.toCachedBytes("m");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCellsDeleted(SweepPriorityRow row) {
        deleteCellsDeleted(ImmutableSet.of(row));
    }

    public void deleteCellsDeleted(Iterable<SweepPriorityRow> rows) {
        byte[] col = PtBytes.toCachedBytes("d");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteCellsExamined(SweepPriorityRow row) {
        deleteCellsExamined(ImmutableSet.of(row));
    }

    public void deleteCellsExamined(Iterable<SweepPriorityRow> rows) {
        byte[] col = PtBytes.toCachedBytes("e");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(SweepPriorityRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<SweepPriorityRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 5);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("d")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("e")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("t")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("m")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("w")));
        t.delete(tableRef, cells);
    }

    public Optional<SweepPriorityRowResult> getRow(SweepPriorityRow row) {
        return getRow(row, allColumns);
    }

    public Optional<SweepPriorityRowResult> getRow(SweepPriorityRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(SweepPriorityRowResult.of(rowResult));
        }
    }

    @Override
    public List<SweepPriorityRowResult> getRows(Iterable<SweepPriorityRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<SweepPriorityRowResult> getRows(Iterable<SweepPriorityRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<SweepPriorityRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(SweepPriorityRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<SweepPriorityNamedColumnValue<?>> getRowColumns(SweepPriorityRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<SweepPriorityNamedColumnValue<?>> getRowColumns(SweepPriorityRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<SweepPriorityNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> getRowsMultimap(Iterable<SweepPriorityRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> getRowsMultimap(Iterable<SweepPriorityRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> getRowsMultimapInternal(Iterable<SweepPriorityRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<SweepPriorityRow, SweepPriorityNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            SweepPriorityRow row = SweepPriorityRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<SweepPriorityRow, BatchingVisitable<SweepPriorityNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepPriorityRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepPriorityRow, BatchingVisitable<SweepPriorityNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepPriorityRow row = SweepPriorityRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<SweepPriorityNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<SweepPriorityRow, SweepPriorityNamedColumnValue<?>>> getRowsColumnRange(Iterable<SweepPriorityRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            SweepPriorityRow row = SweepPriorityRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            SweepPriorityNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<SweepPriorityRow, Iterator<SweepPriorityNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<SweepPriorityRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<SweepPriorityRow, Iterator<SweepPriorityNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            SweepPriorityRow row = SweepPriorityRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<SweepPriorityNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private RangeRequest optimizeRangeRequest(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            return range.getBuilder().retainColumns(allColumns).build();
        }
        return range;
    }

    private Iterable<RangeRequest> optimizeRangeRequests(Iterable<RangeRequest> ranges) {
        return Iterables.transform(ranges, this::optimizeRangeRequest);
    }

    public BatchingVisitableView<SweepPriorityRowResult> getRange(RangeRequest range) {
        return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, SweepPriorityRowResult>() {
            @Override
            public SweepPriorityRowResult apply(RowResult<byte[]> input) {
                return SweepPriorityRowResult.of(input);
            }
        });
    }

    @Deprecated
    public IterableView<BatchingVisitable<SweepPriorityRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<SweepPriorityRowResult>>() {
            @Override
            public BatchingVisitable<SweepPriorityRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, SweepPriorityRowResult>() {
                    @Override
                    public SweepPriorityRowResult apply(RowResult<byte[]> row) {
                        return SweepPriorityRowResult.of(row);
                    }
                });
            }
        });
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   int concurrencyLevel,
                                   BiFunction<RangeRequest, BatchingVisitable<SweepPriorityRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .concurrencyLevel(concurrencyLevel)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SweepPriorityRowResult::of)))
                            .build());
    }

    public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                   BiFunction<RangeRequest, BatchingVisitable<SweepPriorityRowResult>, T> visitableProcessor) {
        return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                            .tableRef(tableRef)
                            .rangeRequests(ranges)
                            .rangeRequestOptimizer(this::optimizeRangeRequest)
                            .visitableProcessor((rangeRequest, visitable) ->
                                    visitableProcessor.apply(rangeRequest,
                                            BatchingVisitables.transform(visitable, SweepPriorityRowResult::of)))
                            .build());
    }

    public Stream<BatchingVisitable<SweepPriorityRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
        Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
        return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, SweepPriorityRowResult::of));
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(SweepPriorityRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<SweepPriorityRow>, RuntimeException>() {
            @Override
            public boolean visit(List<SweepPriorityRow> rows) {
                delete(rows);
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
    static String __CLASS_HASH = "x4e//sU8HojuLoJg24wmLA==";
}
