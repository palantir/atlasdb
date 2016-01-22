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
import com.palantir.atlasdb.keyvalue.api.Prefix;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.Namespace;
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
public final class UpgTaskMetadataTable implements
        AtlasDbMutablePersistentTable<UpgTaskMetadataTable.UpgTaskMetadataRow,
                                         UpgTaskMetadataTable.UpgTaskMetadataNamedColumnValue<?>,
                                         UpgTaskMetadataTable.UpgTaskMetadataRowResult>,
        AtlasDbNamedMutableTable<UpgTaskMetadataTable.UpgTaskMetadataRow,
                                    UpgTaskMetadataTable.UpgTaskMetadataNamedColumnValue<?>,
                                    UpgTaskMetadataTable.UpgTaskMetadataRowResult> {
    private final Transaction t;
    private final List<UpgTaskMetadataTrigger> triggers;
    private final static String rawTableName = "upg_task_metadata";
    private final String tableName;
    private final Namespace namespace;

    static UpgTaskMetadataTable of(Transaction t, Namespace namespace) {
        return new UpgTaskMetadataTable(t, namespace, ImmutableList.<UpgTaskMetadataTrigger>of());
    }

    static UpgTaskMetadataTable of(Transaction t, Namespace namespace, UpgTaskMetadataTrigger trigger, UpgTaskMetadataTrigger... triggers) {
        return new UpgTaskMetadataTable(t, namespace, ImmutableList.<UpgTaskMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static UpgTaskMetadataTable of(Transaction t, Namespace namespace, List<UpgTaskMetadataTrigger> triggers) {
        return new UpgTaskMetadataTable(t, namespace, triggers);
    }

    private UpgTaskMetadataTable(Transaction t, Namespace namespace, List<UpgTaskMetadataTrigger> triggers) {
        this.t = t;
        this.tableName = namespace.getName().isEmpty() ? rawTableName : namespace.getName() + "." + rawTableName;
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
     * UpgTaskMetadataRow {
     *   {@literal String namespace};
     *   {@literal Long version};
     *   {@literal Long hotfixVersion};
     *   {@literal Long hotfixHotfix};
     *   {@literal String extraId};
     *   {@literal Long rangeId};
     * }
     * </pre>
     */
    public static final class UpgTaskMetadataRow implements Persistable, Comparable<UpgTaskMetadataRow> {
        private final String namespace;
        private final long version;
        private final long hotfixVersion;
        private final long hotfixHotfix;
        private final String extraId;
        private final long rangeId;

        public static UpgTaskMetadataRow of(String namespace, long version, long hotfixVersion, long hotfixHotfix, String extraId, long rangeId) {
            return new UpgTaskMetadataRow(namespace, version, hotfixVersion, hotfixHotfix, extraId, rangeId);
        }

        private UpgTaskMetadataRow(String namespace, long version, long hotfixVersion, long hotfixHotfix, String extraId, long rangeId) {
            this.namespace = namespace;
            this.version = version;
            this.hotfixVersion = hotfixVersion;
            this.hotfixHotfix = hotfixHotfix;
            this.extraId = extraId;
            this.rangeId = rangeId;
        }

        public String getNamespace() {
            return namespace;
        }

        public long getVersion() {
            return version;
        }

        public long getHotfixVersion() {
            return hotfixVersion;
        }

        public long getHotfixHotfix() {
            return hotfixHotfix;
        }

        public String getExtraId() {
            return extraId;
        }

        public long getRangeId() {
            return rangeId;
        }

        public static Function<UpgTaskMetadataRow, String> getNamespaceFun() {
            return new Function<UpgTaskMetadataRow, String>() {
                @Override
                public String apply(UpgTaskMetadataRow row) {
                    return row.namespace;
                }
            };
        }

        public static Function<UpgTaskMetadataRow, Long> getVersionFun() {
            return new Function<UpgTaskMetadataRow, Long>() {
                @Override
                public Long apply(UpgTaskMetadataRow row) {
                    return row.version;
                }
            };
        }

        public static Function<UpgTaskMetadataRow, Long> getHotfixVersionFun() {
            return new Function<UpgTaskMetadataRow, Long>() {
                @Override
                public Long apply(UpgTaskMetadataRow row) {
                    return row.hotfixVersion;
                }
            };
        }

        public static Function<UpgTaskMetadataRow, Long> getHotfixHotfixFun() {
            return new Function<UpgTaskMetadataRow, Long>() {
                @Override
                public Long apply(UpgTaskMetadataRow row) {
                    return row.hotfixHotfix;
                }
            };
        }

        public static Function<UpgTaskMetadataRow, String> getExtraIdFun() {
            return new Function<UpgTaskMetadataRow, String>() {
                @Override
                public String apply(UpgTaskMetadataRow row) {
                    return row.extraId;
                }
            };
        }

        public static Function<UpgTaskMetadataRow, Long> getRangeIdFun() {
            return new Function<UpgTaskMetadataRow, Long>() {
                @Override
                public Long apply(UpgTaskMetadataRow row) {
                    return row.rangeId;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            byte[] hotfixHotfixBytes = EncodingUtils.encodeUnsignedVarLong(hotfixHotfix);
            byte[] extraIdBytes = EncodingUtils.encodeVarString(extraId);
            byte[] rangeIdBytes = EncodingUtils.encodeUnsignedVarLong(rangeId);
            return EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes, hotfixHotfixBytes, extraIdBytes, rangeIdBytes);
        }

        public static final Hydrator<UpgTaskMetadataRow> BYTES_HYDRATOR = new Hydrator<UpgTaskMetadataRow>() {
            @Override
            public UpgTaskMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String namespace = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(namespace);
                Long version = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(version);
                Long hotfixVersion = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(hotfixVersion);
                Long hotfixHotfix = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(hotfixHotfix);
                String extraId = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(extraId);
                Long rangeId = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(rangeId);
                return new UpgTaskMetadataRow(namespace, version, hotfixVersion, hotfixHotfix, extraId, rangeId);
            }
        };

        public static RangeRequest.Builder createPrefixRangeUnsorted(String namespace) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(namespaceBytes));
        }

        public static Prefix prefixUnsorted(String namespace) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            return new Prefix(EncodingUtils.add(namespaceBytes));
        }

        public static RangeRequest.Builder createPrefixRangeUnsorted(String namespace, long version) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(namespaceBytes, versionBytes));
        }

        public static Prefix prefixUnsorted(String namespace, long version) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            return new Prefix(EncodingUtils.add(namespaceBytes, versionBytes));
        }

        public static RangeRequest.Builder createPrefixRangeUnsorted(String namespace, long version, long hotfixVersion) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes));
        }

        public static Prefix prefixUnsorted(String namespace, long version, long hotfixVersion) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            return new Prefix(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes));
        }

        public static RangeRequest.Builder createPrefixRangeUnsorted(String namespace, long version, long hotfixVersion, long hotfixHotfix) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            byte[] hotfixHotfixBytes = EncodingUtils.encodeUnsignedVarLong(hotfixHotfix);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes, hotfixHotfixBytes));
        }

        public static Prefix prefixUnsorted(String namespace, long version, long hotfixVersion, long hotfixHotfix) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            byte[] hotfixHotfixBytes = EncodingUtils.encodeUnsignedVarLong(hotfixHotfix);
            return new Prefix(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes, hotfixHotfixBytes));
        }

        public static RangeRequest.Builder createPrefixRange(String namespace, long version, long hotfixVersion, long hotfixHotfix, String extraId) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            byte[] hotfixHotfixBytes = EncodingUtils.encodeUnsignedVarLong(hotfixHotfix);
            byte[] extraIdBytes = EncodingUtils.encodeVarString(extraId);
            return RangeRequest.builder().prefixRange(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes, hotfixHotfixBytes, extraIdBytes));
        }

        public static Prefix prefix(String namespace, long version, long hotfixVersion, long hotfixHotfix, String extraId) {
            byte[] namespaceBytes = EncodingUtils.encodeVarString(namespace);
            byte[] versionBytes = EncodingUtils.encodeUnsignedVarLong(version);
            byte[] hotfixVersionBytes = EncodingUtils.encodeUnsignedVarLong(hotfixVersion);
            byte[] hotfixHotfixBytes = EncodingUtils.encodeUnsignedVarLong(hotfixHotfix);
            byte[] extraIdBytes = EncodingUtils.encodeVarString(extraId);
            return new Prefix(EncodingUtils.add(namespaceBytes, versionBytes, hotfixVersionBytes, hotfixHotfixBytes, extraIdBytes));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("namespace", namespace)
                .add("version", version)
                .add("hotfixVersion", hotfixVersion)
                .add("hotfixHotfix", hotfixHotfix)
                .add("extraId", extraId)
                .add("rangeId", rangeId)
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
            UpgTaskMetadataRow other = (UpgTaskMetadataRow) obj;
            return Objects.equal(namespace, other.namespace) && Objects.equal(version, other.version) && Objects.equal(hotfixVersion, other.hotfixVersion) && Objects.equal(hotfixHotfix, other.hotfixHotfix) && Objects.equal(extraId, other.extraId) && Objects.equal(rangeId, other.rangeId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(namespace, version, hotfixVersion, hotfixHotfix, extraId, rangeId);
        }

        @Override
        public int compareTo(UpgTaskMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.namespace, o.namespace)
                .compare(this.version, o.version)
                .compare(this.hotfixVersion, o.hotfixVersion)
                .compare(this.hotfixHotfix, o.hotfixHotfix)
                .compare(this.extraId, o.extraId)
                .compare(this.rangeId, o.rangeId)
                .result();
        }
    }

    public interface UpgTaskMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Start implements UpgTaskMetadataNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Start of(byte[] value) {
            return new Start(value);
        }

        private Start(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "start";
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

        public static final Hydrator<Start> BYTES_HYDRATOR = new Hydrator<Start>() {
            @Override
            public Start hydrateFromBytes(byte[] bytes) {
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

    public interface UpgTaskMetadataTrigger {
        public void putUpgTaskMetadata(Multimap<UpgTaskMetadataRow, ? extends UpgTaskMetadataNamedColumnValue<?>> newRows);
    }

    public static final class UpgTaskMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static UpgTaskMetadataRowResult of(RowResult<byte[]> row) {
            return new UpgTaskMetadataRowResult(row);
        }

        private UpgTaskMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public UpgTaskMetadataRow getRowName() {
            return UpgTaskMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<UpgTaskMetadataRowResult, UpgTaskMetadataRow> getRowNameFun() {
            return new Function<UpgTaskMetadataRowResult, UpgTaskMetadataRow>() {
                @Override
                public UpgTaskMetadataRow apply(UpgTaskMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, UpgTaskMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, UpgTaskMetadataRowResult>() {
                @Override
                public UpgTaskMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new UpgTaskMetadataRowResult(rowResult);
                }
            };
        }

        public boolean hasStart() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("s"));
        }

        public byte[] getStart() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("s"));
            if (bytes == null) {
                return null;
            }
            Start value = Start.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<UpgTaskMetadataRowResult, byte[]> getStartFun() {
            return new Function<UpgTaskMetadataRowResult, byte[]>() {
                @Override
                public byte[] apply(UpgTaskMetadataRowResult rowResult) {
                    return rowResult.getStart();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Start", getStart())
                .toString();
        }
    }

    public enum UpgTaskMetadataNamedColumn {
        START {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("s");
            }
        };

        public abstract byte[] getShortName();

        public static Function<UpgTaskMetadataNamedColumn, byte[]> toShortName() {
            return new Function<UpgTaskMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(UpgTaskMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<UpgTaskMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, UpgTaskMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(UpgTaskMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends UpgTaskMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends UpgTaskMetadataNamedColumnValue<?>>>builder()
                .put("s", Start.BYTES_HYDRATOR)
                .build();

    public Map<UpgTaskMetadataRow, byte[]> getStarts(Collection<UpgTaskMetadataRow> rows) {
        Map<Cell, UpgTaskMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UpgTaskMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("s")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UpgTaskMetadataRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Start.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putStart(UpgTaskMetadataRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Start.of(value)));
    }

    public void putStart(Map<UpgTaskMetadataRow, byte[]> map) {
        Map<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgTaskMetadataRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Start.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putStartUnlessExists(UpgTaskMetadataRow row, byte[] value) {
        putUnlessExists(ImmutableMultimap.of(row, Start.of(value)));
    }

    public void putStartUnlessExists(Map<UpgTaskMetadataRow, byte[]> map) {
        Map<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgTaskMetadataRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Start.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<UpgTaskMetadataRow, ? extends UpgTaskMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows));
        for (UpgTaskMetadataTrigger trigger : triggers) {
            trigger.putUpgTaskMetadata(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<UpgTaskMetadataRow, ? extends UpgTaskMetadataNamedColumnValue<?>> rows) {
        Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<UpgTaskMetadataRow, ? extends UpgTaskMetadataNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteStart(UpgTaskMetadataRow row) {
        deleteStart(ImmutableSet.of(row));
    }

    public void deleteStart(Iterable<UpgTaskMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("s");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(UpgTaskMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<UpgTaskMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size());
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("s")));
        t.delete(tableName, cells);
    }

    @Override
    public Optional<UpgTaskMetadataRowResult> getRow(UpgTaskMetadataRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<UpgTaskMetadataRowResult> getRow(UpgTaskMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(UpgTaskMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<UpgTaskMetadataRowResult> getRows(Iterable<UpgTaskMetadataRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<UpgTaskMetadataRowResult> getRows(Iterable<UpgTaskMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<UpgTaskMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(UpgTaskMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<UpgTaskMetadataRowResult> getAsyncRows(Iterable<UpgTaskMetadataRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<UpgTaskMetadataRowResult> getAsyncRows(final Iterable<UpgTaskMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<UpgTaskMetadataRowResult>> c =
                new Callable<List<UpgTaskMetadataRowResult>>() {
            @Override
            public List<UpgTaskMetadataRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<UpgTaskMetadataNamedColumnValue<?>> getRowColumns(UpgTaskMetadataRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<UpgTaskMetadataNamedColumnValue<?>> getRowColumns(UpgTaskMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<UpgTaskMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<UpgTaskMetadataRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<UpgTaskMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<UpgTaskMetadataRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<UpgTaskMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>>> c =
                new Callable<Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>>>() {
            @Override
            public Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<UpgTaskMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<UpgTaskMetadataRow, UpgTaskMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            UpgTaskMetadataRow row = UpgTaskMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<UpgTaskMetadataRowResult> getRange(RangeRequest range) {
        if (range.getColumnNames().isEmpty()) {
            range = range.getBuilder().retainColumns(ColumnSelection.all()).build();
        }
        return BatchingVisitables.transform(t.getRange(tableName, range), new Function<RowResult<byte[]>, UpgTaskMetadataRowResult>() {
            @Override
            public UpgTaskMetadataRowResult apply(RowResult<byte[]> input) {
                return UpgTaskMetadataRowResult.of(input);
            }
        });
    }

    public IterableView<BatchingVisitable<UpgTaskMetadataRowResult>> getRanges(Iterable<RangeRequest> ranges) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableName, ranges);
        return IterableView.of(rangeResults).transform(
                new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<UpgTaskMetadataRowResult>>() {
            @Override
            public BatchingVisitable<UpgTaskMetadataRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, UpgTaskMetadataRowResult>() {
                    @Override
                    public UpgTaskMetadataRowResult apply(RowResult<byte[]> row) {
                        return UpgTaskMetadataRowResult.of(row);
                    }
                });
            }
        });
    }

    public void deleteRange(RangeRequest range) {
        deleteRanges(ImmutableSet.of(range));
    }

    public void deleteRanges(Iterable<RangeRequest> ranges) {
        BatchingVisitables.concat(getRanges(ranges))
                          .transform(UpgTaskMetadataRowResult.getRowNameFun())
                          .batchAccept(1000, new AbortingVisitor<List<UpgTaskMetadataRow>, RuntimeException>() {
            @Override
            public boolean visit(List<UpgTaskMetadataRow> rows) {
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
     * {@link Throwables}
     * {@link TimeUnit}
     * {@link Transaction}
     * {@link TypedRowResult}
     * {@link UnsignedBytes}
     * {@link ValueType}
     */
    static String __CLASS_HASH = "6PIvByP/YNUl4J9aj57Bzg==";
}
