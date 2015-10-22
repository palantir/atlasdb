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


public final class UpgradeMetadataTable implements
        AtlasDbMutablePersistentTable<UpgradeMetadataTable.UpgradeMetadataRow,
                                         UpgradeMetadataTable.UpgradeMetadataNamedColumnValue<?>,
                                         UpgradeMetadataTable.UpgradeMetadataRowResult>,
        AtlasDbNamedMutableTable<UpgradeMetadataTable.UpgradeMetadataRow,
                                    UpgradeMetadataTable.UpgradeMetadataNamedColumnValue<?>,
                                    UpgradeMetadataTable.UpgradeMetadataRowResult> {
    private final Transaction t;
    private final List<UpgradeMetadataTrigger> triggers;
    private final static String rawTableName = "upgrade_metadata";
    private final String tableName;
    private final Namespace namespace;

    static UpgradeMetadataTable of(Transaction t, Namespace namespace) {
        return new UpgradeMetadataTable(t, namespace, ImmutableList.<UpgradeMetadataTrigger>of());
    }

    static UpgradeMetadataTable of(Transaction t, Namespace namespace, UpgradeMetadataTrigger trigger, UpgradeMetadataTrigger... triggers) {
        return new UpgradeMetadataTable(t, namespace, ImmutableList.<UpgradeMetadataTrigger>builder().add(trigger).add(triggers).build());
    }

    static UpgradeMetadataTable of(Transaction t, Namespace namespace, List<UpgradeMetadataTrigger> triggers) {
        return new UpgradeMetadataTable(t, namespace, triggers);
    }

    private UpgradeMetadataTable(Transaction t, Namespace namespace, List<UpgradeMetadataTrigger> triggers) {
        this.t = t;
        this.tableName = namespace.getName() + "." + rawTableName;
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
     * UpgradeMetadataRow {
     *   {@literal String namespace};
     * }
     * </pre>
     */
    public static final class UpgradeMetadataRow implements Persistable, Comparable<UpgradeMetadataRow> {
        private final String namespace;

        public static UpgradeMetadataRow of(String namespace) {
            return new UpgradeMetadataRow(namespace);
        }

        private UpgradeMetadataRow(String namespace) {
            this.namespace = namespace;
        }

        public String getNamespace() {
            return namespace;
        }

        public static Function<UpgradeMetadataRow, String> getNamespaceFun() {
            return new Function<UpgradeMetadataRow, String>() {
                @Override
                public String apply(UpgradeMetadataRow row) {
                    return row.namespace;
                }
            };
        }

        public static Function<String, UpgradeMetadataRow> fromNamespaceFun() {
            return new Function<String, UpgradeMetadataRow>() {
                @Override
                public UpgradeMetadataRow apply(String row) {
                    return UpgradeMetadataRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] namespaceBytes = PtBytes.toBytes(namespace);
            return EncodingUtils.add(namespaceBytes);
        }

        public static final Hydrator<UpgradeMetadataRow> BYTES_HYDRATOR = new Hydrator<UpgradeMetadataRow>() {
            @Override
            public UpgradeMetadataRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                String namespace = PtBytes.toString(__input, __index, __input.length-__index);
                __index += 0;
                return UpgradeMetadataRow.of(namespace);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("namespace", namespace)
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
            UpgradeMetadataRow other = (UpgradeMetadataRow) obj;
            return Objects.equal(namespace, other.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(namespace);
        }

        @Override
        public int compareTo(UpgradeMetadataRow o) {
            return ComparisonChain.start()
                .compare(this.namespace, o.namespace)
                .result();
        }
    }

    public interface UpgradeMetadataNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion;
     *   name: "SchemaVersion"
     *   field {
     *     name: "version"
     *     number: 1
     *     label: LABEL_REQUIRED
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "hotfix"
     *     number: 2
     *     label: LABEL_OPTIONAL
     *     type: TYPE_INT64
     *   }
     *   field {
     *     name: "hotfix_hotfix"
     *     number: 3
     *     label: LABEL_OPTIONAL
     *     type: TYPE_INT64
     *   }
     * }
     * </pre>
     */
    public static final class CurrentVersion implements UpgradeMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> {
        private final com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value;

        public static CurrentVersion of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
            return new CurrentVersion(value);
        }

        private CurrentVersion(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "current_version";
        }

        @Override
        public String getShortColumnName() {
            return "c";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<CurrentVersion> BYTES_HYDRATOR = new Hydrator<CurrentVersion>() {
            @Override
            public CurrentVersion hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.parseFrom(bytes));
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

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions;
     *   name: "SchemaVersions"
     *   field {
     *     name: "versions"
     *     number: 1
     *     label: LABEL_REPEATED
     *     type: TYPE_MESSAGE
     *     type_name: ".com.palantir.atlasdb.protos.generated.SchemaVersion"
     *   }
     * }
     * </pre>
     */
    public static final class FinishedTasks implements UpgradeMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> {
        private final com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value;

        public static FinishedTasks of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
            return new FinishedTasks(value);
        }

        private FinishedTasks(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "finished_tasks";
        }

        @Override
        public String getShortColumnName() {
            return "f";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("f");
        }

        public static final Hydrator<FinishedTasks> BYTES_HYDRATOR = new Hydrator<FinishedTasks>() {
            @Override
            public FinishedTasks hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.parseFrom(bytes));
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

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions;
     *   name: "SchemaVersions"
     *   field {
     *     name: "versions"
     *     number: 1
     *     label: LABEL_REPEATED
     *     type: TYPE_MESSAGE
     *     type_name: ".com.palantir.atlasdb.protos.generated.SchemaVersion"
     *   }
     * }
     * </pre>
     */
    public static final class RunningTasks implements UpgradeMetadataNamedColumnValue<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> {
        private final com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value;

        public static RunningTasks of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
            return new RunningTasks(value);
        }

        private RunningTasks(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "running_tasks";
        }

        @Override
        public String getShortColumnName() {
            return "r";
        }

        @Override
        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("r");
        }

        public static final Hydrator<RunningTasks> BYTES_HYDRATOR = new Hydrator<RunningTasks>() {
            @Override
            public RunningTasks hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.parseFrom(bytes));
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

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Status implements UpgradeMetadataNamedColumnValue<Long> {
        private final Long value;

        public static Status of(Long value) {
            return new Status(value);
        }

        private Status(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "status";
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

        public static final Hydrator<Status> BYTES_HYDRATOR = new Hydrator<Status>() {
            @Override
            public Status hydrateFromBytes(byte[] bytes) {
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

    public interface UpgradeMetadataTrigger {
        public void putUpgradeMetadata(Multimap<UpgradeMetadataRow, ? extends UpgradeMetadataNamedColumnValue<?>> newRows);
    }

    public static final class UpgradeMetadataRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static UpgradeMetadataRowResult of(RowResult<byte[]> row) {
            return new UpgradeMetadataRowResult(row);
        }

        private UpgradeMetadataRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public UpgradeMetadataRow getRowName() {
            return UpgradeMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<UpgradeMetadataRowResult, UpgradeMetadataRow> getRowNameFun() {
            return new Function<UpgradeMetadataRowResult, UpgradeMetadataRow>() {
                @Override
                public UpgradeMetadataRow apply(UpgradeMetadataRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, UpgradeMetadataRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, UpgradeMetadataRowResult>() {
                @Override
                public UpgradeMetadataRowResult apply(RowResult<byte[]> rowResult) {
                    return new UpgradeMetadataRowResult(rowResult);
                }
            };
        }

        public boolean hasCurrentVersion() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public boolean hasFinishedTasks() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("f"));
        }

        public boolean hasRunningTasks() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("r"));
        }

        public boolean hasStatus() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("s"));
        }

        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getCurrentVersion() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            CurrentVersion value = CurrentVersion.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions getFinishedTasks() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("f"));
            if (bytes == null) {
                return null;
            }
            FinishedTasks value = FinishedTasks.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions getRunningTasks() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("r"));
            if (bytes == null) {
                return null;
            }
            RunningTasks value = RunningTasks.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getStatus() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("s"));
            if (bytes == null) {
                return null;
            }
            Status value = Status.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> getCurrentVersionFun() {
            return new Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion>() {
                @Override
                public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion apply(UpgradeMetadataRowResult rowResult) {
                    return rowResult.getCurrentVersion();
                }
            };
        }

        public static Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> getFinishedTasksFun() {
            return new Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions>() {
                @Override
                public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions apply(UpgradeMetadataRowResult rowResult) {
                    return rowResult.getFinishedTasks();
                }
            };
        }

        public static Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> getRunningTasksFun() {
            return new Function<UpgradeMetadataRowResult, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions>() {
                @Override
                public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions apply(UpgradeMetadataRowResult rowResult) {
                    return rowResult.getRunningTasks();
                }
            };
        }

        public static Function<UpgradeMetadataRowResult, Long> getStatusFun() {
            return new Function<UpgradeMetadataRowResult, Long>() {
                @Override
                public Long apply(UpgradeMetadataRowResult rowResult) {
                    return rowResult.getStatus();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("CurrentVersion", getCurrentVersion())
                .add("FinishedTasks", getFinishedTasks())
                .add("RunningTasks", getRunningTasks())
                .add("Status", getStatus())
                .toString();
        }
    }

    public enum UpgradeMetadataNamedColumn {
        CURRENT_VERSION {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        },
        FINISHED_TASKS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("f");
            }
        },
        RUNNING_TASKS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("r");
            }
        },
        STATUS {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("s");
            }
        };

        public abstract byte[] getShortName();

        public static Function<UpgradeMetadataNamedColumn, byte[]> toShortName() {
            return new Function<UpgradeMetadataNamedColumn, byte[]>() {
                @Override
                public byte[] apply(UpgradeMetadataNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<UpgradeMetadataNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, UpgradeMetadataNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(UpgradeMetadataNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends UpgradeMetadataNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends UpgradeMetadataNamedColumnValue<?>>>builder()
                .put("s", Status.BYTES_HYDRATOR)
                .put("c", CurrentVersion.BYTES_HYDRATOR)
                .put("r", RunningTasks.BYTES_HYDRATOR)
                .put("f", FinishedTasks.BYTES_HYDRATOR)
                .build();

    public Map<UpgradeMetadataRow, Long> getStatuss(Collection<UpgradeMetadataRow> rows) {
        Map<Cell, UpgradeMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UpgradeMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("s")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UpgradeMetadataRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Status.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> getCurrentVersions(Collection<UpgradeMetadataRow> rows) {
        Map<Cell, UpgradeMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UpgradeMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion val = CurrentVersion.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> getRunningTaskss(Collection<UpgradeMetadataRow> rows) {
        Map<Cell, UpgradeMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UpgradeMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("r")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions val = RunningTasks.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> getFinishedTaskss(Collection<UpgradeMetadataRow> rows) {
        Map<Cell, UpgradeMetadataRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UpgradeMetadataRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("f")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions val = FinishedTasks.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putStatus(UpgradeMetadataRow row, Long value) {
        put(ImmutableMultimap.of(row, Status.of(value)));
    }

    public void putStatus(Map<UpgradeMetadataRow, Long> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Status.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putStatusUnlessExists(UpgradeMetadataRow row, Long value) {
        putUnlessExists(ImmutableMultimap.of(row, Status.of(value)));
    }

    public void putStatusUnlessExists(Map<UpgradeMetadataRow, Long> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Status.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putCurrentVersion(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
        put(ImmutableMultimap.of(row, CurrentVersion.of(value)));
    }

    public void putCurrentVersion(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> e : map.entrySet()) {
            toPut.put(e.getKey(), CurrentVersion.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCurrentVersionUnlessExists(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
        putUnlessExists(ImmutableMultimap.of(row, CurrentVersion.of(value)));
    }

    public void putCurrentVersionUnlessExists(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> e : map.entrySet()) {
            toPut.put(e.getKey(), CurrentVersion.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putRunningTasks(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
        put(ImmutableMultimap.of(row, RunningTasks.of(value)));
    }

    public void putRunningTasks(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> e : map.entrySet()) {
            toPut.put(e.getKey(), RunningTasks.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putRunningTasksUnlessExists(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
        putUnlessExists(ImmutableMultimap.of(row, RunningTasks.of(value)));
    }

    public void putRunningTasksUnlessExists(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> e : map.entrySet()) {
            toPut.put(e.getKey(), RunningTasks.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    public void putFinishedTasks(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
        put(ImmutableMultimap.of(row, FinishedTasks.of(value)));
    }

    public void putFinishedTasks(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> e : map.entrySet()) {
            toPut.put(e.getKey(), FinishedTasks.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putFinishedTasksUnlessExists(UpgradeMetadataRow row, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions value) {
        putUnlessExists(ImmutableMultimap.of(row, FinishedTasks.of(value)));
    }

    public void putFinishedTasksUnlessExists(Map<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> map) {
        Map<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UpgradeMetadataRow, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions> e : map.entrySet()) {
            toPut.put(e.getKey(), FinishedTasks.of(e.getValue()));
        }
        putUnlessExists(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<UpgradeMetadataRow, ? extends UpgradeMetadataNamedColumnValue<?>> rows) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(rows));
        for (UpgradeMetadataTrigger trigger : triggers) {
            trigger.putUpgradeMetadata(rows);
        }
    }

    @Override
    public void putUnlessExists(Multimap<UpgradeMetadataRow, ? extends UpgradeMetadataNamedColumnValue<?>> rows) {
        Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> existing = getRowsMultimap(rows.keySet());
        Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> toPut = HashMultimap.create();
        for (Entry<UpgradeMetadataRow, ? extends UpgradeMetadataNamedColumnValue<?>> entry : rows.entries()) {
            if (!existing.containsEntry(entry.getKey(), entry.getValue())) {
                toPut.put(entry.getKey(), entry.getValue());
            }
        }
        put(toPut);
    }

    public void deleteStatus(UpgradeMetadataRow row) {
        deleteStatus(ImmutableSet.of(row));
    }

    public void deleteStatus(Iterable<UpgradeMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("s");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    public void deleteCurrentVersion(UpgradeMetadataRow row) {
        deleteCurrentVersion(ImmutableSet.of(row));
    }

    public void deleteCurrentVersion(Iterable<UpgradeMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    public void deleteRunningTasks(UpgradeMetadataRow row) {
        deleteRunningTasks(ImmutableSet.of(row));
    }

    public void deleteRunningTasks(Iterable<UpgradeMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("r");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    public void deleteFinishedTasks(UpgradeMetadataRow row) {
        deleteFinishedTasks(ImmutableSet.of(row));
    }

    public void deleteFinishedTasks(Iterable<UpgradeMetadataRow> rows) {
        byte[] col = PtBytes.toCachedBytes("f");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(UpgradeMetadataRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<UpgradeMetadataRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 4);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("f")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("r")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("s")));
        t.delete(tableName, cells);
    }

    @Override
    public Optional<UpgradeMetadataRowResult> getRow(UpgradeMetadataRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<UpgradeMetadataRowResult> getRow(UpgradeMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(UpgradeMetadataRowResult.of(rowResult));
        }
    }

    @Override
    public List<UpgradeMetadataRowResult> getRows(Iterable<UpgradeMetadataRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<UpgradeMetadataRowResult> getRows(Iterable<UpgradeMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<UpgradeMetadataRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(UpgradeMetadataRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<UpgradeMetadataRowResult> getAsyncRows(Iterable<UpgradeMetadataRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<UpgradeMetadataRowResult> getAsyncRows(final Iterable<UpgradeMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<UpgradeMetadataRowResult>> c =
                new Callable<List<UpgradeMetadataRowResult>>() {
            @Override
            public List<UpgradeMetadataRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<UpgradeMetadataNamedColumnValue<?>> getRowColumns(UpgradeMetadataRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<UpgradeMetadataNamedColumnValue<?>> getRowColumns(UpgradeMetadataRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<UpgradeMetadataNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<UpgradeMetadataRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getRowsMultimap(Iterable<UpgradeMetadataRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<UpgradeMetadataRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<UpgradeMetadataRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>>> c =
                new Callable<Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>>>() {
            @Override
            public Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getRowsMultimapInternal(Iterable<UpgradeMetadataRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<UpgradeMetadataRow, UpgradeMetadataNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            UpgradeMetadataRow row = UpgradeMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<UpgradeMetadataRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<UpgradeMetadataRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, UpgradeMetadataRowResult>() {
            @Override
            public UpgradeMetadataRowResult apply(RowResult<byte[]> input) {
                return UpgradeMetadataRowResult.of(input);
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

    static String __CLASS_HASH = "h03MSdcbTez7kajqIeeSaA==";
}
