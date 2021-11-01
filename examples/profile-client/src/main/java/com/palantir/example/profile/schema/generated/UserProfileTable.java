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
public final class UserProfileTable implements
        AtlasDbMutablePersistentTable<UserProfileTable.UserProfileRow,
                                         UserProfileTable.UserProfileNamedColumnValue<?>,
                                         UserProfileTable.UserProfileRowResult>,
        AtlasDbNamedMutableTable<UserProfileTable.UserProfileRow,
                                    UserProfileTable.UserProfileNamedColumnValue<?>,
                                    UserProfileTable.UserProfileRowResult> {
    private final Transaction t;
    private final List<UserProfileTrigger> triggers;
    private final static String rawTableName = "user_profile";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(UserProfileNamedColumn.values());

    static UserProfileTable of(Transaction t, Namespace namespace) {
        return new UserProfileTable(t, namespace, ImmutableList.<UserProfileTrigger>of());
    }

    static UserProfileTable of(Transaction t, Namespace namespace, UserProfileTrigger trigger, UserProfileTrigger... triggers) {
        return new UserProfileTable(t, namespace, ImmutableList.<UserProfileTrigger>builder().add(trigger).add(triggers).build());
    }

    static UserProfileTable of(Transaction t, Namespace namespace, List<UserProfileTrigger> triggers) {
        return new UserProfileTable(t, namespace, triggers);
    }

    private UserProfileTable(Transaction t, Namespace namespace, List<UserProfileTrigger> triggers) {
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
     * UserProfileRow {
     *   {@literal UUID id};
     * }
     * </pre>
     */
    public static final class UserProfileRow implements Persistable, Comparable<UserProfileRow> {
        private final UUID id;

        public static UserProfileRow of(UUID id) {
            return new UserProfileRow(id);
        }

        private UserProfileRow(UUID id) {
            this.id = id;
        }

        public UUID getId() {
            return id;
        }

        public static Function<UserProfileRow, UUID> getIdFun() {
            return new Function<UserProfileRow, UUID>() {
                @Override
                public UUID apply(UserProfileRow row) {
                    return row.id;
                }
            };
        }

        public static Function<UUID, UserProfileRow> fromIdFun() {
            return new Function<UUID, UserProfileRow>() {
                @Override
                public UserProfileRow apply(UUID row) {
                    return UserProfileRow.of(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUUID(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<UserProfileRow> BYTES_HYDRATOR = new Hydrator<UserProfileRow>() {
            @Override
            public UserProfileRow hydrateFromBytes(byte[] _input) {
                int _index = 0;
                UUID id = EncodingUtils.decodeUUID(_input, _index);
                _index += 16;
                return new UserProfileRow(id);
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
            UserProfileRow other = (UserProfileRow) obj;
            return Objects.equals(id, other.id);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(UserProfileRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    public interface UserProfileNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: com.palantir.example.profile.schema.CreationData;
     * }
     * </pre>
     */
    public static final class Create implements UserProfileNamedColumnValue<com.palantir.example.profile.schema.CreationData> {
        private final com.palantir.example.profile.schema.CreationData value;

        public static Create of(com.palantir.example.profile.schema.CreationData value) {
            return new Create(value);
        }

        private Create(com.palantir.example.profile.schema.CreationData value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "create";
        }

        @Override
        public String getShortColumnName() {
            return "c";
        }

        @Override
        public com.palantir.example.profile.schema.CreationData getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = com.palantir.atlasdb.compress.CompressionUtils.compress(new com.palantir.example.profile.schema.CreationData.Persister().persistToBytes(value), com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("c");
        }

        public static final Hydrator<Create> BYTES_HYDRATOR = new Hydrator<Create>() {
            @Override
            public Create hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(new com.palantir.example.profile.schema.CreationData.Persister().hydrateFromBytes(com.palantir.atlasdb.compress.CompressionUtils.decompress(bytes, com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE)));
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
     *   type: com.fasterxml.jackson.databind.JsonNode;
     * }
     * </pre>
     */
    public static final class Json implements UserProfileNamedColumnValue<com.fasterxml.jackson.databind.JsonNode> {
        private final com.fasterxml.jackson.databind.JsonNode value;

        public static Json of(com.fasterxml.jackson.databind.JsonNode value) {
            return new Json(value);
        }

        private Json(com.fasterxml.jackson.databind.JsonNode value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "json";
        }

        @Override
        public String getShortColumnName() {
            return "j";
        }

        @Override
        public com.fasterxml.jackson.databind.JsonNode getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = com.palantir.atlasdb.compress.CompressionUtils.compress(new com.palantir.atlasdb.persister.JsonNodePersister().persistToBytes(value), com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("j");
        }

        public static final Hydrator<Json> BYTES_HYDRATOR = new Hydrator<Json>() {
            @Override
            public Json hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(REUSABLE_PERSISTER.hydrateFromBytes(com.palantir.atlasdb.compress.CompressionUtils.decompress(bytes, com.palantir.atlasdb.table.description.ColumnValueDescription.Compression.NONE)));
            }
            private final com.palantir.atlasdb.persister.JsonNodePersister REUSABLE_PERSISTER = new com.palantir.atlasdb.persister.JsonNodePersister();
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
     *   type: com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
     *   name: "UserProfile"
     *   field {
     *     name: "name"
     *     number: 1
     *     label: LABEL_REQUIRED
     *     type: TYPE_STRING
     *   }
     *   field {
     *     name: "birthEpochDay"
     *     number: 2
     *     label: LABEL_REQUIRED
     *     type: TYPE_SINT64
     *   }
     * }
     * </pre>
     */
    public static final class Metadata implements UserProfileNamedColumnValue<com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> {
        private final com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile value;

        public static Metadata of(com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile value) {
            return new Metadata(value);
        }

        private Metadata(com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "metadata";
        }

        @Override
        public String getShortColumnName() {
            return "m";
        }

        @Override
        public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.toByteArray();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("m");
        }

        public static final Hydrator<Metadata> BYTES_HYDRATOR = new Hydrator<Metadata>() {
            @Override
            public Metadata hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                try {
                    return of(com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.parseFrom(bytes));
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
    public static final class PhotoStreamId implements UserProfileNamedColumnValue<Long> {
        private final Long value;

        public static PhotoStreamId of(Long value) {
            return new PhotoStreamId(value);
        }

        private PhotoStreamId(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "photo_stream_id";
        }

        @Override
        public String getShortColumnName() {
            return "p";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = PtBytes.toBytes(Long.MIN_VALUE ^ value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("p");
        }

        public static final Hydrator<PhotoStreamId> BYTES_HYDRATOR = new Hydrator<PhotoStreamId>() {
            @Override
            public PhotoStreamId hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(Long.MIN_VALUE ^ PtBytes.toLong(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface UserProfileTrigger {
        public void putUserProfile(Multimap<UserProfileRow, ? extends UserProfileNamedColumnValue<?>> newRows);
    }

    public static final class UserProfileRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static UserProfileRowResult of(RowResult<byte[]> row) {
            return new UserProfileRowResult(row);
        }

        private UserProfileRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public UserProfileRow getRowName() {
            return UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<UserProfileRowResult, UserProfileRow> getRowNameFun() {
            return new Function<UserProfileRowResult, UserProfileRow>() {
                @Override
                public UserProfileRow apply(UserProfileRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, UserProfileRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, UserProfileRowResult>() {
                @Override
                public UserProfileRowResult apply(RowResult<byte[]> rowResult) {
                    return new UserProfileRowResult(rowResult);
                }
            };
        }

        public boolean hasCreate() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c"));
        }

        public boolean hasJson() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("j"));
        }

        public boolean hasMetadata() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("m"));
        }

        public boolean hasPhotoStreamId() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("p"));
        }

        public com.palantir.example.profile.schema.CreationData getCreate() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c"));
            if (bytes == null) {
                return null;
            }
            Create value = Create.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public com.fasterxml.jackson.databind.JsonNode getJson() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("j"));
            if (bytes == null) {
                return null;
            }
            Json value = Json.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile getMetadata() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("m"));
            if (bytes == null) {
                return null;
            }
            Metadata value = Metadata.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getPhotoStreamId() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("p"));
            if (bytes == null) {
                return null;
            }
            PhotoStreamId value = PhotoStreamId.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<UserProfileRowResult, com.palantir.example.profile.schema.CreationData> getCreateFun() {
            return new Function<UserProfileRowResult, com.palantir.example.profile.schema.CreationData>() {
                @Override
                public com.palantir.example.profile.schema.CreationData apply(UserProfileRowResult rowResult) {
                    return rowResult.getCreate();
                }
            };
        }

        public static Function<UserProfileRowResult, com.fasterxml.jackson.databind.JsonNode> getJsonFun() {
            return new Function<UserProfileRowResult, com.fasterxml.jackson.databind.JsonNode>() {
                @Override
                public com.fasterxml.jackson.databind.JsonNode apply(UserProfileRowResult rowResult) {
                    return rowResult.getJson();
                }
            };
        }

        public static Function<UserProfileRowResult, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> getMetadataFun() {
            return new Function<UserProfileRowResult, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile>() {
                @Override
                public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile apply(UserProfileRowResult rowResult) {
                    return rowResult.getMetadata();
                }
            };
        }

        public static Function<UserProfileRowResult, Long> getPhotoStreamIdFun() {
            return new Function<UserProfileRowResult, Long>() {
                @Override
                public Long apply(UserProfileRowResult rowResult) {
                    return rowResult.getPhotoStreamId();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Create", getCreate())
                .add("Json", getJson())
                .add("Metadata", getMetadata())
                .add("PhotoStreamId", getPhotoStreamId())
                .toString();
        }
    }

    public enum UserProfileNamedColumn {
        CREATE {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c");
            }
        },
        JSON {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("j");
            }
        },
        METADATA {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("m");
            }
        },
        PHOTO_STREAM_ID {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("p");
            }
        };

        public abstract byte[] getShortName();

        public static Function<UserProfileNamedColumn, byte[]> toShortName() {
            return new Function<UserProfileNamedColumn, byte[]>() {
                @Override
                public byte[] apply(UserProfileNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<UserProfileNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, UserProfileNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(UserProfileNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends UserProfileNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends UserProfileNamedColumnValue<?>>>builder()
                .put("m", Metadata.BYTES_HYDRATOR)
                .put("c", Create.BYTES_HYDRATOR)
                .put("j", Json.BYTES_HYDRATOR)
                .put("p", PhotoStreamId.BYTES_HYDRATOR)
                .build();

    public Map<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> getMetadatas(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("m")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UserProfileRow, com.palantir.example.profile.schema.CreationData> getCreates(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<UserProfileRow, com.palantir.example.profile.schema.CreationData> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.example.profile.schema.CreationData val = Create.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UserProfileRow, com.fasterxml.jackson.databind.JsonNode> getJsons(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("j")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<UserProfileRow, com.fasterxml.jackson.databind.JsonNode> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.fasterxml.jackson.databind.JsonNode val = Json.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UserProfileRow, Long> getPhotoStreamIds(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("p")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<UserProfileRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = PhotoStreamId.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putMetadata(UserProfileRow row, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile value) {
        put(ImmutableMultimap.of(row, Metadata.of(value)));
    }

    public void putMetadata(Map<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> map) {
        Map<UserProfileRow, UserProfileNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> e : map.entrySet()) {
            toPut.put(e.getKey(), Metadata.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putCreate(UserProfileRow row, com.palantir.example.profile.schema.CreationData value) {
        put(ImmutableMultimap.of(row, Create.of(value)));
    }

    public void putCreate(Map<UserProfileRow, com.palantir.example.profile.schema.CreationData> map) {
        Map<UserProfileRow, UserProfileNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserProfileRow, com.palantir.example.profile.schema.CreationData> e : map.entrySet()) {
            toPut.put(e.getKey(), Create.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putJson(UserProfileRow row, com.fasterxml.jackson.databind.JsonNode value) {
        put(ImmutableMultimap.of(row, Json.of(value)));
    }

    public void putJson(Map<UserProfileRow, com.fasterxml.jackson.databind.JsonNode> map) {
        Map<UserProfileRow, UserProfileNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserProfileRow, com.fasterxml.jackson.databind.JsonNode> e : map.entrySet()) {
            toPut.put(e.getKey(), Json.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putPhotoStreamId(UserProfileRow row, Long value) {
        put(ImmutableMultimap.of(row, PhotoStreamId.of(value)));
    }

    public void putPhotoStreamId(Map<UserProfileRow, Long> map) {
        Map<UserProfileRow, UserProfileNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<UserProfileRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), PhotoStreamId.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<UserProfileRow, ? extends UserProfileNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> affectedCells = getAffectedCells(rows);
        deleteCookiesIdx(affectedCells);
        deleteCreatedIdx(affectedCells);
        deleteUserBirthdaysIdx(affectedCells);
        for (Entry<UserProfileRow, ? extends UserProfileNamedColumnValue<?>> e : rows.entries()) {
            if (e.getValue() instanceof Json)
            {
                Json col = (Json) e.getValue();
                {
                    UserProfileRow row = e.getKey();
                    CookiesIdxTable table = CookiesIdxTable.of(this);
                    Iterable<String> cookieIterable = com.palantir.example.profile.schema.ProfileSchema.getCookies(col.getValue());
                    UUID id = row.getId();
                    for (String cookie : cookieIterable) {
                        CookiesIdxTable.CookiesIdxRow indexRow = CookiesIdxTable.CookiesIdxRow.of(cookie);
                        CookiesIdxTable.CookiesIdxColumn indexCol = CookiesIdxTable.CookiesIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                        CookiesIdxTable.CookiesIdxColumnValue indexColVal = CookiesIdxTable.CookiesIdxColumnValue.of(indexCol, 0L);
                        table.put(indexRow, indexColVal);
                    }
                }
            }
            if (e.getValue() instanceof Create)
            {
                Create col = (Create) e.getValue();
                {
                    UserProfileRow row = e.getKey();
                    CreatedIdxTable table = CreatedIdxTable.of(this);
                    long time = col.getValue().getTimeCreated();
                    UUID id = row.getId();
                    CreatedIdxTable.CreatedIdxRow indexRow = CreatedIdxTable.CreatedIdxRow.of(time);
                    CreatedIdxTable.CreatedIdxColumn indexCol = CreatedIdxTable.CreatedIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    CreatedIdxTable.CreatedIdxColumnValue indexColVal = CreatedIdxTable.CreatedIdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
            if (e.getValue() instanceof Metadata)
            {
                Metadata col = (Metadata) e.getValue();
                {
                    UserProfileRow row = e.getKey();
                    UserBirthdaysIdxTable table = UserBirthdaysIdxTable.of(this);
                    long birthday = col.getValue().getBirthEpochDay();
                    UUID id = row.getId();
                    UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue indexColVal = UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
        }
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (UserProfileTrigger trigger : triggers) {
            trigger.putUserProfile(rows);
        }
    }

    public void deleteMetadata(UserProfileRow row) {
        deleteMetadata(ImmutableSet.of(row));
    }

    public void deleteMetadata(Iterable<UserProfileRow> rows) {
        byte[] col = PtBytes.toCachedBytes("m");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        Map<Cell, byte[]> results = t.get(tableRef, cells);
        deleteUserBirthdaysIdxRaw(results);
        t.delete(tableRef, cells);
    }

    private void deleteUserBirthdaysIdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Metadata col = (Metadata) shortNameToHydrator.get("m").hydrateFromBytes(result.getValue());
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long birthday = col.getValue().getBirthEpochDay();
            UUID id = row.getId();
            UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
            UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.user_birthdays_idx"), indexCells);
    }

    public void deleteCreate(UserProfileRow row) {
        deleteCreate(ImmutableSet.of(row));
    }

    public void deleteCreate(Iterable<UserProfileRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        Map<Cell, byte[]> results = t.get(tableRef, cells);
        deleteCreatedIdxRaw(results);
        t.delete(tableRef, cells);
    }

    private void deleteCreatedIdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Create col = (Create) shortNameToHydrator.get("c").hydrateFromBytes(result.getValue());
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long time = col.getValue().getTimeCreated();
            UUID id = row.getId();
            CreatedIdxTable.CreatedIdxRow indexRow = CreatedIdxTable.CreatedIdxRow.of(time);
            CreatedIdxTable.CreatedIdxColumn indexCol = CreatedIdxTable.CreatedIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.created_idx"), indexCells);
    }

    public void deleteJson(UserProfileRow row) {
        deleteJson(ImmutableSet.of(row));
    }

    public void deleteJson(Iterable<UserProfileRow> rows) {
        byte[] col = PtBytes.toCachedBytes("j");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        Map<Cell, byte[]> results = t.get(tableRef, cells);
        deleteCookiesIdxRaw(results);
        t.delete(tableRef, cells);
    }

    private void deleteCookiesIdxRaw(Map<Cell, byte[]> results) {
        Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Json col = (Json) shortNameToHydrator.get("j").hydrateFromBytes(result.getValue());
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            Iterable<String> cookieIterable = com.palantir.example.profile.schema.ProfileSchema.getCookies(col.getValue());
            UUID id = row.getId();
            for (String cookie : cookieIterable) {
                CookiesIdxTable.CookiesIdxRow indexRow = CookiesIdxTable.CookiesIdxRow.of(cookie);
                CookiesIdxTable.CookiesIdxColumn indexCol = CookiesIdxTable.CookiesIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
                indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.cookies_idx"), indexCells);
    }

    public void deletePhotoStreamId(UserProfileRow row) {
        deletePhotoStreamId(ImmutableSet.of(row));
    }

    public void deletePhotoStreamId(Iterable<UserProfileRow> rows) {
        byte[] col = PtBytes.toCachedBytes("p");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(UserProfileRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<UserProfileRow> rows) {
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result = getRowsMultimap(rows);
        deleteCookiesIdx(result);
        deleteCreatedIdx(result);
        deleteUserBirthdaysIdx(result);
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 4);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("j")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("m")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("p")));
        t.delete(tableRef, cells);
    }

    public Optional<UserProfileRowResult> getRow(UserProfileRow row) {
        return getRow(row, allColumns);
    }

    public Optional<UserProfileRowResult> getRow(UserProfileRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(UserProfileRowResult.of(rowResult));
        }
    }

    @Override
    public List<UserProfileRowResult> getRows(Iterable<UserProfileRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<UserProfileRowResult> getRows(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<UserProfileRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(UserProfileRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<UserProfileNamedColumnValue<?>> getRowColumns(UserProfileRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<UserProfileNamedColumnValue<?>> getRowColumns(UserProfileRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<UserProfileNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowsMultimap(Iterable<UserProfileRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowsMultimap(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowsMultimapInternal(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<UserProfileRow, BatchingVisitable<UserProfileNamedColumnValue<?>>> getRowsColumnRange(Iterable<UserProfileRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<UserProfileRow, BatchingVisitable<UserProfileNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<UserProfileNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<UserProfileRow, UserProfileNamedColumnValue<?>>> getRowsColumnRange(Iterable<UserProfileRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            UserProfileNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<UserProfileRow, Iterator<UserProfileNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<UserProfileRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<UserProfileRow, Iterator<UserProfileNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<UserProfileNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    private Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getAffectedCells(Multimap<UserProfileRow, ? extends UserProfileNamedColumnValue<?>> rows) {
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> oldData = getRowsMultimap(rows.keySet());
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> cellsAffected = ArrayListMultimap.create();
        for (UserProfileRow row : oldData.keySet()) {
            Set<String> columns = new HashSet<String>();
            for (UserProfileNamedColumnValue<?> v : rows.get(row)) {
                columns.add(v.getColumnName());
            }
            for (UserProfileNamedColumnValue<?> v : oldData.get(row)) {
                if (columns.contains(v.getColumnName())) {
                    cellsAffected.put(row, v);
                }
            }
        }
        return cellsAffected;
    }

    private void deleteCookiesIdx(Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<UserProfileRow, UserProfileNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Json) {
                Json col = (Json) e.getValue();{
                    UserProfileRow row = e.getKey();
                    Iterable<String> cookieIterable = com.palantir.example.profile.schema.ProfileSchema.getCookies(col.getValue());
                    UUID id = row.getId();
                    for (String cookie : cookieIterable) {
                        CookiesIdxTable.CookiesIdxRow indexRow = CookiesIdxTable.CookiesIdxRow.of(cookie);
                        CookiesIdxTable.CookiesIdxColumn indexCol = CookiesIdxTable.CookiesIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                        indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                    }
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.cookies_idx"), indexCells.build());
    }

    private void deleteCreatedIdx(Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<UserProfileRow, UserProfileNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Create) {
                Create col = (Create) e.getValue();{
                    UserProfileRow row = e.getKey();
                    long time = col.getValue().getTimeCreated();
                    UUID id = row.getId();
                    CreatedIdxTable.CreatedIdxRow indexRow = CreatedIdxTable.CreatedIdxRow.of(time);
                    CreatedIdxTable.CreatedIdxColumn indexCol = CreatedIdxTable.CreatedIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.created_idx"), indexCells.build());
    }

    private void deleteUserBirthdaysIdx(Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<UserProfileRow, UserProfileNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Metadata) {
                Metadata col = (Metadata) e.getValue();{
                    UserProfileRow row = e.getKey();
                    long birthday = col.getValue().getBirthEpochDay();
                    UUID id = row.getId();
                    UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete(TableReference.createFromFullyQualifiedName("default.user_birthdays_idx"), indexCells.build());
    }

    private ColumnSelection optimizeColumnSelection(ColumnSelection columns) {
        if (columns.allColumnsSelected()) {
            return allColumns;
        }
        return columns;
    }

    public BatchingVisitableView<UserProfileRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<UserProfileRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, UserProfileRowResult>() {
            @Override
            public UserProfileRowResult apply(RowResult<byte[]> input) {
                return UserProfileRowResult.of(input);
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

    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class CookiesIdxTable implements
            AtlasDbDynamicMutablePersistentTable<CookiesIdxTable.CookiesIdxRow,
                                                    CookiesIdxTable.CookiesIdxColumn,
                                                    CookiesIdxTable.CookiesIdxColumnValue,
                                                    CookiesIdxTable.CookiesIdxRowResult> {
        private final Transaction t;
        private final List<CookiesIdxTrigger> triggers;
        private final static String rawTableName = "cookies_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static CookiesIdxTable of(UserProfileTable table) {
            return new CookiesIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<CookiesIdxTrigger>of());
        }

        public static CookiesIdxTable of(UserProfileTable table, CookiesIdxTrigger trigger, CookiesIdxTrigger... triggers) {
            return new CookiesIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<CookiesIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static CookiesIdxTable of(UserProfileTable table, List<CookiesIdxTrigger> triggers) {
            return new CookiesIdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private CookiesIdxTable(Transaction t, Namespace namespace, List<CookiesIdxTrigger> triggers) {
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
         * CookiesIdxRow {
         *   {@literal String cookie};
         * }
         * </pre>
         */
        public static final class CookiesIdxRow implements Persistable, Comparable<CookiesIdxRow> {
            private final String cookie;

            public static CookiesIdxRow of(String cookie) {
                return new CookiesIdxRow(cookie);
            }

            private CookiesIdxRow(String cookie) {
                this.cookie = cookie;
            }

            public String getCookie() {
                return cookie;
            }

            public static Function<CookiesIdxRow, String> getCookieFun() {
                return new Function<CookiesIdxRow, String>() {
                    @Override
                    public String apply(CookiesIdxRow row) {
                        return row.cookie;
                    }
                };
            }

            public static Function<String, CookiesIdxRow> fromCookieFun() {
                return new Function<String, CookiesIdxRow>() {
                    @Override
                    public CookiesIdxRow apply(String row) {
                        return CookiesIdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] cookieBytes = PtBytes.toBytes(cookie);
                return EncodingUtils.add(cookieBytes);
            }

            public static final Hydrator<CookiesIdxRow> BYTES_HYDRATOR = new Hydrator<CookiesIdxRow>() {
                @Override
                public CookiesIdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    String cookie = PtBytes.toString(_input, _index, _input.length-_index);
                    _index += 0;
                    return new CookiesIdxRow(cookie);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("cookie", cookie)
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
                CookiesIdxRow other = (CookiesIdxRow) obj;
                return Objects.equals(cookie, other.cookie);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(cookie);
            }

            @Override
            public int compareTo(CookiesIdxRow o) {
                return ComparisonChain.start()
                    .compare(this.cookie, o.cookie)
                    .result();
            }
        }

        /**
         * <pre>
         * CookiesIdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * </pre>
         */
        public static final class CookiesIdxColumn implements Persistable, Comparable<CookiesIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final UUID id;

            public static CookiesIdxColumn of(byte[] rowName, byte[] columnName, UUID id) {
                return new CookiesIdxColumn(rowName, columnName, id);
            }

            private CookiesIdxColumn(byte[] rowName, byte[] columnName, UUID id) {
                this.rowName = rowName;
                this.columnName = columnName;
                this.id = id;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public UUID getId() {
                return id;
            }

            public static Function<CookiesIdxColumn, byte[]> getRowNameFun() {
                return new Function<CookiesIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(CookiesIdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<CookiesIdxColumn, byte[]> getColumnNameFun() {
                return new Function<CookiesIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(CookiesIdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<CookiesIdxColumn, UUID> getIdFun() {
                return new Function<CookiesIdxColumn, UUID>() {
                    @Override
                    public UUID apply(CookiesIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = EncodingUtils.encodeUUID(id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<CookiesIdxColumn> BYTES_HYDRATOR = new Hydrator<CookiesIdxColumn>() {
                @Override
                public CookiesIdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    UUID id = EncodingUtils.decodeUUID(_input, _index);
                    _index += 16;
                    return new CookiesIdxColumn(rowName, columnName, id);
                }
            };

            public static BatchColumnRangeSelection createPrefixRangeUnsorted(byte[] rowName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes), batchSize);
            }

            public static Prefix prefixUnsorted(byte[] rowName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return new Prefix(EncodingUtils.add(rowNameBytes));
            }

            public static BatchColumnRangeSelection createPrefixRange(byte[] rowName, byte[] columnName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes, columnNameBytes), batchSize);
            }

            public static Prefix prefix(byte[] rowName, byte[] columnName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return new Prefix(EncodingUtils.add(rowNameBytes, columnNameBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                CookiesIdxColumn other = (CookiesIdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(CookiesIdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface CookiesIdxTrigger {
            public void putCookiesIdx(Multimap<CookiesIdxRow, ? extends CookiesIdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class CookiesIdxColumnValue implements ColumnValue<Long> {
            private final CookiesIdxColumn columnName;
            private final Long value;

            public static CookiesIdxColumnValue of(CookiesIdxColumn columnName, Long value) {
                return new CookiesIdxColumnValue(columnName, value);
            }

            private CookiesIdxColumnValue(CookiesIdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public CookiesIdxColumn getColumnName() {
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

            public static Function<CookiesIdxColumnValue, CookiesIdxColumn> getColumnNameFun() {
                return new Function<CookiesIdxColumnValue, CookiesIdxColumn>() {
                    @Override
                    public CookiesIdxColumn apply(CookiesIdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<CookiesIdxColumnValue, Long> getValueFun() {
                return new Function<CookiesIdxColumnValue, Long>() {
                    @Override
                    public Long apply(CookiesIdxColumnValue columnValue) {
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

        public static final class CookiesIdxRowResult implements TypedRowResult {
            private final CookiesIdxRow rowName;
            private final ImmutableSet<CookiesIdxColumnValue> columnValues;

            public static CookiesIdxRowResult of(RowResult<byte[]> rowResult) {
                CookiesIdxRow rowName = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<CookiesIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = CookiesIdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(CookiesIdxColumnValue.of(col, value));
                }
                return new CookiesIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private CookiesIdxRowResult(CookiesIdxRow rowName, ImmutableSet<CookiesIdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public CookiesIdxRow getRowName() {
                return rowName;
            }

            public Set<CookiesIdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<CookiesIdxRowResult, CookiesIdxRow> getRowNameFun() {
                return new Function<CookiesIdxRowResult, CookiesIdxRow>() {
                    @Override
                    public CookiesIdxRow apply(CookiesIdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<CookiesIdxRowResult, ImmutableSet<CookiesIdxColumnValue>> getColumnValuesFun() {
                return new Function<CookiesIdxRowResult, ImmutableSet<CookiesIdxColumnValue>>() {
                    @Override
                    public ImmutableSet<CookiesIdxColumnValue> apply(CookiesIdxRowResult rowResult) {
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
        public void delete(CookiesIdxRow row, CookiesIdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<CookiesIdxRow> rows) {
            Multimap<CookiesIdxRow, CookiesIdxColumn> toRemove = HashMultimap.create();
            Multimap<CookiesIdxRow, CookiesIdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<CookiesIdxRow, CookiesIdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<CookiesIdxRow, CookiesIdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(CookiesIdxRow rowName, Iterable<CookiesIdxColumnValue> values) {
            put(ImmutableMultimap.<CookiesIdxRow, CookiesIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(CookiesIdxRow rowName, CookiesIdxColumnValue... values) {
            put(ImmutableMultimap.<CookiesIdxRow, CookiesIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<CookiesIdxRow, ? extends CookiesIdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (CookiesIdxTrigger trigger : triggers) {
                trigger.putCookiesIdx(values);
            }
        }

        @Override
        public void touch(Multimap<CookiesIdxRow, CookiesIdxColumn> values) {
            Multimap<CookiesIdxRow, CookiesIdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<CookiesIdxRow, CookiesIdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<CookiesIdxRow, CookiesIdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<CookiesIdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(CookiesIdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<CookiesIdxRow, CookiesIdxColumnValue> get(Multimap<CookiesIdxRow, CookiesIdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<CookiesIdxRow, CookiesIdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    CookiesIdxRow row = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = CookiesIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, CookiesIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<CookiesIdxColumnValue> getRowColumns(CookiesIdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<CookiesIdxColumnValue> getRowColumns(CookiesIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<CookiesIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = CookiesIdxColumnValue.hydrateValue(e.getValue());
                    ret.add(CookiesIdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<CookiesIdxRow, CookiesIdxColumnValue> getRowsMultimap(Iterable<CookiesIdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<CookiesIdxRow, CookiesIdxColumnValue> getRowsMultimap(Iterable<CookiesIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<CookiesIdxRow, CookiesIdxColumnValue> getRowsMultimapInternal(Iterable<CookiesIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<CookiesIdxRow, CookiesIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<CookiesIdxRow, CookiesIdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                CookiesIdxRow row = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = CookiesIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, CookiesIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<CookiesIdxRow, BatchingVisitable<CookiesIdxColumnValue>> getRowsColumnRange(Iterable<CookiesIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<CookiesIdxRow, BatchingVisitable<CookiesIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                CookiesIdxRow row = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<CookiesIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = CookiesIdxColumnValue.hydrateValue(result.getValue());
                    return CookiesIdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<CookiesIdxRow, CookiesIdxColumnValue>> getRowsColumnRange(Iterable<CookiesIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                CookiesIdxRow row = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = CookiesIdxColumnValue.hydrateValue(e.getValue());
                CookiesIdxColumnValue colValue = CookiesIdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<CookiesIdxRow, Iterator<CookiesIdxColumnValue>> getRowsColumnRangeIterator(Iterable<CookiesIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<CookiesIdxRow, Iterator<CookiesIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                CookiesIdxRow row = CookiesIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<CookiesIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    CookiesIdxColumn col = CookiesIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = CookiesIdxColumnValue.hydrateValue(result.getValue());
                    return CookiesIdxColumnValue.of(col, val);
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

        public BatchingVisitableView<CookiesIdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, CookiesIdxRowResult>() {
                @Override
                public CookiesIdxRowResult apply(RowResult<byte[]> input) {
                    return CookiesIdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<CookiesIdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<CookiesIdxRowResult>>() {
                @Override
                public BatchingVisitable<CookiesIdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, CookiesIdxRowResult>() {
                        @Override
                        public CookiesIdxRowResult apply(RowResult<byte[]> row) {
                            return CookiesIdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<CookiesIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, CookiesIdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<CookiesIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, CookiesIdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<CookiesIdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, CookiesIdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<CookiesIdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<CookiesIdxRowResult> rowResults) {
                    Multimap<CookiesIdxRow, CookiesIdxColumn> toRemove = HashMultimap.create();
                    for (CookiesIdxRowResult rowResult : rowResults) {
                        for (CookiesIdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class CreatedIdxTable implements
            AtlasDbDynamicMutablePersistentTable<CreatedIdxTable.CreatedIdxRow,
                                                    CreatedIdxTable.CreatedIdxColumn,
                                                    CreatedIdxTable.CreatedIdxColumnValue,
                                                    CreatedIdxTable.CreatedIdxRowResult> {
        private final Transaction t;
        private final List<CreatedIdxTrigger> triggers;
        private final static String rawTableName = "created_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static CreatedIdxTable of(UserProfileTable table) {
            return new CreatedIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<CreatedIdxTrigger>of());
        }

        public static CreatedIdxTable of(UserProfileTable table, CreatedIdxTrigger trigger, CreatedIdxTrigger... triggers) {
            return new CreatedIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<CreatedIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static CreatedIdxTable of(UserProfileTable table, List<CreatedIdxTrigger> triggers) {
            return new CreatedIdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private CreatedIdxTable(Transaction t, Namespace namespace, List<CreatedIdxTrigger> triggers) {
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
         * CreatedIdxRow {
         *   {@literal Long time};
         * }
         * </pre>
         */
        public static final class CreatedIdxRow implements Persistable, Comparable<CreatedIdxRow> {
            private final long time;

            public static CreatedIdxRow of(long time) {
                return new CreatedIdxRow(time);
            }

            private CreatedIdxRow(long time) {
                this.time = time;
            }

            public long getTime() {
                return time;
            }

            public static Function<CreatedIdxRow, Long> getTimeFun() {
                return new Function<CreatedIdxRow, Long>() {
                    @Override
                    public Long apply(CreatedIdxRow row) {
                        return row.time;
                    }
                };
            }

            public static Function<Long, CreatedIdxRow> fromTimeFun() {
                return new Function<Long, CreatedIdxRow>() {
                    @Override
                    public CreatedIdxRow apply(Long row) {
                        return CreatedIdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] timeBytes = EncodingUtils.encodeUnsignedVarLong(time);
                return EncodingUtils.add(timeBytes);
            }

            public static final Hydrator<CreatedIdxRow> BYTES_HYDRATOR = new Hydrator<CreatedIdxRow>() {
                @Override
                public CreatedIdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long time = EncodingUtils.decodeUnsignedVarLong(_input, _index);
                    _index += EncodingUtils.sizeOfUnsignedVarLong(time);
                    return new CreatedIdxRow(time);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("time", time)
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
                CreatedIdxRow other = (CreatedIdxRow) obj;
                return Objects.equals(time, other.time);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(time);
            }

            @Override
            public int compareTo(CreatedIdxRow o) {
                return ComparisonChain.start()
                    .compare(this.time, o.time)
                    .result();
            }
        }

        /**
         * <pre>
         * CreatedIdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * </pre>
         */
        public static final class CreatedIdxColumn implements Persistable, Comparable<CreatedIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final UUID id;

            public static CreatedIdxColumn of(byte[] rowName, byte[] columnName, UUID id) {
                return new CreatedIdxColumn(rowName, columnName, id);
            }

            private CreatedIdxColumn(byte[] rowName, byte[] columnName, UUID id) {
                this.rowName = rowName;
                this.columnName = columnName;
                this.id = id;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public UUID getId() {
                return id;
            }

            public static Function<CreatedIdxColumn, byte[]> getRowNameFun() {
                return new Function<CreatedIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(CreatedIdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<CreatedIdxColumn, byte[]> getColumnNameFun() {
                return new Function<CreatedIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(CreatedIdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<CreatedIdxColumn, UUID> getIdFun() {
                return new Function<CreatedIdxColumn, UUID>() {
                    @Override
                    public UUID apply(CreatedIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = EncodingUtils.encodeUUID(id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<CreatedIdxColumn> BYTES_HYDRATOR = new Hydrator<CreatedIdxColumn>() {
                @Override
                public CreatedIdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    UUID id = EncodingUtils.decodeUUID(_input, _index);
                    _index += 16;
                    return new CreatedIdxColumn(rowName, columnName, id);
                }
            };

            public static BatchColumnRangeSelection createPrefixRangeUnsorted(byte[] rowName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes), batchSize);
            }

            public static Prefix prefixUnsorted(byte[] rowName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return new Prefix(EncodingUtils.add(rowNameBytes));
            }

            public static BatchColumnRangeSelection createPrefixRange(byte[] rowName, byte[] columnName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes, columnNameBytes), batchSize);
            }

            public static Prefix prefix(byte[] rowName, byte[] columnName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return new Prefix(EncodingUtils.add(rowNameBytes, columnNameBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                CreatedIdxColumn other = (CreatedIdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(CreatedIdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface CreatedIdxTrigger {
            public void putCreatedIdx(Multimap<CreatedIdxRow, ? extends CreatedIdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class CreatedIdxColumnValue implements ColumnValue<Long> {
            private final CreatedIdxColumn columnName;
            private final Long value;

            public static CreatedIdxColumnValue of(CreatedIdxColumn columnName, Long value) {
                return new CreatedIdxColumnValue(columnName, value);
            }

            private CreatedIdxColumnValue(CreatedIdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public CreatedIdxColumn getColumnName() {
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

            public static Function<CreatedIdxColumnValue, CreatedIdxColumn> getColumnNameFun() {
                return new Function<CreatedIdxColumnValue, CreatedIdxColumn>() {
                    @Override
                    public CreatedIdxColumn apply(CreatedIdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<CreatedIdxColumnValue, Long> getValueFun() {
                return new Function<CreatedIdxColumnValue, Long>() {
                    @Override
                    public Long apply(CreatedIdxColumnValue columnValue) {
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

        public static final class CreatedIdxRowResult implements TypedRowResult {
            private final CreatedIdxRow rowName;
            private final ImmutableSet<CreatedIdxColumnValue> columnValues;

            public static CreatedIdxRowResult of(RowResult<byte[]> rowResult) {
                CreatedIdxRow rowName = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<CreatedIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = CreatedIdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(CreatedIdxColumnValue.of(col, value));
                }
                return new CreatedIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private CreatedIdxRowResult(CreatedIdxRow rowName, ImmutableSet<CreatedIdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public CreatedIdxRow getRowName() {
                return rowName;
            }

            public Set<CreatedIdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<CreatedIdxRowResult, CreatedIdxRow> getRowNameFun() {
                return new Function<CreatedIdxRowResult, CreatedIdxRow>() {
                    @Override
                    public CreatedIdxRow apply(CreatedIdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<CreatedIdxRowResult, ImmutableSet<CreatedIdxColumnValue>> getColumnValuesFun() {
                return new Function<CreatedIdxRowResult, ImmutableSet<CreatedIdxColumnValue>>() {
                    @Override
                    public ImmutableSet<CreatedIdxColumnValue> apply(CreatedIdxRowResult rowResult) {
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
        public void delete(CreatedIdxRow row, CreatedIdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<CreatedIdxRow> rows) {
            Multimap<CreatedIdxRow, CreatedIdxColumn> toRemove = HashMultimap.create();
            Multimap<CreatedIdxRow, CreatedIdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<CreatedIdxRow, CreatedIdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<CreatedIdxRow, CreatedIdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(CreatedIdxRow rowName, Iterable<CreatedIdxColumnValue> values) {
            put(ImmutableMultimap.<CreatedIdxRow, CreatedIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(CreatedIdxRow rowName, CreatedIdxColumnValue... values) {
            put(ImmutableMultimap.<CreatedIdxRow, CreatedIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<CreatedIdxRow, ? extends CreatedIdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (CreatedIdxTrigger trigger : triggers) {
                trigger.putCreatedIdx(values);
            }
        }

        @Override
        public void touch(Multimap<CreatedIdxRow, CreatedIdxColumn> values) {
            Multimap<CreatedIdxRow, CreatedIdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<CreatedIdxRow, CreatedIdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<CreatedIdxRow, CreatedIdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<CreatedIdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(CreatedIdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<CreatedIdxRow, CreatedIdxColumnValue> get(Multimap<CreatedIdxRow, CreatedIdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<CreatedIdxRow, CreatedIdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    CreatedIdxRow row = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = CreatedIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, CreatedIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<CreatedIdxColumnValue> getRowColumns(CreatedIdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<CreatedIdxColumnValue> getRowColumns(CreatedIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<CreatedIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = CreatedIdxColumnValue.hydrateValue(e.getValue());
                    ret.add(CreatedIdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<CreatedIdxRow, CreatedIdxColumnValue> getRowsMultimap(Iterable<CreatedIdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<CreatedIdxRow, CreatedIdxColumnValue> getRowsMultimap(Iterable<CreatedIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<CreatedIdxRow, CreatedIdxColumnValue> getRowsMultimapInternal(Iterable<CreatedIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<CreatedIdxRow, CreatedIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<CreatedIdxRow, CreatedIdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                CreatedIdxRow row = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = CreatedIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, CreatedIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<CreatedIdxRow, BatchingVisitable<CreatedIdxColumnValue>> getRowsColumnRange(Iterable<CreatedIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<CreatedIdxRow, BatchingVisitable<CreatedIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                CreatedIdxRow row = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<CreatedIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = CreatedIdxColumnValue.hydrateValue(result.getValue());
                    return CreatedIdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<CreatedIdxRow, CreatedIdxColumnValue>> getRowsColumnRange(Iterable<CreatedIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                CreatedIdxRow row = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = CreatedIdxColumnValue.hydrateValue(e.getValue());
                CreatedIdxColumnValue colValue = CreatedIdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<CreatedIdxRow, Iterator<CreatedIdxColumnValue>> getRowsColumnRangeIterator(Iterable<CreatedIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<CreatedIdxRow, Iterator<CreatedIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                CreatedIdxRow row = CreatedIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<CreatedIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    CreatedIdxColumn col = CreatedIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = CreatedIdxColumnValue.hydrateValue(result.getValue());
                    return CreatedIdxColumnValue.of(col, val);
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

        public BatchingVisitableView<CreatedIdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, CreatedIdxRowResult>() {
                @Override
                public CreatedIdxRowResult apply(RowResult<byte[]> input) {
                    return CreatedIdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<CreatedIdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<CreatedIdxRowResult>>() {
                @Override
                public BatchingVisitable<CreatedIdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, CreatedIdxRowResult>() {
                        @Override
                        public CreatedIdxRowResult apply(RowResult<byte[]> row) {
                            return CreatedIdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<CreatedIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, CreatedIdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<CreatedIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, CreatedIdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<CreatedIdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, CreatedIdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<CreatedIdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<CreatedIdxRowResult> rowResults) {
                    Multimap<CreatedIdxRow, CreatedIdxColumn> toRemove = HashMultimap.create();
                    for (CreatedIdxRowResult rowResult : rowResults) {
                        for (CreatedIdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    }


    @Generated("com.palantir.atlasdb.table.description.render.TableRenderer")
    @SuppressWarnings({"all", "deprecation"})
    public static final class UserBirthdaysIdxTable implements
            AtlasDbDynamicMutablePersistentTable<UserBirthdaysIdxTable.UserBirthdaysIdxRow,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxRowResult> {
        private final Transaction t;
        private final List<UserBirthdaysIdxTrigger> triggers;
        private final static String rawTableName = "user_birthdays_idx";
        private final TableReference tableRef;
        private final static ColumnSelection allColumns = ColumnSelection.all();

        public static UserBirthdaysIdxTable of(UserProfileTable table) {
            return new UserBirthdaysIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<UserBirthdaysIdxTrigger>of());
        }

        public static UserBirthdaysIdxTable of(UserProfileTable table, UserBirthdaysIdxTrigger trigger, UserBirthdaysIdxTrigger... triggers) {
            return new UserBirthdaysIdxTable(table.t, table.tableRef.getNamespace(), ImmutableList.<UserBirthdaysIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static UserBirthdaysIdxTable of(UserProfileTable table, List<UserBirthdaysIdxTrigger> triggers) {
            return new UserBirthdaysIdxTable(table.t, table.tableRef.getNamespace(), triggers);
        }

        private UserBirthdaysIdxTable(Transaction t, Namespace namespace, List<UserBirthdaysIdxTrigger> triggers) {
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
         * UserBirthdaysIdxRow {
         *   {@literal Long birthday};
         * }
         * </pre>
         */
        public static final class UserBirthdaysIdxRow implements Persistable, Comparable<UserBirthdaysIdxRow> {
            private final long birthday;

            public static UserBirthdaysIdxRow of(long birthday) {
                return new UserBirthdaysIdxRow(birthday);
            }

            private UserBirthdaysIdxRow(long birthday) {
                this.birthday = birthday;
            }

            public long getBirthday() {
                return birthday;
            }

            public static Function<UserBirthdaysIdxRow, Long> getBirthdayFun() {
                return new Function<UserBirthdaysIdxRow, Long>() {
                    @Override
                    public Long apply(UserBirthdaysIdxRow row) {
                        return row.birthday;
                    }
                };
            }

            public static Function<Long, UserBirthdaysIdxRow> fromBirthdayFun() {
                return new Function<Long, UserBirthdaysIdxRow>() {
                    @Override
                    public UserBirthdaysIdxRow apply(Long row) {
                        return UserBirthdaysIdxRow.of(row);
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] birthdayBytes = EncodingUtils.encodeSignedVarLong(birthday);
                return EncodingUtils.add(birthdayBytes);
            }

            public static final Hydrator<UserBirthdaysIdxRow> BYTES_HYDRATOR = new Hydrator<UserBirthdaysIdxRow>() {
                @Override
                public UserBirthdaysIdxRow hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    Long birthday = EncodingUtils.decodeSignedVarLong(_input, _index);
                    _index += EncodingUtils.sizeOfSignedVarLong(birthday);
                    return new UserBirthdaysIdxRow(birthday);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("birthday", birthday)
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
                UserBirthdaysIdxRow other = (UserBirthdaysIdxRow) obj;
                return Objects.equals(birthday, other.birthday);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Objects.hashCode(birthday);
            }

            @Override
            public int compareTo(UserBirthdaysIdxRow o) {
                return ComparisonChain.start()
                    .compare(this.birthday, o.birthday)
                    .result();
            }
        }

        /**
         * <pre>
         * UserBirthdaysIdxColumn {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * </pre>
         */
        public static final class UserBirthdaysIdxColumn implements Persistable, Comparable<UserBirthdaysIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final UUID id;

            public static UserBirthdaysIdxColumn of(byte[] rowName, byte[] columnName, UUID id) {
                return new UserBirthdaysIdxColumn(rowName, columnName, id);
            }

            private UserBirthdaysIdxColumn(byte[] rowName, byte[] columnName, UUID id) {
                this.rowName = rowName;
                this.columnName = columnName;
                this.id = id;
            }

            public byte[] getRowName() {
                return rowName;
            }

            public byte[] getColumnName() {
                return columnName;
            }

            public UUID getId() {
                return id;
            }

            public static Function<UserBirthdaysIdxColumn, byte[]> getRowNameFun() {
                return new Function<UserBirthdaysIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(UserBirthdaysIdxColumn row) {
                        return row.rowName;
                    }
                };
            }

            public static Function<UserBirthdaysIdxColumn, byte[]> getColumnNameFun() {
                return new Function<UserBirthdaysIdxColumn, byte[]>() {
                    @Override
                    public byte[] apply(UserBirthdaysIdxColumn row) {
                        return row.columnName;
                    }
                };
            }

            public static Function<UserBirthdaysIdxColumn, UUID> getIdFun() {
                return new Function<UserBirthdaysIdxColumn, UUID>() {
                    @Override
                    public UUID apply(UserBirthdaysIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = EncodingUtils.encodeUUID(id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<UserBirthdaysIdxColumn> BYTES_HYDRATOR = new Hydrator<UserBirthdaysIdxColumn>() {
                @Override
                public UserBirthdaysIdxColumn hydrateFromBytes(byte[] _input) {
                    int _index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(_input, _index);
                    _index += EncodingUtils.sizeOfSizedBytes(columnName);
                    UUID id = EncodingUtils.decodeUUID(_input, _index);
                    _index += 16;
                    return new UserBirthdaysIdxColumn(rowName, columnName, id);
                }
            };

            public static BatchColumnRangeSelection createPrefixRangeUnsorted(byte[] rowName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes), batchSize);
            }

            public static Prefix prefixUnsorted(byte[] rowName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                return new Prefix(EncodingUtils.add(rowNameBytes));
            }

            public static BatchColumnRangeSelection createPrefixRange(byte[] rowName, byte[] columnName, int batchSize) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return ColumnRangeSelections.createPrefixRange(EncodingUtils.add(rowNameBytes, columnNameBytes), batchSize);
            }

            public static Prefix prefix(byte[] rowName, byte[] columnName) {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                return new Prefix(EncodingUtils.add(rowNameBytes, columnNameBytes));
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("rowName", rowName)
                    .add("columnName", columnName)
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
                UserBirthdaysIdxColumn other = (UserBirthdaysIdxColumn) obj;
                return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName) && Objects.equals(id, other.id);
            }

            @SuppressWarnings("ArrayHashCode")
            @Override
            public int hashCode() {
                return Arrays.deepHashCode(new Object[]{ rowName, columnName, id });
            }

            @Override
            public int compareTo(UserBirthdaysIdxColumn o) {
                return ComparisonChain.start()
                    .compare(this.rowName, o.rowName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.columnName, o.columnName, UnsignedBytes.lexicographicalComparator())
                    .compare(this.id, o.id)
                    .result();
            }
        }

        public interface UserBirthdaysIdxTrigger {
            public void putUserBirthdaysIdx(Multimap<UserBirthdaysIdxRow, ? extends UserBirthdaysIdxColumnValue> newRows);
        }

        /**
         * <pre>
         * Column name description {
         *   {@literal byte[] rowName};
         *   {@literal byte[] columnName};
         *   {@literal UUID id};
         * }
         * Column value description {
         *   type: Long;
         * }
         * </pre>
         */
        public static final class UserBirthdaysIdxColumnValue implements ColumnValue<Long> {
            private final UserBirthdaysIdxColumn columnName;
            private final Long value;

            public static UserBirthdaysIdxColumnValue of(UserBirthdaysIdxColumn columnName, Long value) {
                return new UserBirthdaysIdxColumnValue(columnName, value);
            }

            private UserBirthdaysIdxColumnValue(UserBirthdaysIdxColumn columnName, Long value) {
                this.columnName = columnName;
                this.value = value;
            }

            public UserBirthdaysIdxColumn getColumnName() {
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

            public static Function<UserBirthdaysIdxColumnValue, UserBirthdaysIdxColumn> getColumnNameFun() {
                return new Function<UserBirthdaysIdxColumnValue, UserBirthdaysIdxColumn>() {
                    @Override
                    public UserBirthdaysIdxColumn apply(UserBirthdaysIdxColumnValue columnValue) {
                        return columnValue.getColumnName();
                    }
                };
            }

            public static Function<UserBirthdaysIdxColumnValue, Long> getValueFun() {
                return new Function<UserBirthdaysIdxColumnValue, Long>() {
                    @Override
                    public Long apply(UserBirthdaysIdxColumnValue columnValue) {
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

        public static final class UserBirthdaysIdxRowResult implements TypedRowResult {
            private final UserBirthdaysIdxRow rowName;
            private final ImmutableSet<UserBirthdaysIdxColumnValue> columnValues;

            public static UserBirthdaysIdxRowResult of(RowResult<byte[]> rowResult) {
                UserBirthdaysIdxRow rowName = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
                Set<UserBirthdaysIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long value = UserBirthdaysIdxColumnValue.hydrateValue(e.getValue());
                    columnValues.add(UserBirthdaysIdxColumnValue.of(col, value));
                }
                return new UserBirthdaysIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
            }

            private UserBirthdaysIdxRowResult(UserBirthdaysIdxRow rowName, ImmutableSet<UserBirthdaysIdxColumnValue> columnValues) {
                this.rowName = rowName;
                this.columnValues = columnValues;
            }

            @Override
            public UserBirthdaysIdxRow getRowName() {
                return rowName;
            }

            public Set<UserBirthdaysIdxColumnValue> getColumnValues() {
                return columnValues;
            }

            public static Function<UserBirthdaysIdxRowResult, UserBirthdaysIdxRow> getRowNameFun() {
                return new Function<UserBirthdaysIdxRowResult, UserBirthdaysIdxRow>() {
                    @Override
                    public UserBirthdaysIdxRow apply(UserBirthdaysIdxRowResult rowResult) {
                        return rowResult.rowName;
                    }
                };
            }

            public static Function<UserBirthdaysIdxRowResult, ImmutableSet<UserBirthdaysIdxColumnValue>> getColumnValuesFun() {
                return new Function<UserBirthdaysIdxRowResult, ImmutableSet<UserBirthdaysIdxColumnValue>>() {
                    @Override
                    public ImmutableSet<UserBirthdaysIdxColumnValue> apply(UserBirthdaysIdxRowResult rowResult) {
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
        public void delete(UserBirthdaysIdxRow row, UserBirthdaysIdxColumn column) {
            delete(ImmutableMultimap.of(row, column));
        }

        @Override
        public void delete(Iterable<UserBirthdaysIdxRow> rows) {
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> toRemove = HashMultimap.create();
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> result = getRowsMultimap(rows);
            for (Entry<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> e : result.entries()) {
                toRemove.put(e.getKey(), e.getValue().getColumnName());
            }
            delete(toRemove);
        }

        @Override
        public void delete(Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> values) {
            t.delete(tableRef, ColumnValues.toCells(values));
        }

        @Override
        public void put(UserBirthdaysIdxRow rowName, Iterable<UserBirthdaysIdxColumnValue> values) {
            put(ImmutableMultimap.<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(UserBirthdaysIdxRow rowName, UserBirthdaysIdxColumnValue... values) {
            put(ImmutableMultimap.<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>builder().putAll(rowName, values).build());
        }

        @Override
        public void put(Multimap<UserBirthdaysIdxRow, ? extends UserBirthdaysIdxColumnValue> values) {
            t.useTable(tableRef, this);
            t.put(tableRef, ColumnValues.toCellValues(values));
            for (UserBirthdaysIdxTrigger trigger : triggers) {
                trigger.putUserBirthdaysIdx(values);
            }
        }

        @Override
        public void touch(Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> values) {
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> currentValues = get(values);
            put(currentValues);
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> toDelete = HashMultimap.create(values);
            for (Map.Entry<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> e : currentValues.entries()) {
                toDelete.remove(e.getKey(), e.getValue().getColumnName());
            }
            delete(toDelete);
        }

        public static ColumnSelection getColumnSelection(Collection<UserBirthdaysIdxColumn> cols) {
            return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
        }

        public static ColumnSelection getColumnSelection(UserBirthdaysIdxColumn... cols) {
            return getColumnSelection(Arrays.asList(cols));
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> get(Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> cells) {
            Set<Cell> rawCells = ColumnValues.toCells(cells);
            Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> rowMap = HashMultimap.create();
            for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
                if (e.getValue().length > 0) {
                    UserBirthdaysIdxRow row = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                    Long val = UserBirthdaysIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, UserBirthdaysIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public List<UserBirthdaysIdxColumnValue> getRowColumns(UserBirthdaysIdxRow row) {
            return getRowColumns(row, allColumns);
        }

        @Override
        public List<UserBirthdaysIdxColumnValue> getRowColumns(UserBirthdaysIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
            if (rowResult == null) {
                return ImmutableList.of();
            } else {
                List<UserBirthdaysIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
                for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = UserBirthdaysIdxColumnValue.hydrateValue(e.getValue());
                    ret.add(UserBirthdaysIdxColumnValue.of(col, val));
                }
                return ret;
            }
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowsMultimap(Iterable<UserBirthdaysIdxRow> rows) {
            return getRowsMultimapInternal(rows, allColumns);
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowsMultimap(Iterable<UserBirthdaysIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        private Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowsMultimapInternal(Iterable<UserBirthdaysIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
            return getRowMapFromRowResults(results.values());
        }

        private static Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
            Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> rowMap = HashMultimap.create();
            for (RowResult<byte[]> result : rowResults) {
                UserBirthdaysIdxRow row = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
                for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                    Long val = UserBirthdaysIdxColumnValue.hydrateValue(e.getValue());
                    rowMap.put(row, UserBirthdaysIdxColumnValue.of(col, val));
                }
            }
            return rowMap;
        }

        @Override
        public Map<UserBirthdaysIdxRow, BatchingVisitable<UserBirthdaysIdxColumnValue>> getRowsColumnRange(Iterable<UserBirthdaysIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<UserBirthdaysIdxRow, BatchingVisitable<UserBirthdaysIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                UserBirthdaysIdxRow row = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                BatchingVisitable<UserBirthdaysIdxColumnValue> bv = BatchingVisitables.transform(e.getValue(), result -> {
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = UserBirthdaysIdxColumnValue.hydrateValue(result.getValue());
                    return UserBirthdaysIdxColumnValue.of(col, val);
                });
                transformed.put(row, bv);
            }
            return transformed;
        }

        @Override
        public Iterator<Map.Entry<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>> getRowsColumnRange(Iterable<UserBirthdaysIdxRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
            Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
            return Iterators.transform(results, e -> {
                UserBirthdaysIdxRow row = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = UserBirthdaysIdxColumnValue.hydrateValue(e.getValue());
                UserBirthdaysIdxColumnValue colValue = UserBirthdaysIdxColumnValue.of(col, val);
                return Maps.immutableEntry(row, colValue);
            });
        }

        @Override
        public Map<UserBirthdaysIdxRow, Iterator<UserBirthdaysIdxColumnValue>> getRowsColumnRangeIterator(Iterable<UserBirthdaysIdxRow> rows, BatchColumnRangeSelection columnRangeSelection) {
            Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
            Map<UserBirthdaysIdxRow, Iterator<UserBirthdaysIdxColumnValue>> transformed = Maps.newHashMapWithExpectedSize(results.size());
            for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
                UserBirthdaysIdxRow row = UserBirthdaysIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Iterator<UserBirthdaysIdxColumnValue> bv = Iterators.transform(e.getValue(), result -> {
                    UserBirthdaysIdxColumn col = UserBirthdaysIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());
                    Long val = UserBirthdaysIdxColumnValue.hydrateValue(result.getValue());
                    return UserBirthdaysIdxColumnValue.of(col, val);
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

        public BatchingVisitableView<UserBirthdaysIdxRowResult> getRange(RangeRequest range) {
            return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new Function<RowResult<byte[]>, UserBirthdaysIdxRowResult>() {
                @Override
                public UserBirthdaysIdxRowResult apply(RowResult<byte[]> input) {
                    return UserBirthdaysIdxRowResult.of(input);
                }
            });
        }

        @Deprecated
        public IterableView<BatchingVisitable<UserBirthdaysIdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef, optimizeRangeRequests(ranges));
            return IterableView.of(rangeResults).transform(
                    new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<UserBirthdaysIdxRowResult>>() {
                @Override
                public BatchingVisitable<UserBirthdaysIdxRowResult> apply(BatchingVisitable<RowResult<byte[]>> visitable) {
                    return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, UserBirthdaysIdxRowResult>() {
                        @Override
                        public UserBirthdaysIdxRowResult apply(RowResult<byte[]> row) {
                            return UserBirthdaysIdxRowResult.of(row);
                        }
                    });
                }
            });
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       int concurrencyLevel,
                                       BiFunction<RangeRequest, BatchingVisitable<UserBirthdaysIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .concurrencyLevel(concurrencyLevel)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, UserBirthdaysIdxRowResult::of)))
                                .build());
        }

        public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,
                                       BiFunction<RangeRequest, BatchingVisitable<UserBirthdaysIdxRowResult>, T> visitableProcessor) {
            return t.getRanges(ImmutableGetRangesQuery.<T>builder()
                                .tableRef(tableRef)
                                .rangeRequests(ranges)
                                .rangeRequestOptimizer(this::optimizeRangeRequest)
                                .visitableProcessor((rangeRequest, visitable) ->
                                        visitableProcessor.apply(rangeRequest,
                                                BatchingVisitables.transform(visitable, UserBirthdaysIdxRowResult::of)))
                                .build());
        }

        public Stream<BatchingVisitable<UserBirthdaysIdxRowResult>> getRangesLazy(Iterable<RangeRequest> ranges) {
            Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef, optimizeRangeRequests(ranges));
            return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, UserBirthdaysIdxRowResult::of));
        }

        public void deleteRange(RangeRequest range) {
            deleteRanges(ImmutableSet.of(range));
        }

        public void deleteRanges(Iterable<RangeRequest> ranges) {
            BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<UserBirthdaysIdxRowResult>, RuntimeException>() {
                @Override
                public boolean visit(List<UserBirthdaysIdxRowResult> rowResults) {
                    Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> toRemove = HashMultimap.create();
                    for (UserBirthdaysIdxRowResult rowResult : rowResults) {
                        for (UserBirthdaysIdxColumnValue columnValue : rowResult.getColumnValues()) {
                            toRemove.put(rowResult.getRowName(), columnValue.getColumnName());
                        }
                    }
                    delete(toRemove);
                    return true;
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
    static String __CLASS_HASH = "ElEfEgDtzUQU+6sxJk99mQ==";
}
