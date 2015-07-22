package com.palantir.example.profile.schema.generated;

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


public final class UserProfileTable implements
        AtlasDbMutablePersistentTable<UserProfileTable.UserProfileRow,
                                         UserProfileTable.UserProfileNamedColumnValue<?>,
                                         UserProfileTable.UserProfileRowResult>,
        AtlasDbNamedMutableTable<UserProfileTable.UserProfileRow,
                                    UserProfileTable.UserProfileNamedColumnValue<?>,
                                    UserProfileTable.UserProfileRowResult> {
    private final Transaction t;
    private final List<UserProfileTrigger> triggers;
    private final static String tableName = "user_profile";

    static UserProfileTable of(Transaction t) {
        return new UserProfileTable(t, ImmutableList.<UserProfileTrigger>of());
    }

    static UserProfileTable of(Transaction t, UserProfileTrigger trigger, UserProfileTrigger... triggers) {
        return new UserProfileTable(t, ImmutableList.<UserProfileTrigger>builder().add(trigger).add(triggers).build());
    }

    static UserProfileTable of(Transaction t, List<UserProfileTrigger> triggers) {
        return new UserProfileTable(t, triggers);
    }

    private UserProfileTable(Transaction t, List<UserProfileTrigger> triggers) {
        this.t = t;
        this.triggers = triggers;
    }

    public static String getTableName() {
        return tableName;
    }

    /**
     * <pre>
     * UserProfileRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class UserProfileRow implements Persistable, Comparable<UserProfileRow> {
        private final long id;

        public static UserProfileRow of(long id) {
            return new UserProfileRow(id);
        }

        private UserProfileRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<UserProfileRow, Long> getIdFun() {
            return new Function<UserProfileRow, Long>() {
                @Override
                public Long apply(UserProfileRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, UserProfileRow> fromIdFun() {
            return new Function<Long, UserProfileRow>() {
                @Override
                public UserProfileRow apply(Long row) {
                    return new UserProfileRow(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<UserProfileRow> BYTES_HYDRATOR = new Hydrator<UserProfileRow>() {
            @Override
            public UserProfileRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                return of(id);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
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
            return Objects.equal(id, other.id);
        }

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

        public boolean hasMetadata() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("m"));
        }

        public boolean hasPhotoStreamId() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("p"));
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
            return MoreObjects.toStringHelper(this)
                    .add("RowName", getRowName())
                    .add("Metadata", getMetadata())
                    .add("PhotoStreamId", getPhotoStreamId())
                .toString();
        }
    }

    public enum UserProfileNamedColumn {
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
                .put("p", PhotoStreamId.BYTES_HYDRATOR)
                .build();

    public Map<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> getMetadatas(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("m")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
        Map<UserProfileRow, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile val = Metadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<UserProfileRow, Long> getPhotoStreamIds(Collection<UserProfileRow> rows) {
        Map<Cell, UserProfileRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (UserProfileRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("p")), row);
        }
        Map<Cell, byte[]> results = t.get(tableName, cells.keySet());
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
        t.useTable(tableName, this);
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> affectedCells = getAffectedCells(rows);
        deleteUserBirthdaysIdx(affectedCells);
        for (Entry<UserProfileRow, ? extends UserProfileNamedColumnValue<?>> e : rows.entries()) {
            if (e.getValue() instanceof Metadata)
            {
                Metadata col = (Metadata) e.getValue();
                {
                    UserProfileRow row = e.getKey();
                    UserBirthdaysIdxTable table = UserBirthdaysIdxTable.of(t);
                    long birthday = col.getValue().getBirthEpochDay();
                    long id = row.getId();
                    UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue indexColVal = UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue.of(indexCol, 0L);
                    table.put(indexRow, indexColVal);
                }
            }
        }
        t.put(tableName, ColumnValues.toCellValues(rows));
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
        Map<Cell, byte[]> results = t.get(tableName, cells);
        deleteUserBirthdaysIdxRaw(results);
        t.delete(tableName, cells);
    }

    private void deleteUserBirthdaysIdxRaw(Map<Cell, byte[]> results) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<Cell, byte[]> result : results.entrySet()) {
            Metadata col = (Metadata) shortNameToHydrator.get("m").hydrateFromBytes(result.getValue());
            UserProfileRow row = UserProfileRow.BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());
            long birthday = col.getValue().getBirthEpochDay();
            long id = row.getId();
            UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
            UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), col.persistColumnName(), id);
            indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
        }
        t.delete("user_birthdays_idx", indexCells.build());
    }

    public void deletePhotoStreamId(UserProfileRow row) {
        deletePhotoStreamId(ImmutableSet.of(row));
    }

    public void deletePhotoStreamId(Iterable<UserProfileRow> rows) {
        byte[] col = PtBytes.toCachedBytes("p");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableName, cells);
    }

    @Override
    public void delete(UserProfileRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<UserProfileRow> rows) {
        Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result = getRowsMultimap(rows);
        deleteUserBirthdaysIdx(result);
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        ImmutableSet.Builder<Cell> cells = ImmutableSet.builder();
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("m")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("p")));
        t.delete(tableName, cells.build());
    }

    @Override
    public Optional<UserProfileRowResult> getRow(UserProfileRow row) {
        return getRow(row, ColumnSelection.all());
    }

    @Override
    public Optional<UserProfileRowResult> getRow(UserProfileRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.absent();
        } else {
            return Optional.of(UserProfileRowResult.of(rowResult));
        }
    }

    @Override
    public List<UserProfileRowResult> getRows(Iterable<UserProfileRow> rows) {
        return getRows(rows, ColumnSelection.all());
    }

    @Override
    public List<UserProfileRowResult> getRows(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        List<UserProfileRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(UserProfileRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<UserProfileRowResult> getAsyncRows(Iterable<UserProfileRow> rows, ExecutorService exec) {
        return getAsyncRows(rows, ColumnSelection.all(), exec);
    }

    @Override
    public List<UserProfileRowResult> getAsyncRows(final Iterable<UserProfileRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<List<UserProfileRowResult>> c =
                new Callable<List<UserProfileRowResult>>() {
            @Override
            public List<UserProfileRowResult> call() {
                return getRows(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), List.class);
    }

    @Override
    public List<UserProfileNamedColumnValue<?>> getRowColumns(UserProfileRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<UserProfileNamedColumnValue<?>> getRowColumns(UserProfileRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
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
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowsMultimap(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getAsyncRowsMultimap(Iterable<UserProfileRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getAsyncRowsMultimap(final Iterable<UserProfileRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<UserProfileRow, UserProfileNamedColumnValue<?>>> c =
                new Callable<Multimap<UserProfileRow, UserProfileNamedColumnValue<?>>>() {
            @Override
            public Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> getRowsMultimapInternal(Iterable<UserProfileRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
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

    private void deleteUserBirthdaysIdx(Multimap<UserProfileRow, UserProfileNamedColumnValue<?>> result) {
        ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();
        for (Entry<UserProfileRow, UserProfileNamedColumnValue<?>> e : result.entries()) {
            if (e.getValue() instanceof Metadata) {
                Metadata col = (Metadata) e.getValue();{
                    UserProfileRow row = e.getKey();
                    long birthday = col.getValue().getBirthEpochDay();
                    long id = row.getId();
                    UserBirthdaysIdxTable.UserBirthdaysIdxRow indexRow = UserBirthdaysIdxTable.UserBirthdaysIdxRow.of(birthday);
                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn indexCol = UserBirthdaysIdxTable.UserBirthdaysIdxColumn.of(row.persistToBytes(), e.getValue().persistColumnName(), id);
                    indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));
                }
            }
        }
        t.delete("user_birthdays_idx", indexCells.build());
    }

    public BatchingVisitableView<UserProfileRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<UserProfileRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, UserProfileRowResult>() {
            @Override
            public UserProfileRowResult apply(RowResult<byte[]> input) {
                return UserProfileRowResult.of(input);
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

    public static final class UserBirthdaysIdxTable implements
            AtlasDbDynamicMutablePersistentTable<UserBirthdaysIdxTable.UserBirthdaysIdxRow,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxColumn,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue,
                                                    UserBirthdaysIdxTable.UserBirthdaysIdxRowResult> {
        private final Transaction t;
        private final List<UserBirthdaysIdxTrigger> triggers;
        private final static String tableName = "user_birthdays_idx";

        public static UserBirthdaysIdxTable of(Transaction t) {
            return new UserBirthdaysIdxTable(t, ImmutableList.<UserBirthdaysIdxTrigger>of());
        }

        public static UserBirthdaysIdxTable of(Transaction t, UserBirthdaysIdxTrigger trigger, UserBirthdaysIdxTrigger... triggers) {
            return new UserBirthdaysIdxTable(t, ImmutableList.<UserBirthdaysIdxTrigger>builder().add(trigger).add(triggers).build());
        }

        public static UserBirthdaysIdxTable of(Transaction t, List<UserBirthdaysIdxTrigger> triggers) {
            return new UserBirthdaysIdxTable(t, triggers);
        }

        private UserBirthdaysIdxTable(Transaction t, List<UserBirthdaysIdxTrigger> triggers) {
            this.t = t;
            this.triggers = triggers;
        }

        public static String getTableName() {
            return tableName;
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
                        return new UserBirthdaysIdxRow(row);
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
                public UserBirthdaysIdxRow hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    Long birthday = EncodingUtils.decodeSignedVarLong(__input, __index);
                    __index += EncodingUtils.sizeOfSignedVarLong(birthday);
                    return of(birthday);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
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
                return Objects.equal(birthday, other.birthday);
            }

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
         *   {@literal Long id};
         * }
         * </pre>
         */
        public static final class UserBirthdaysIdxColumn implements Persistable, Comparable<UserBirthdaysIdxColumn> {
            private final byte[] rowName;
            private final byte[] columnName;
            private final long id;

            public static UserBirthdaysIdxColumn of(byte[] rowName, byte[] columnName, long id) {
                return new UserBirthdaysIdxColumn(rowName, columnName, id);
            }

            private UserBirthdaysIdxColumn(byte[] rowName, byte[] columnName, long id) {
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

            public long getId() {
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

            public static Function<UserBirthdaysIdxColumn, Long> getIdFun() {
                return new Function<UserBirthdaysIdxColumn, Long>() {
                    @Override
                    public Long apply(UserBirthdaysIdxColumn row) {
                        return row.id;
                    }
                };
            }

            @Override
            public byte[] persistToBytes() {
                byte[] rowNameBytes = EncodingUtils.encodeSizedBytes(rowName);
                byte[] columnNameBytes = EncodingUtils.encodeSizedBytes(columnName);
                byte[] idBytes = PtBytes.toBytes(Long.MIN_VALUE ^ id);
                return EncodingUtils.add(rowNameBytes, columnNameBytes, idBytes);
            }

            public static final Hydrator<UserBirthdaysIdxColumn> BYTES_HYDRATOR = new Hydrator<UserBirthdaysIdxColumn>() {
                @Override
                public UserBirthdaysIdxColumn hydrateFromBytes(byte[] __input) {
                    int __index = 0;
                    byte[] rowName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(rowName);
                    byte[] columnName = EncodingUtils.decodeSizedBytes(__input, __index);
                    __index += EncodingUtils.sizeOfSizedBytes(columnName);
                    Long id = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                    __index += 8;
                    return of(rowName, columnName, id);
                }
            };

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
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
                return Objects.equal(rowName, other.rowName) && Objects.equal(columnName, other.columnName) && Objects.equal(id, other.id);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(rowName, columnName, id);
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
         *   {@literal Long id};
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
            t.delete(tableName, ColumnValues.toCells(values));
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
            t.useTable(tableName, this);
            t.put(tableName, ColumnValues.toCellValues(values));
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
            Map<Cell, byte[]> rawResults = t.get(tableName, rawCells);
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
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getAsync(final Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumn> cells, ExecutorService exec) {
            Callable<Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>> c =
                    new Callable<Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>>() {
                @Override
                public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> call() {
                    return get(cells);
                }
            };
            return AsyncProxy.create(exec.submit(c), Multimap.class);
        }

        @Override
        public List<UserBirthdaysIdxColumnValue> getRowColumns(UserBirthdaysIdxRow row) {
            return getRowColumns(row, ColumnSelection.all());
        }

        @Override
        public List<UserBirthdaysIdxColumnValue> getRowColumns(UserBirthdaysIdxRow row, ColumnSelection columns) {
            byte[] bytes = row.persistToBytes();
            RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
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
            return getRowsMultimapInternal(rows, ColumnSelection.all());
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowsMultimap(Iterable<UserBirthdaysIdxRow> rows, ColumnSelection columns) {
            return getRowsMultimapInternal(rows, columns);
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getAsyncRowsMultimap(Iterable<UserBirthdaysIdxRow> rows, ExecutorService exec) {
            return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
        }

        @Override
        public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getAsyncRowsMultimap(final Iterable<UserBirthdaysIdxRow> rows, final ColumnSelection columns, ExecutorService exec) {
            Callable<Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>> c =
                    new Callable<Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue>>() {
                @Override
                public Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> call() {
                    return getRowsMultimapInternal(rows, columns);
                }
            };
            return AsyncProxy.create(exec.submit(c), Multimap.class);
        }

        private Multimap<UserBirthdaysIdxRow, UserBirthdaysIdxColumnValue> getRowsMultimapInternal(Iterable<UserBirthdaysIdxRow> rows, ColumnSelection columns) {
            SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
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

        public BatchingVisitableView<UserBirthdaysIdxRowResult> getRange(RangeRequest range) {
            if (range.getColumnNames().isEmpty()) {
                range = range.getBuilder().retainColumns(ColumnSelection.all()).build();
            }
            return BatchingVisitables.transform(t.getRange(tableName, range), new Function<RowResult<byte[]>, UserBirthdaysIdxRowResult>() {
                @Override
                public UserBirthdaysIdxRowResult apply(RowResult<byte[]> input) {
                    return UserBirthdaysIdxRowResult.of(input);
                }
            });
        }

        public IterableView<BatchingVisitable<UserBirthdaysIdxRowResult>> getRanges(Iterable<RangeRequest> ranges) {
            Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableName, ranges);
            return IterableView.of(Iterables.transform(rangeResults,
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
            }));
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
     * {@link HashMultimap}
     * {@link HashSet}
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
     */
    static String __CLASS_HASH = "L+6b+QKYNSI5433jKguIKQ==";
}
