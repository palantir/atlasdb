package com.palantir.atlasdb.table.description.generated;

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

import javax.annotation.Nullable;
import javax.annotation.processing.Generated;

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
@SuppressWarnings({"deprecation"})
public final class AllValueTypesTestTable implements
        AtlasDbMutablePersistentTable<AllValueTypesTestTable.AllValueTypesTestRow,
                                         AllValueTypesTestTable.AllValueTypesTestNamedColumnValue<?>,
                                         AllValueTypesTestTable.AllValueTypesTestRowResult>,
        AtlasDbNamedMutableTable<AllValueTypesTestTable.AllValueTypesTestRow,
                                    AllValueTypesTestTable.AllValueTypesTestNamedColumnValue<?>,
                                    AllValueTypesTestTable.AllValueTypesTestRowResult> {
    private final Transaction t;
    private final List<AllValueTypesTestTrigger> triggers;
    private final static String rawTableName = "AllValueTypesTest";
    private final TableReference tableRef;
    private final static ColumnSelection allColumns = getColumnSelection(AllValueTypesTestNamedColumn.values());

    static AllValueTypesTestTable of(Transaction t, Namespace namespace) {
        return new AllValueTypesTestTable(t, namespace, ImmutableList.<AllValueTypesTestTrigger>of());
    }

    static AllValueTypesTestTable of(Transaction t, Namespace namespace, AllValueTypesTestTrigger trigger, AllValueTypesTestTrigger... triggers) {
        return new AllValueTypesTestTable(t, namespace, ImmutableList.<AllValueTypesTestTrigger>builder().add(trigger).add(triggers).build());
    }

    static AllValueTypesTestTable of(Transaction t, Namespace namespace, List<AllValueTypesTestTrigger> triggers) {
        return new AllValueTypesTestTable(t, namespace, triggers);
    }

    private AllValueTypesTestTable(Transaction t, Namespace namespace, List<AllValueTypesTestTrigger> triggers) {
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
     * AllValueTypesTestRow {
     *   {@literal Long componentVARLONG};
     *   {@literal Long componentVARSIGNEDLONG};
     *   {@literal Long componentFIXEDLONG};
     *   {@literal Long componentFIXEDLONGLITTLEENDIAN};
     *   {@literal Sha256Hash componentSHA256HASH};
     *   {@literal String componentVARSTRING};
     *   {@literal byte[] componentSIZEDBLOB};
     *   {@literal Long componentNULLABLEFIXEDLONG};
     *   {@literal UUID componentUUID};
     *   {@literal byte[] blobComponent};
     * }
     * </pre>
     */
    public static final class AllValueTypesTestRow implements Persistable, Comparable<AllValueTypesTestRow> {
        private final long componentVARLONG;
        private final long componentVARSIGNEDLONG;
        private final long componentFIXEDLONG;
        private final long componentFIXEDLONGLITTLEENDIAN;
        private final Sha256Hash componentSHA256HASH;
        private final String componentVARSTRING;
        private final byte[] componentSIZEDBLOB;
        private final @Nullable Long componentNULLABLEFIXEDLONG;
        private final UUID componentUUID;
        private final byte[] blobComponent;

        public static AllValueTypesTestRow of(long componentVARLONG, long componentVARSIGNEDLONG, long componentFIXEDLONG, long componentFIXEDLONGLITTLEENDIAN, Sha256Hash componentSHA256HASH, String componentVARSTRING, byte[] componentSIZEDBLOB, @Nullable Long componentNULLABLEFIXEDLONG, UUID componentUUID, byte[] blobComponent) {
            return new AllValueTypesTestRow(componentVARLONG, componentVARSIGNEDLONG, componentFIXEDLONG, componentFIXEDLONGLITTLEENDIAN, componentSHA256HASH, componentVARSTRING, componentSIZEDBLOB, componentNULLABLEFIXEDLONG, componentUUID, blobComponent);
        }

        private AllValueTypesTestRow(long componentVARLONG, long componentVARSIGNEDLONG, long componentFIXEDLONG, long componentFIXEDLONGLITTLEENDIAN, Sha256Hash componentSHA256HASH, String componentVARSTRING, byte[] componentSIZEDBLOB, @Nullable Long componentNULLABLEFIXEDLONG, UUID componentUUID, byte[] blobComponent) {
            this.componentVARLONG = componentVARLONG;
            this.componentVARSIGNEDLONG = componentVARSIGNEDLONG;
            this.componentFIXEDLONG = componentFIXEDLONG;
            this.componentFIXEDLONGLITTLEENDIAN = componentFIXEDLONGLITTLEENDIAN;
            this.componentSHA256HASH = componentSHA256HASH;
            this.componentVARSTRING = componentVARSTRING;
            this.componentSIZEDBLOB = componentSIZEDBLOB;
            this.componentNULLABLEFIXEDLONG = componentNULLABLEFIXEDLONG;
            this.componentUUID = componentUUID;
            this.blobComponent = blobComponent;
        }

        public long getComponentVARLONG() {
            return componentVARLONG;
        }

        public long getComponentVARSIGNEDLONG() {
            return componentVARSIGNEDLONG;
        }

        public long getComponentFIXEDLONG() {
            return componentFIXEDLONG;
        }

        public long getComponentFIXEDLONGLITTLEENDIAN() {
            return componentFIXEDLONGLITTLEENDIAN;
        }

        public Sha256Hash getComponentSHA256HASH() {
            return componentSHA256HASH;
        }

        public String getComponentVARSTRING() {
            return componentVARSTRING;
        }

        public byte[] getComponentSIZEDBLOB() {
            return componentSIZEDBLOB;
        }

        public @Nullable Long getComponentNULLABLEFIXEDLONG() {
            return componentNULLABLEFIXEDLONG;
        }

        public UUID getComponentUUID() {
            return componentUUID;
        }

        public byte[] getBlobComponent() {
            return blobComponent;
        }

        public static Function<AllValueTypesTestRow, Long> getComponentVARLONGFun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.componentVARLONG;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponentVARSIGNEDLONGFun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.componentVARSIGNEDLONG;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponentFIXEDLONGFun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.componentFIXEDLONG;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponentFIXEDLONGLITTLEENDIANFun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.componentFIXEDLONGLITTLEENDIAN;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Sha256Hash> getComponentSHA256HASHFun() {
            return new Function<AllValueTypesTestRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(AllValueTypesTestRow row) {
                    return row.componentSHA256HASH;
                }
            };
        }

        public static Function<AllValueTypesTestRow, String> getComponentVARSTRINGFun() {
            return new Function<AllValueTypesTestRow, String>() {
                @Override
                public String apply(AllValueTypesTestRow row) {
                    return row.componentVARSTRING;
                }
            };
        }

        public static Function<AllValueTypesTestRow, byte[]> getComponentSIZEDBLOBFun() {
            return new Function<AllValueTypesTestRow, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRow row) {
                    return row.componentSIZEDBLOB;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponentNULLABLEFIXEDLONGFun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.componentNULLABLEFIXEDLONG;
                }
            };
        }

        public static Function<AllValueTypesTestRow, UUID> getComponentUUIDFun() {
            return new Function<AllValueTypesTestRow, UUID>() {
                @Override
                public UUID apply(AllValueTypesTestRow row) {
                    return row.componentUUID;
                }
            };
        }

        public static Function<AllValueTypesTestRow, byte[]> getBlobComponentFun() {
            return new Function<AllValueTypesTestRow, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRow row) {
                    return row.blobComponent;
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] componentVARLONGBytes = EncodingUtils.encodeUnsignedVarLong(componentVARLONG);
            byte[] componentVARSIGNEDLONGBytes = EncodingUtils.encodeSignedVarLong(componentVARSIGNEDLONG);
            byte[] componentFIXEDLONGBytes = PtBytes.toBytes(Long.MIN_VALUE ^ componentFIXEDLONG);
            byte[] componentFIXEDLONGLITTLEENDIANBytes = EncodingUtils.encodeLittleEndian(componentFIXEDLONGLITTLEENDIAN);
            byte[] componentSHA256HASHBytes = componentSHA256HASH.getBytes();
            byte[] componentVARSTRINGBytes = EncodingUtils.encodeVarString(componentVARSTRING);
            byte[] componentSIZEDBLOBBytes = EncodingUtils.encodeSizedBytes(componentSIZEDBLOB);
            byte[] componentNULLABLEFIXEDLONGBytes = EncodingUtils.encodeNullableFixedLong(componentNULLABLEFIXEDLONG);
            byte[] componentUUIDBytes = EncodingUtils.encodeUUID(componentUUID);
            byte[] blobComponentBytes = blobComponent;
            return EncodingUtils.add(componentVARLONGBytes, componentVARSIGNEDLONGBytes, componentFIXEDLONGBytes, componentFIXEDLONGLITTLEENDIANBytes, componentSHA256HASHBytes, componentVARSTRINGBytes, componentSIZEDBLOBBytes, componentNULLABLEFIXEDLONGBytes, componentUUIDBytes, blobComponentBytes);
        }

        public static final Hydrator<AllValueTypesTestRow> BYTES_HYDRATOR = new Hydrator<AllValueTypesTestRow>() {
            @Override
            public AllValueTypesTestRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                long componentVARLONG = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(componentVARLONG);
                long componentVARSIGNEDLONG = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(componentVARSIGNEDLONG);
                long componentFIXEDLONG = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                long componentFIXEDLONGLITTLEENDIAN = EncodingUtils.decodeLittleEndian(__input, __index);
                __index += 8;
                Sha256Hash componentSHA256HASH = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                String componentVARSTRING = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(componentVARSTRING);
                byte[] componentSIZEDBLOB = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(componentSIZEDBLOB);
                @Nullable Long componentNULLABLEFIXEDLONG = EncodingUtils.decodeNullableFixedLong(__input,__index);
                __index += 9;
                UUID componentUUID = EncodingUtils.decodeUUID(__input, __index);
                __index += 16;
                byte[] blobComponent = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                return new AllValueTypesTestRow(componentVARLONG, componentVARSIGNEDLONG, componentFIXEDLONG, componentFIXEDLONGLITTLEENDIAN, componentSHA256HASH, componentVARSTRING, componentSIZEDBLOB, componentNULLABLEFIXEDLONG, componentUUID, blobComponent);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("componentVARLONG", componentVARLONG)
                .add("componentVARSIGNEDLONG", componentVARSIGNEDLONG)
                .add("componentFIXEDLONG", componentFIXEDLONG)
                .add("componentFIXEDLONGLITTLEENDIAN", componentFIXEDLONGLITTLEENDIAN)
                .add("componentSHA256HASH", componentSHA256HASH)
                .add("componentVARSTRING", componentVARSTRING)
                .add("componentSIZEDBLOB", componentSIZEDBLOB)
                .add("componentNULLABLEFIXEDLONG", componentNULLABLEFIXEDLONG)
                .add("componentUUID", componentUUID)
                .add("blobComponent", blobComponent)
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
            AllValueTypesTestRow other = (AllValueTypesTestRow) obj;
            return Objects.equals(componentVARLONG, other.componentVARLONG) && Objects.equals(componentVARSIGNEDLONG, other.componentVARSIGNEDLONG) && Objects.equals(componentFIXEDLONG, other.componentFIXEDLONG) && Objects.equals(componentFIXEDLONGLITTLEENDIAN, other.componentFIXEDLONGLITTLEENDIAN) && Objects.equals(componentSHA256HASH, other.componentSHA256HASH) && Objects.equals(componentVARSTRING, other.componentVARSTRING) && Arrays.equals(componentSIZEDBLOB, other.componentSIZEDBLOB) && Objects.equals(componentNULLABLEFIXEDLONG, other.componentNULLABLEFIXEDLONG) && Objects.equals(componentUUID, other.componentUUID) && Arrays.equals(blobComponent, other.blobComponent);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ componentVARLONG, componentVARSIGNEDLONG, componentFIXEDLONG, componentFIXEDLONGLITTLEENDIAN, componentSHA256HASH, componentVARSTRING, componentSIZEDBLOB, componentNULLABLEFIXEDLONG, componentUUID, blobComponent });
        }

        @Override
        public int compareTo(AllValueTypesTestRow o) {
            return ComparisonChain.start()
                .compare(this.componentVARLONG, o.componentVARLONG)
                .compare(this.componentVARSIGNEDLONG, o.componentVARSIGNEDLONG)
                .compare(this.componentFIXEDLONG, o.componentFIXEDLONG)
                .compare(this.componentFIXEDLONGLITTLEENDIAN, o.componentFIXEDLONGLITTLEENDIAN)
                .compare(this.componentSHA256HASH, o.componentSHA256HASH)
                .compare(this.componentVARSTRING, o.componentVARSTRING)
                .compare(this.componentSIZEDBLOB, o.componentSIZEDBLOB, UnsignedBytes.lexicographicalComparator())
                .compare(this.componentNULLABLEFIXEDLONG, o.componentNULLABLEFIXEDLONG)
                .compare(this.componentUUID, o.componentUUID)
                .compare(this.blobComponent, o.blobComponent, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface AllValueTypesTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class ColumnBLOB implements AllValueTypesTestNamedColumnValue<byte[]> {
        private final byte[] value;

        public static ColumnBLOB of(byte[] value) {
            return new ColumnBLOB(value);
        }

        private ColumnBLOB(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnBLOB";
        }

        @Override
        public String getShortColumnName() {
            return "cBLOB";
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
            return PtBytes.toCachedBytes("cBLOB");
        }

        public static final Hydrator<ColumnBLOB> BYTES_HYDRATOR = new Hydrator<ColumnBLOB>() {
            @Override
            public ColumnBLOB hydrateFromBytes(byte[] bytes) {
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

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class ColumnFIXEDLONG implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static ColumnFIXEDLONG of(Long value) {
            return new ColumnFIXEDLONG(value);
        }

        private ColumnFIXEDLONG(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnFIXED_LONG";
        }

        @Override
        public String getShortColumnName() {
            return "cFIXED_LONG";
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
            return PtBytes.toCachedBytes("cFIXED_LONG");
        }

        public static final Hydrator<ColumnFIXEDLONG> BYTES_HYDRATOR = new Hydrator<ColumnFIXEDLONG>() {
            @Override
            public ColumnFIXEDLONG hydrateFromBytes(byte[] bytes) {
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

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class ColumnFIXEDLONGLITTLEENDIAN implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static ColumnFIXEDLONGLITTLEENDIAN of(Long value) {
            return new ColumnFIXEDLONGLITTLEENDIAN(value);
        }

        private ColumnFIXEDLONGLITTLEENDIAN(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnFIXED_LONG_LITTLE_ENDIAN";
        }

        @Override
        public String getShortColumnName() {
            return "cFIXED_LONG_LITTLE_ENDIAN";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeLittleEndian(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN");
        }

        public static final Hydrator<ColumnFIXEDLONGLITTLEENDIAN> BYTES_HYDRATOR = new Hydrator<ColumnFIXEDLONGLITTLEENDIAN>() {
            @Override
            public ColumnFIXEDLONGLITTLEENDIAN hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeLittleEndian(bytes, 0));
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
    public static final class ColumnNULLABLEFIXEDLONG implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static ColumnNULLABLEFIXEDLONG of(Long value) {
            return new ColumnNULLABLEFIXEDLONG(value);
        }

        private ColumnNULLABLEFIXEDLONG(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnNULLABLE_FIXED_LONG";
        }

        @Override
        public String getShortColumnName() {
            return "cNULLABLE_FIXED_LONG";
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeNullableFixedLong(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG");
        }

        public static final Hydrator<ColumnNULLABLEFIXEDLONG> BYTES_HYDRATOR = new Hydrator<ColumnNULLABLEFIXEDLONG>() {
            @Override
            public ColumnNULLABLEFIXEDLONG hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeNullableFixedLong(bytes,0));
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
     *   type: Sha256Hash;
     * }
     * </pre>
     */
    public static final class ColumnSHA256HASH implements AllValueTypesTestNamedColumnValue<Sha256Hash> {
        private final Sha256Hash value;

        public static ColumnSHA256HASH of(Sha256Hash value) {
            return new ColumnSHA256HASH(value);
        }

        private ColumnSHA256HASH(Sha256Hash value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnSHA256HASH";
        }

        @Override
        public String getShortColumnName() {
            return "cSHA256HASH";
        }

        @Override
        public Sha256Hash getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = value.getBytes();
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cSHA256HASH");
        }

        public static final Hydrator<ColumnSHA256HASH> BYTES_HYDRATOR = new Hydrator<ColumnSHA256HASH>() {
            @Override
            public ColumnSHA256HASH hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(new Sha256Hash(EncodingUtils.get32Bytes(bytes, 0)));
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
    public static final class ColumnSIZEDBLOB implements AllValueTypesTestNamedColumnValue<byte[]> {
        private final byte[] value;

        public static ColumnSIZEDBLOB of(byte[] value) {
            return new ColumnSIZEDBLOB(value);
        }

        private ColumnSIZEDBLOB(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnSIZED_BLOB";
        }

        @Override
        public String getShortColumnName() {
            return "cSIZED_BLOB";
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeSizedBytes(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cSIZED_BLOB");
        }

        public static final Hydrator<ColumnSIZEDBLOB> BYTES_HYDRATOR = new Hydrator<ColumnSIZEDBLOB>() {
            @Override
            public ColumnSIZEDBLOB hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeSizedBytes(bytes, 0));
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
    public static final class ColumnSTRING implements AllValueTypesTestNamedColumnValue<String> {
        private final String value;

        public static ColumnSTRING of(String value) {
            return new ColumnSTRING(value);
        }

        private ColumnSTRING(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnSTRING";
        }

        @Override
        public String getShortColumnName() {
            return "cSTRING";
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
            return PtBytes.toCachedBytes("cSTRING");
        }

        public static final Hydrator<ColumnSTRING> BYTES_HYDRATOR = new Hydrator<ColumnSTRING>() {
            @Override
            public ColumnSTRING hydrateFromBytes(byte[] bytes) {
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
     *   type: UUID;
     * }
     * </pre>
     */
    public static final class ColumnUUID implements AllValueTypesTestNamedColumnValue<UUID> {
        private final UUID value;

        public static ColumnUUID of(UUID value) {
            return new ColumnUUID(value);
        }

        private ColumnUUID(UUID value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnUUID";
        }

        @Override
        public String getShortColumnName() {
            return "cUUID";
        }

        @Override
        public UUID getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeUUID(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cUUID");
        }

        public static final Hydrator<ColumnUUID> BYTES_HYDRATOR = new Hydrator<ColumnUUID>() {
            @Override
            public ColumnUUID hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeUUID(bytes, 0));
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
    public static final class ColumnVARLONG implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static ColumnVARLONG of(Long value) {
            return new ColumnVARLONG(value);
        }

        private ColumnVARLONG(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnVAR_LONG";
        }

        @Override
        public String getShortColumnName() {
            return "cVAR_LONG";
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
            return PtBytes.toCachedBytes("cVAR_LONG");
        }

        public static final Hydrator<ColumnVARLONG> BYTES_HYDRATOR = new Hydrator<ColumnVARLONG>() {
            @Override
            public ColumnVARLONG hydrateFromBytes(byte[] bytes) {
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
    public static final class ColumnVARSIGNEDLONG implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static ColumnVARSIGNEDLONG of(Long value) {
            return new ColumnVARSIGNEDLONG(value);
        }

        private ColumnVARSIGNEDLONG(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnVAR_SIGNED_LONG";
        }

        @Override
        public String getShortColumnName() {
            return "cVAR_SIGNED_LONG";
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
            return PtBytes.toCachedBytes("cVAR_SIGNED_LONG");
        }

        public static final Hydrator<ColumnVARSIGNEDLONG> BYTES_HYDRATOR = new Hydrator<ColumnVARSIGNEDLONG>() {
            @Override
            public ColumnVARSIGNEDLONG hydrateFromBytes(byte[] bytes) {
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
     *   type: String;
     * }
     * </pre>
     */
    public static final class ColumnVARSTRING implements AllValueTypesTestNamedColumnValue<String> {
        private final String value;

        public static ColumnVARSTRING of(String value) {
            return new ColumnVARSTRING(value);
        }

        private ColumnVARSTRING(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "columnVAR_STRING";
        }

        @Override
        public String getShortColumnName() {
            return "cVAR_STRING";
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public byte[] persistValue() {
            byte[] bytes = EncodingUtils.encodeVarString(value);
            return CompressionUtils.compress(bytes, Compression.NONE);
        }

        @Override
        public byte[] persistColumnName() {
            return PtBytes.toCachedBytes("cVAR_STRING");
        }

        public static final Hydrator<ColumnVARSTRING> BYTES_HYDRATOR = new Hydrator<ColumnVARSTRING>() {
            @Override
            public ColumnVARSTRING hydrateFromBytes(byte[] bytes) {
                bytes = CompressionUtils.decompress(bytes, Compression.NONE);
                return of(EncodingUtils.decodeVarString(bytes, 0));
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("Value", this.value)
                .toString();
        }
    }

    public interface AllValueTypesTestTrigger {
        public void putAllValueTypesTest(Multimap<AllValueTypesTestRow, ? extends AllValueTypesTestNamedColumnValue<?>> newRows);
    }

    public static final class AllValueTypesTestRowResult implements TypedRowResult {
        private final RowResult<byte[]> row;

        public static AllValueTypesTestRowResult of(RowResult<byte[]> row) {
            return new AllValueTypesTestRowResult(row);
        }

        private AllValueTypesTestRowResult(RowResult<byte[]> row) {
            this.row = row;
        }

        @Override
        public AllValueTypesTestRow getRowName() {
            return AllValueTypesTestRow.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());
        }

        public static Function<AllValueTypesTestRowResult, AllValueTypesTestRow> getRowNameFun() {
            return new Function<AllValueTypesTestRowResult, AllValueTypesTestRow>() {
                @Override
                public AllValueTypesTestRow apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getRowName();
                }
            };
        }

        public static Function<RowResult<byte[]>, AllValueTypesTestRowResult> fromRawRowResultFun() {
            return new Function<RowResult<byte[]>, AllValueTypesTestRowResult>() {
                @Override
                public AllValueTypesTestRowResult apply(RowResult<byte[]> rowResult) {
                    return new AllValueTypesTestRowResult(rowResult);
                }
            };
        }

        public boolean hasColumnBLOB() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cBLOB"));
        }

        public boolean hasColumnFIXEDLONG() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cFIXED_LONG"));
        }

        public boolean hasColumnFIXEDLONGLITTLEENDIAN() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN"));
        }

        public boolean hasColumnNULLABLEFIXEDLONG() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG"));
        }

        public boolean hasColumnSHA256HASH() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cSHA256HASH"));
        }

        public boolean hasColumnSIZEDBLOB() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cSIZED_BLOB"));
        }

        public boolean hasColumnSTRING() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cSTRING"));
        }

        public boolean hasColumnUUID() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cUUID"));
        }

        public boolean hasColumnVARLONG() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cVAR_LONG"));
        }

        public boolean hasColumnVARSIGNEDLONG() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cVAR_SIGNED_LONG"));
        }

        public boolean hasColumnVARSTRING() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("cVAR_STRING"));
        }

        public byte[] getColumnBLOB() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cBLOB"));
            if (bytes == null) {
                return null;
            }
            ColumnBLOB value = ColumnBLOB.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumnFIXEDLONG() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cFIXED_LONG"));
            if (bytes == null) {
                return null;
            }
            ColumnFIXEDLONG value = ColumnFIXEDLONG.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumnFIXEDLONGLITTLEENDIAN() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN"));
            if (bytes == null) {
                return null;
            }
            ColumnFIXEDLONGLITTLEENDIAN value = ColumnFIXEDLONGLITTLEENDIAN.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumnNULLABLEFIXEDLONG() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG"));
            if (bytes == null) {
                return null;
            }
            ColumnNULLABLEFIXEDLONG value = ColumnNULLABLEFIXEDLONG.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Sha256Hash getColumnSHA256HASH() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cSHA256HASH"));
            if (bytes == null) {
                return null;
            }
            ColumnSHA256HASH value = ColumnSHA256HASH.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public byte[] getColumnSIZEDBLOB() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cSIZED_BLOB"));
            if (bytes == null) {
                return null;
            }
            ColumnSIZEDBLOB value = ColumnSIZEDBLOB.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public String getColumnSTRING() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cSTRING"));
            if (bytes == null) {
                return null;
            }
            ColumnSTRING value = ColumnSTRING.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public UUID getColumnUUID() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cUUID"));
            if (bytes == null) {
                return null;
            }
            ColumnUUID value = ColumnUUID.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumnVARLONG() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cVAR_LONG"));
            if (bytes == null) {
                return null;
            }
            ColumnVARLONG value = ColumnVARLONG.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumnVARSIGNEDLONG() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cVAR_SIGNED_LONG"));
            if (bytes == null) {
                return null;
            }
            ColumnVARSIGNEDLONG value = ColumnVARSIGNEDLONG.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public String getColumnVARSTRING() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("cVAR_STRING"));
            if (bytes == null) {
                return null;
            }
            ColumnVARSTRING value = ColumnVARSTRING.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<AllValueTypesTestRowResult, byte[]> getColumnBLOBFun() {
            return new Function<AllValueTypesTestRowResult, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnBLOB();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumnFIXEDLONGFun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnFIXEDLONG();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumnFIXEDLONGLITTLEENDIANFun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnFIXEDLONGLITTLEENDIAN();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumnNULLABLEFIXEDLONGFun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnNULLABLEFIXEDLONG();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Sha256Hash> getColumnSHA256HASHFun() {
            return new Function<AllValueTypesTestRowResult, Sha256Hash>() {
                @Override
                public Sha256Hash apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnSHA256HASH();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, byte[]> getColumnSIZEDBLOBFun() {
            return new Function<AllValueTypesTestRowResult, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnSIZEDBLOB();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, String> getColumnSTRINGFun() {
            return new Function<AllValueTypesTestRowResult, String>() {
                @Override
                public String apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnSTRING();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, UUID> getColumnUUIDFun() {
            return new Function<AllValueTypesTestRowResult, UUID>() {
                @Override
                public UUID apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnUUID();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumnVARLONGFun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnVARLONG();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumnVARSIGNEDLONGFun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnVARSIGNEDLONG();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, String> getColumnVARSTRINGFun() {
            return new Function<AllValueTypesTestRowResult, String>() {
                @Override
                public String apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumnVARSTRING();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("ColumnBLOB", getColumnBLOB())
                .add("ColumnFIXEDLONG", getColumnFIXEDLONG())
                .add("ColumnFIXEDLONGLITTLEENDIAN", getColumnFIXEDLONGLITTLEENDIAN())
                .add("ColumnNULLABLEFIXEDLONG", getColumnNULLABLEFIXEDLONG())
                .add("ColumnSHA256HASH", getColumnSHA256HASH())
                .add("ColumnSIZEDBLOB", getColumnSIZEDBLOB())
                .add("ColumnSTRING", getColumnSTRING())
                .add("ColumnUUID", getColumnUUID())
                .add("ColumnVARLONG", getColumnVARLONG())
                .add("ColumnVARSIGNEDLONG", getColumnVARSIGNEDLONG())
                .add("ColumnVARSTRING", getColumnVARSTRING())
                .toString();
        }
    }

    public enum AllValueTypesTestNamedColumn {
        COLUMN_B_L_O_B {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cBLOB");
            }
        },
        COLUMN_F_I_X_E_D__L_O_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cFIXED_LONG");
            }
        },
        COLUMN_F_I_X_E_D__L_O_N_G__L_I_T_T_L_E__E_N_D_I_A_N {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN");
            }
        },
        COLUMN_N_U_L_L_A_B_L_E__F_I_X_E_D__L_O_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG");
            }
        },
        COLUMN_S_H_A256_H_A_S_H {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cSHA256HASH");
            }
        },
        COLUMN_S_I_Z_E_D__B_L_O_B {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cSIZED_BLOB");
            }
        },
        COLUMN_S_T_R_I_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cSTRING");
            }
        },
        COLUMN_U_U_I_D {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cUUID");
            }
        },
        COLUMN_V_A_R__L_O_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cVAR_LONG");
            }
        },
        COLUMN_V_A_R__S_I_G_N_E_D__L_O_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cVAR_SIGNED_LONG");
            }
        },
        COLUMN_V_A_R__S_T_R_I_N_G {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("cVAR_STRING");
            }
        };

        public abstract byte[] getShortName();

        public static Function<AllValueTypesTestNamedColumn, byte[]> toShortName() {
            return new Function<AllValueTypesTestNamedColumn, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestNamedColumn namedColumn) {
                    return namedColumn.getShortName();
                }
            };
        }
    }

    public static ColumnSelection getColumnSelection(Collection<AllValueTypesTestNamedColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, AllValueTypesTestNamedColumn.toShortName()));
    }

    public static ColumnSelection getColumnSelection(AllValueTypesTestNamedColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    private static final Map<String, Hydrator<? extends AllValueTypesTestNamedColumnValue<?>>> shortNameToHydrator =
            ImmutableMap.<String, Hydrator<? extends AllValueTypesTestNamedColumnValue<?>>>builder()
                .put("cVAR_LONG", ColumnVARLONG.BYTES_HYDRATOR)
                .put("cVAR_SIGNED_LONG", ColumnVARSIGNEDLONG.BYTES_HYDRATOR)
                .put("cFIXED_LONG", ColumnFIXEDLONG.BYTES_HYDRATOR)
                .put("cFIXED_LONG_LITTLE_ENDIAN", ColumnFIXEDLONGLITTLEENDIAN.BYTES_HYDRATOR)
                .put("cSHA256HASH", ColumnSHA256HASH.BYTES_HYDRATOR)
                .put("cVAR_STRING", ColumnVARSTRING.BYTES_HYDRATOR)
                .put("cSTRING", ColumnSTRING.BYTES_HYDRATOR)
                .put("cBLOB", ColumnBLOB.BYTES_HYDRATOR)
                .put("cSIZED_BLOB", ColumnSIZEDBLOB.BYTES_HYDRATOR)
                .put("cNULLABLE_FIXED_LONG", ColumnNULLABLEFIXEDLONG.BYTES_HYDRATOR)
                .put("cUUID", ColumnUUID.BYTES_HYDRATOR)
                .build();

    public Map<AllValueTypesTestRow, Long> getColumnVARLONGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cVAR_LONG")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = ColumnVARLONG.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumnVARSIGNEDLONGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cVAR_SIGNED_LONG")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = ColumnVARSIGNEDLONG.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumnFIXEDLONGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cFIXED_LONG")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = ColumnFIXEDLONG.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumnFIXEDLONGLITTLEENDIANs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = ColumnFIXEDLONGLITTLEENDIAN.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Sha256Hash> getColumnSHA256HASHs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cSHA256HASH")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Sha256Hash> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Sha256Hash val = ColumnSHA256HASH.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, String> getColumnVARSTRINGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cVAR_STRING")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = ColumnVARSTRING.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, String> getColumnSTRINGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cSTRING")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = ColumnSTRING.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, byte[]> getColumnBLOBs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cBLOB")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = ColumnBLOB.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, byte[]> getColumnSIZEDBLOBs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cSIZED_BLOB")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = ColumnSIZEDBLOB.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumnNULLABLEFIXEDLONGs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = ColumnNULLABLEFIXEDLONG.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, UUID> getColumnUUIDs(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("cUUID")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, UUID> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            UUID val = ColumnUUID.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumnVARLONG(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, ColumnVARLONG.of(value)));
    }

    public void putColumnVARLONG(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnVARLONG.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnVARSIGNEDLONG(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, ColumnVARSIGNEDLONG.of(value)));
    }

    public void putColumnVARSIGNEDLONG(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnVARSIGNEDLONG.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnFIXEDLONG(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, ColumnFIXEDLONG.of(value)));
    }

    public void putColumnFIXEDLONG(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnFIXEDLONG.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnFIXEDLONGLITTLEENDIAN(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, ColumnFIXEDLONGLITTLEENDIAN.of(value)));
    }

    public void putColumnFIXEDLONGLITTLEENDIAN(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnFIXEDLONGLITTLEENDIAN.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnSHA256HASH(AllValueTypesTestRow row, Sha256Hash value) {
        put(ImmutableMultimap.of(row, ColumnSHA256HASH.of(value)));
    }

    public void putColumnSHA256HASH(Map<AllValueTypesTestRow, Sha256Hash> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Sha256Hash> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnSHA256HASH.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnVARSTRING(AllValueTypesTestRow row, String value) {
        put(ImmutableMultimap.of(row, ColumnVARSTRING.of(value)));
    }

    public void putColumnVARSTRING(Map<AllValueTypesTestRow, String> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnVARSTRING.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnSTRING(AllValueTypesTestRow row, String value) {
        put(ImmutableMultimap.of(row, ColumnSTRING.of(value)));
    }

    public void putColumnSTRING(Map<AllValueTypesTestRow, String> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnSTRING.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnBLOB(AllValueTypesTestRow row, byte[] value) {
        put(ImmutableMultimap.of(row, ColumnBLOB.of(value)));
    }

    public void putColumnBLOB(Map<AllValueTypesTestRow, byte[]> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnBLOB.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnSIZEDBLOB(AllValueTypesTestRow row, byte[] value) {
        put(ImmutableMultimap.of(row, ColumnSIZEDBLOB.of(value)));
    }

    public void putColumnSIZEDBLOB(Map<AllValueTypesTestRow, byte[]> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnSIZEDBLOB.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnNULLABLEFIXEDLONG(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, ColumnNULLABLEFIXEDLONG.of(value)));
    }

    public void putColumnNULLABLEFIXEDLONG(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnNULLABLEFIXEDLONG.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumnUUID(AllValueTypesTestRow row, UUID value) {
        put(ImmutableMultimap.of(row, ColumnUUID.of(value)));
    }

    public void putColumnUUID(Map<AllValueTypesTestRow, UUID> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, UUID> e : map.entrySet()) {
            toPut.put(e.getKey(), ColumnUUID.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    @Override
    public void put(Multimap<AllValueTypesTestRow, ? extends AllValueTypesTestNamedColumnValue<?>> rows) {
        t.useTable(tableRef, this);
        t.put(tableRef, ColumnValues.toCellValues(rows));
        for (AllValueTypesTestTrigger trigger : triggers) {
            trigger.putAllValueTypesTest(rows);
        }
    }

    public void deleteColumnVARLONG(AllValueTypesTestRow row) {
        deleteColumnVARLONG(ImmutableSet.of(row));
    }

    public void deleteColumnVARLONG(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cVAR_LONG");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnVARSIGNEDLONG(AllValueTypesTestRow row) {
        deleteColumnVARSIGNEDLONG(ImmutableSet.of(row));
    }

    public void deleteColumnVARSIGNEDLONG(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cVAR_SIGNED_LONG");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnFIXEDLONG(AllValueTypesTestRow row) {
        deleteColumnFIXEDLONG(ImmutableSet.of(row));
    }

    public void deleteColumnFIXEDLONG(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cFIXED_LONG");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnFIXEDLONGLITTLEENDIAN(AllValueTypesTestRow row) {
        deleteColumnFIXEDLONGLITTLEENDIAN(ImmutableSet.of(row));
    }

    public void deleteColumnFIXEDLONGLITTLEENDIAN(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnSHA256HASH(AllValueTypesTestRow row) {
        deleteColumnSHA256HASH(ImmutableSet.of(row));
    }

    public void deleteColumnSHA256HASH(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cSHA256HASH");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnVARSTRING(AllValueTypesTestRow row) {
        deleteColumnVARSTRING(ImmutableSet.of(row));
    }

    public void deleteColumnVARSTRING(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cVAR_STRING");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnSTRING(AllValueTypesTestRow row) {
        deleteColumnSTRING(ImmutableSet.of(row));
    }

    public void deleteColumnSTRING(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cSTRING");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnBLOB(AllValueTypesTestRow row) {
        deleteColumnBLOB(ImmutableSet.of(row));
    }

    public void deleteColumnBLOB(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cBLOB");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnSIZEDBLOB(AllValueTypesTestRow row) {
        deleteColumnSIZEDBLOB(ImmutableSet.of(row));
    }

    public void deleteColumnSIZEDBLOB(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cSIZED_BLOB");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnNULLABLEFIXEDLONG(AllValueTypesTestRow row) {
        deleteColumnNULLABLEFIXEDLONG(ImmutableSet.of(row));
    }

    public void deleteColumnNULLABLEFIXEDLONG(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumnUUID(AllValueTypesTestRow row) {
        deleteColumnUUID(ImmutableSet.of(row));
    }

    public void deleteColumnUUID(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("cUUID");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    @Override
    public void delete(AllValueTypesTestRow row) {
        delete(ImmutableSet.of(row));
    }

    @Override
    public void delete(Iterable<AllValueTypesTestRow> rows) {
        List<byte[]> rowBytes = Persistables.persistAll(rows);
        Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size() * 11);
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cBLOB")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cFIXED_LONG")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cFIXED_LONG_LITTLE_ENDIAN")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cNULLABLE_FIXED_LONG")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cSHA256HASH")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cSIZED_BLOB")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cSTRING")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cUUID")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cVAR_LONG")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cVAR_SIGNED_LONG")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("cVAR_STRING")));
        t.delete(tableRef, cells);
    }

    public Optional<AllValueTypesTestRowResult> getRow(AllValueTypesTestRow row) {
        return getRow(row, allColumns);
    }

    public Optional<AllValueTypesTestRowResult> getRow(AllValueTypesTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(AllValueTypesTestRowResult.of(rowResult));
        }
    }

    @Override
    public List<AllValueTypesTestRowResult> getRows(Iterable<AllValueTypesTestRow> rows) {
        return getRows(rows, allColumns);
    }

    @Override
    public List<AllValueTypesTestRowResult> getRows(Iterable<AllValueTypesTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        List<AllValueTypesTestRowResult> rowResults = Lists.newArrayListWithCapacity(results.size());
        for (RowResult<byte[]> row : results.values()) {
            rowResults.add(AllValueTypesTestRowResult.of(row));
        }
        return rowResults;
    }

    @Override
    public List<AllValueTypesTestNamedColumnValue<?>> getRowColumns(AllValueTypesTestRow row) {
        return getRowColumns(row, allColumns);
    }

    @Override
    public List<AllValueTypesTestNamedColumnValue<?>> getRowColumns(AllValueTypesTestRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<AllValueTypesTestNamedColumnValue<?>> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
            return ret;
        }
    }

    @Override
    public Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> getRowsMultimap(Iterable<AllValueTypesTestRow> rows) {
        return getRowsMultimapInternal(rows, allColumns);
    }

    @Override
    public Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> getRowsMultimap(Iterable<AllValueTypesTestRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    private Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> getRowsMultimapInternal(Iterable<AllValueTypesTestRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> rowMap = ArrayListMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            AllValueTypesTestRow row = AllValueTypesTestRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));
            }
        }
        return rowMap;
    }

    @Override
    public Map<AllValueTypesTestRow, BatchingVisitable<AllValueTypesTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<AllValueTypesTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<AllValueTypesTestRow, BatchingVisitable<AllValueTypesTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            AllValueTypesTestRow row = AllValueTypesTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            BatchingVisitable<AllValueTypesTestNamedColumnValue<?>> bv = BatchingVisitables.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
            });
            transformed.put(row, bv);
        }
        return transformed;
    }

    @Override
    public Iterator<Map.Entry<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>>> getRowsColumnRange(Iterable<AllValueTypesTestRow> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(), Persistables.persistAll(rows), columnRangeSelection, batchHint);
        return Iterators.transform(results, e -> {
            AllValueTypesTestRow row = AllValueTypesTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
            AllValueTypesTestNamedColumnValue<?> colValue = shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());
            return Maps.immutableEntry(row, colValue);
        });
    }

    @Override
    public Map<AllValueTypesTestRow, Iterator<AllValueTypesTestNamedColumnValue<?>>> getRowsColumnRangeIterator(Iterable<AllValueTypesTestRow> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows), columnRangeSelection);
        Map<AllValueTypesTestRow, Iterator<AllValueTypesTestNamedColumnValue<?>>> transformed = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {
            AllValueTypesTestRow row = AllValueTypesTestRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
            Iterator<AllValueTypesTestNamedColumnValue<?>> bv = Iterators.transform(e.getValue(), result -> {
                return shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());
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

    public BatchingVisitableView<AllValueTypesTestRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(allColumns);
    }

    public BatchingVisitableView<AllValueTypesTestRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()
                .retainColumns(optimizeColumnSelection(columns)).build()),
                new Function<RowResult<byte[]>, AllValueTypesTestRowResult>() {
            @Override
            public AllValueTypesTestRowResult apply(RowResult<byte[]> input) {
                return AllValueTypesTestRowResult.of(input);
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
     * {@link Nullable}
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
    static String __CLASS_HASH = "8h+42FefKe+9M/lArw1DGg==";
}
