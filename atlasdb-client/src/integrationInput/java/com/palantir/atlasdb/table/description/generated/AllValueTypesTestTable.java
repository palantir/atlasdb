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
     *   {@literal Long component0};
     *   {@literal Long component1};
     *   {@literal Long component2};
     *   {@literal Long component3};
     *   {@literal Sha256Hash component4};
     *   {@literal String component5};
     *   {@literal byte[] component8};
     *   {@literal Long component9};
     *   {@literal UUID component10};
     *   {@literal byte[] blobComponent};
     * }
     * </pre>
     */
    public static final class AllValueTypesTestRow implements Persistable, Comparable<AllValueTypesTestRow> {
        private final long component0;
        private final long component1;
        private final long component2;
        private final long component3;
        private final Sha256Hash component4;
        private final String component5;
        private final byte[] component8;
        private final Long component9;
        private final UUID component10;
        private final byte[] blobComponent;

        public static AllValueTypesTestRow of(long component0, long component1, long component2, long component3, Sha256Hash component4, String component5, byte[] component8, Long component9, UUID component10, byte[] blobComponent) {
            return new AllValueTypesTestRow(component0, component1, component2, component3, component4, component5, component8, component9, component10, blobComponent);
        }

        private AllValueTypesTestRow(long component0, long component1, long component2, long component3, Sha256Hash component4, String component5, byte[] component8, Long component9, UUID component10, byte[] blobComponent) {
            this.component0 = component0;
            this.component1 = component1;
            this.component2 = component2;
            this.component3 = component3;
            this.component4 = component4;
            this.component5 = component5;
            this.component8 = component8;
            this.component9 = component9;
            this.component10 = component10;
            this.blobComponent = blobComponent;
        }

        public long getComponent0() {
            return component0;
        }

        public long getComponent1() {
            return component1;
        }

        public long getComponent2() {
            return component2;
        }

        public long getComponent3() {
            return component3;
        }

        public Sha256Hash getComponent4() {
            return component4;
        }

        public String getComponent5() {
            return component5;
        }

        public byte[] getComponent8() {
            return component8;
        }

        public Long getComponent9() {
            return component9;
        }

        public UUID getComponent10() {
            return component10;
        }

        public byte[] getBlobComponent() {
            return blobComponent;
        }

        public static Function<AllValueTypesTestRow, Long> getComponent0Fun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.component0;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponent1Fun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.component1;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponent2Fun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.component2;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponent3Fun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.component3;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Sha256Hash> getComponent4Fun() {
            return new Function<AllValueTypesTestRow, Sha256Hash>() {
                @Override
                public Sha256Hash apply(AllValueTypesTestRow row) {
                    return row.component4;
                }
            };
        }

        public static Function<AllValueTypesTestRow, String> getComponent5Fun() {
            return new Function<AllValueTypesTestRow, String>() {
                @Override
                public String apply(AllValueTypesTestRow row) {
                    return row.component5;
                }
            };
        }

        public static Function<AllValueTypesTestRow, byte[]> getComponent8Fun() {
            return new Function<AllValueTypesTestRow, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRow row) {
                    return row.component8;
                }
            };
        }

        public static Function<AllValueTypesTestRow, Long> getComponent9Fun() {
            return new Function<AllValueTypesTestRow, Long>() {
                @Override
                public Long apply(AllValueTypesTestRow row) {
                    return row.component9;
                }
            };
        }

        public static Function<AllValueTypesTestRow, UUID> getComponent10Fun() {
            return new Function<AllValueTypesTestRow, UUID>() {
                @Override
                public UUID apply(AllValueTypesTestRow row) {
                    return row.component10;
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
            byte[] component0Bytes = EncodingUtils.encodeUnsignedVarLong(component0);
            byte[] component1Bytes = EncodingUtils.encodeSignedVarLong(component1);
            byte[] component2Bytes = PtBytes.toBytes(Long.MIN_VALUE ^ component2);
            byte[] component3Bytes = EncodingUtils.encodeLittleEndian(component3);
            byte[] component4Bytes = component4.getBytes();
            byte[] component5Bytes = EncodingUtils.encodeVarString(component5);
            byte[] component8Bytes = EncodingUtils.encodeSizedBytes(component8);
            byte[] component9Bytes = EncodingUtils.encodeNullableFixedLong(component9);
            byte[] component10Bytes = EncodingUtils.encodeUUID(component10);
            byte[] blobComponentBytes = blobComponent;
            return EncodingUtils.add(component0Bytes, component1Bytes, component2Bytes, component3Bytes, component4Bytes, component5Bytes, component8Bytes, component9Bytes, component10Bytes, blobComponentBytes);
        }

        public static final Hydrator<AllValueTypesTestRow> BYTES_HYDRATOR = new Hydrator<AllValueTypesTestRow>() {
            @Override
            public AllValueTypesTestRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long component0 = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(component0);
                Long component1 = EncodingUtils.decodeSignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfSignedVarLong(component1);
                Long component2 = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
                __index += 8;
                Long component3 = EncodingUtils.decodeLittleEndian(__input, __index);
                __index += 8;
                Sha256Hash component4 = new Sha256Hash(EncodingUtils.get32Bytes(__input, __index));
                __index += 32;
                String component5 = EncodingUtils.decodeVarString(__input, __index);
                __index += EncodingUtils.sizeOfVarString(component5);
                byte[] component8 = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(component8);
                Long component9 = EncodingUtils.decodeNullableFixedLong(__input,__index);
                __index += 9;
                UUID component10 = EncodingUtils.decodeUUID(__input, __index);
                __index += 16;
                byte[] blobComponent = EncodingUtils.getBytesFromOffsetToEnd(__input, __index);
                __index += 0;
                return new AllValueTypesTestRow(component0, component1, component2, component3, component4, component5, component8, component9, component10, blobComponent);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("component0", component0)
                .add("component1", component1)
                .add("component2", component2)
                .add("component3", component3)
                .add("component4", component4)
                .add("component5", component5)
                .add("component8", component8)
                .add("component9", component9)
                .add("component10", component10)
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
            return Objects.equals(component0, other.component0) && Objects.equals(component1, other.component1) && Objects.equals(component2, other.component2) && Objects.equals(component3, other.component3) && Objects.equals(component4, other.component4) && Objects.equals(component5, other.component5) && Arrays.equals(component8, other.component8) && Objects.equals(component9, other.component9) && Objects.equals(component10, other.component10) && Arrays.equals(blobComponent, other.blobComponent);
        }

        @SuppressWarnings("ArrayHashCode")
        @Override
        public int hashCode() {
            return Arrays.deepHashCode(new Object[]{ component0, component1, component2, component3, component4, component5, component8, component9, component10, blobComponent });
        }

        @Override
        public int compareTo(AllValueTypesTestRow o) {
            return ComparisonChain.start()
                .compare(this.component0, o.component0)
                .compare(this.component1, o.component1)
                .compare(this.component2, o.component2)
                .compare(this.component3, o.component3)
                .compare(this.component4, o.component4)
                .compare(this.component5, o.component5)
                .compare(this.component8, o.component8, UnsignedBytes.lexicographicalComparator())
                .compare(this.component9, o.component9)
                .compare(this.component10, o.component10)
                .compare(this.blobComponent, o.blobComponent, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface AllValueTypesTestNamedColumnValue<T> extends NamedColumnValue<T> { /* */ }

    /**
     * <pre>
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column0 implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static Column0 of(Long value) {
            return new Column0(value);
        }

        private Column0(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column0";
        }

        @Override
        public String getShortColumnName() {
            return "c0";
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
            return PtBytes.toCachedBytes("c0");
        }

        public static final Hydrator<Column0> BYTES_HYDRATOR = new Hydrator<Column0>() {
            @Override
            public Column0 hydrateFromBytes(byte[] bytes) {
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
    public static final class Column1 implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static Column1 of(Long value) {
            return new Column1(value);
        }

        private Column1(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column1";
        }

        @Override
        public String getShortColumnName() {
            return "c1";
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
            return PtBytes.toCachedBytes("c1");
        }

        public static final Hydrator<Column1> BYTES_HYDRATOR = new Hydrator<Column1>() {
            @Override
            public Column1 hydrateFromBytes(byte[] bytes) {
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
     *   type: UUID;
     * }
     * </pre>
     */
    public static final class Column10 implements AllValueTypesTestNamedColumnValue<UUID> {
        private final UUID value;

        public static Column10 of(UUID value) {
            return new Column10(value);
        }

        private Column10(UUID value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column10";
        }

        @Override
        public String getShortColumnName() {
            return "c10";
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
            return PtBytes.toCachedBytes("c10");
        }

        public static final Hydrator<Column10> BYTES_HYDRATOR = new Hydrator<Column10>() {
            @Override
            public Column10 hydrateFromBytes(byte[] bytes) {
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
    public static final class Column2 implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static Column2 of(Long value) {
            return new Column2(value);
        }

        private Column2(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column2";
        }

        @Override
        public String getShortColumnName() {
            return "c2";
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
            return PtBytes.toCachedBytes("c2");
        }

        public static final Hydrator<Column2> BYTES_HYDRATOR = new Hydrator<Column2>() {
            @Override
            public Column2 hydrateFromBytes(byte[] bytes) {
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
    public static final class Column3 implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static Column3 of(Long value) {
            return new Column3(value);
        }

        private Column3(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column3";
        }

        @Override
        public String getShortColumnName() {
            return "c3";
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
            return PtBytes.toCachedBytes("c3");
        }

        public static final Hydrator<Column3> BYTES_HYDRATOR = new Hydrator<Column3>() {
            @Override
            public Column3 hydrateFromBytes(byte[] bytes) {
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
     *   type: Sha256Hash;
     * }
     * </pre>
     */
    public static final class Column4 implements AllValueTypesTestNamedColumnValue<Sha256Hash> {
        private final Sha256Hash value;

        public static Column4 of(Sha256Hash value) {
            return new Column4(value);
        }

        private Column4(Sha256Hash value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column4";
        }

        @Override
        public String getShortColumnName() {
            return "c4";
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
            return PtBytes.toCachedBytes("c4");
        }

        public static final Hydrator<Column4> BYTES_HYDRATOR = new Hydrator<Column4>() {
            @Override
            public Column4 hydrateFromBytes(byte[] bytes) {
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
     *   type: String;
     * }
     * </pre>
     */
    public static final class Column5 implements AllValueTypesTestNamedColumnValue<String> {
        private final String value;

        public static Column5 of(String value) {
            return new Column5(value);
        }

        private Column5(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column5";
        }

        @Override
        public String getShortColumnName() {
            return "c5";
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
            return PtBytes.toCachedBytes("c5");
        }

        public static final Hydrator<Column5> BYTES_HYDRATOR = new Hydrator<Column5>() {
            @Override
            public Column5 hydrateFromBytes(byte[] bytes) {
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

    /**
     * <pre>
     * Column value description {
     *   type: String;
     * }
     * </pre>
     */
    public static final class Column6 implements AllValueTypesTestNamedColumnValue<String> {
        private final String value;

        public static Column6 of(String value) {
            return new Column6(value);
        }

        private Column6(String value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column6";
        }

        @Override
        public String getShortColumnName() {
            return "c6";
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
            return PtBytes.toCachedBytes("c6");
        }

        public static final Hydrator<Column6> BYTES_HYDRATOR = new Hydrator<Column6>() {
            @Override
            public Column6 hydrateFromBytes(byte[] bytes) {
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
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Column7 implements AllValueTypesTestNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Column7 of(byte[] value) {
            return new Column7(value);
        }

        private Column7(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column7";
        }

        @Override
        public String getShortColumnName() {
            return "c7";
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
            return PtBytes.toCachedBytes("c7");
        }

        public static final Hydrator<Column7> BYTES_HYDRATOR = new Hydrator<Column7>() {
            @Override
            public Column7 hydrateFromBytes(byte[] bytes) {
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
     *   type: byte[];
     * }
     * </pre>
     */
    public static final class Column8 implements AllValueTypesTestNamedColumnValue<byte[]> {
        private final byte[] value;

        public static Column8 of(byte[] value) {
            return new Column8(value);
        }

        private Column8(byte[] value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column8";
        }

        @Override
        public String getShortColumnName() {
            return "c8";
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
            return PtBytes.toCachedBytes("c8");
        }

        public static final Hydrator<Column8> BYTES_HYDRATOR = new Hydrator<Column8>() {
            @Override
            public Column8 hydrateFromBytes(byte[] bytes) {
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
     *   type: Long;
     * }
     * </pre>
     */
    public static final class Column9 implements AllValueTypesTestNamedColumnValue<Long> {
        private final Long value;

        public static Column9 of(Long value) {
            return new Column9(value);
        }

        private Column9(Long value) {
            this.value = value;
        }

        @Override
        public String getColumnName() {
            return "column9";
        }

        @Override
        public String getShortColumnName() {
            return "c9";
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
            return PtBytes.toCachedBytes("c9");
        }

        public static final Hydrator<Column9> BYTES_HYDRATOR = new Hydrator<Column9>() {
            @Override
            public Column9 hydrateFromBytes(byte[] bytes) {
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

        public boolean hasColumn0() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c0"));
        }

        public boolean hasColumn1() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c1"));
        }

        public boolean hasColumn10() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c10"));
        }

        public boolean hasColumn2() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c2"));
        }

        public boolean hasColumn3() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c3"));
        }

        public boolean hasColumn4() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c4"));
        }

        public boolean hasColumn5() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c5"));
        }

        public boolean hasColumn6() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c6"));
        }

        public boolean hasColumn7() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c7"));
        }

        public boolean hasColumn8() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c8"));
        }

        public boolean hasColumn9() {
            return row.getColumns().containsKey(PtBytes.toCachedBytes("c9"));
        }

        public Long getColumn0() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c0"));
            if (bytes == null) {
                return null;
            }
            Column0 value = Column0.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumn1() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c1"));
            if (bytes == null) {
                return null;
            }
            Column1 value = Column1.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public UUID getColumn10() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c10"));
            if (bytes == null) {
                return null;
            }
            Column10 value = Column10.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumn2() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c2"));
            if (bytes == null) {
                return null;
            }
            Column2 value = Column2.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumn3() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c3"));
            if (bytes == null) {
                return null;
            }
            Column3 value = Column3.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Sha256Hash getColumn4() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c4"));
            if (bytes == null) {
                return null;
            }
            Column4 value = Column4.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public String getColumn5() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c5"));
            if (bytes == null) {
                return null;
            }
            Column5 value = Column5.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public String getColumn6() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c6"));
            if (bytes == null) {
                return null;
            }
            Column6 value = Column6.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public byte[] getColumn7() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c7"));
            if (bytes == null) {
                return null;
            }
            Column7 value = Column7.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public byte[] getColumn8() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c8"));
            if (bytes == null) {
                return null;
            }
            Column8 value = Column8.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public Long getColumn9() {
            byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes("c9"));
            if (bytes == null) {
                return null;
            }
            Column9 value = Column9.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            return value.getValue();
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumn0Fun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn0();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumn1Fun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn1();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, UUID> getColumn10Fun() {
            return new Function<AllValueTypesTestRowResult, UUID>() {
                @Override
                public UUID apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn10();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumn2Fun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn2();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumn3Fun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn3();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Sha256Hash> getColumn4Fun() {
            return new Function<AllValueTypesTestRowResult, Sha256Hash>() {
                @Override
                public Sha256Hash apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn4();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, String> getColumn5Fun() {
            return new Function<AllValueTypesTestRowResult, String>() {
                @Override
                public String apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn5();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, String> getColumn6Fun() {
            return new Function<AllValueTypesTestRowResult, String>() {
                @Override
                public String apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn6();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, byte[]> getColumn7Fun() {
            return new Function<AllValueTypesTestRowResult, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn7();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, byte[]> getColumn8Fun() {
            return new Function<AllValueTypesTestRowResult, byte[]>() {
                @Override
                public byte[] apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn8();
                }
            };
        }

        public static Function<AllValueTypesTestRowResult, Long> getColumn9Fun() {
            return new Function<AllValueTypesTestRowResult, Long>() {
                @Override
                public Long apply(AllValueTypesTestRowResult rowResult) {
                    return rowResult.getColumn9();
                }
            };
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("RowName", getRowName())
                .add("Column0", getColumn0())
                .add("Column1", getColumn1())
                .add("Column10", getColumn10())
                .add("Column2", getColumn2())
                .add("Column3", getColumn3())
                .add("Column4", getColumn4())
                .add("Column5", getColumn5())
                .add("Column6", getColumn6())
                .add("Column7", getColumn7())
                .add("Column8", getColumn8())
                .add("Column9", getColumn9())
                .toString();
        }
    }

    public enum AllValueTypesTestNamedColumn {
        COLUMN0 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c0");
            }
        },
        COLUMN1 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c1");
            }
        },
        COLUMN10 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c10");
            }
        },
        COLUMN2 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c2");
            }
        },
        COLUMN3 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c3");
            }
        },
        COLUMN4 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c4");
            }
        },
        COLUMN5 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c5");
            }
        },
        COLUMN6 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c6");
            }
        },
        COLUMN7 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c7");
            }
        },
        COLUMN8 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c8");
            }
        },
        COLUMN9 {
            @Override
            public byte[] getShortName() {
                return PtBytes.toCachedBytes("c9");
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
                .put("c0", Column0.BYTES_HYDRATOR)
                .put("c1", Column1.BYTES_HYDRATOR)
                .put("c2", Column2.BYTES_HYDRATOR)
                .put("c3", Column3.BYTES_HYDRATOR)
                .put("c4", Column4.BYTES_HYDRATOR)
                .put("c5", Column5.BYTES_HYDRATOR)
                .put("c6", Column6.BYTES_HYDRATOR)
                .put("c7", Column7.BYTES_HYDRATOR)
                .put("c8", Column8.BYTES_HYDRATOR)
                .put("c9", Column9.BYTES_HYDRATOR)
                .put("c10", Column10.BYTES_HYDRATOR)
                .build();

    public Map<AllValueTypesTestRow, Long> getColumn0s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c0")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column0.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumn1s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c1")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column1.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumn2s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c2")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column2.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumn3s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c3")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column3.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Sha256Hash> getColumn4s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c4")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Sha256Hash> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Sha256Hash val = Column4.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, String> getColumn5s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c5")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = Column5.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, String> getColumn6s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c6")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, String> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            String val = Column6.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, byte[]> getColumn7s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c7")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Column7.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, byte[]> getColumn8s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c8")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            byte[] val = Column8.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, Long> getColumn9s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c9")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, Long> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            Long val = Column9.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public Map<AllValueTypesTestRow, UUID> getColumn10s(Collection<AllValueTypesTestRow> rows) {
        Map<Cell, AllValueTypesTestRow> cells = Maps.newHashMapWithExpectedSize(rows.size());
        for (AllValueTypesTestRow row : rows) {
            cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes("c10")), row);
        }
        Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());
        Map<AllValueTypesTestRow, UUID> ret = Maps.newHashMapWithExpectedSize(results.size());
        for (Entry<Cell, byte[]> e : results.entrySet()) {
            UUID val = Column10.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();
            ret.put(cells.get(e.getKey()), val);
        }
        return ret;
    }

    public void putColumn0(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column0.of(value)));
    }

    public void putColumn0(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column0.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn1(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column1.of(value)));
    }

    public void putColumn1(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column1.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn2(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column2.of(value)));
    }

    public void putColumn2(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column2.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn3(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column3.of(value)));
    }

    public void putColumn3(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column3.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn4(AllValueTypesTestRow row, Sha256Hash value) {
        put(ImmutableMultimap.of(row, Column4.of(value)));
    }

    public void putColumn4(Map<AllValueTypesTestRow, Sha256Hash> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Sha256Hash> e : map.entrySet()) {
            toPut.put(e.getKey(), Column4.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn5(AllValueTypesTestRow row, String value) {
        put(ImmutableMultimap.of(row, Column5.of(value)));
    }

    public void putColumn5(Map<AllValueTypesTestRow, String> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Column5.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn6(AllValueTypesTestRow row, String value) {
        put(ImmutableMultimap.of(row, Column6.of(value)));
    }

    public void putColumn6(Map<AllValueTypesTestRow, String> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, String> e : map.entrySet()) {
            toPut.put(e.getKey(), Column6.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn7(AllValueTypesTestRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Column7.of(value)));
    }

    public void putColumn7(Map<AllValueTypesTestRow, byte[]> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Column7.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn8(AllValueTypesTestRow row, byte[] value) {
        put(ImmutableMultimap.of(row, Column8.of(value)));
    }

    public void putColumn8(Map<AllValueTypesTestRow, byte[]> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, byte[]> e : map.entrySet()) {
            toPut.put(e.getKey(), Column8.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn9(AllValueTypesTestRow row, Long value) {
        put(ImmutableMultimap.of(row, Column9.of(value)));
    }

    public void putColumn9(Map<AllValueTypesTestRow, Long> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, Long> e : map.entrySet()) {
            toPut.put(e.getKey(), Column9.of(e.getValue()));
        }
        put(Multimaps.forMap(toPut));
    }

    public void putColumn10(AllValueTypesTestRow row, UUID value) {
        put(ImmutableMultimap.of(row, Column10.of(value)));
    }

    public void putColumn10(Map<AllValueTypesTestRow, UUID> map) {
        Map<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> toPut = Maps.newHashMapWithExpectedSize(map.size());
        for (Entry<AllValueTypesTestRow, UUID> e : map.entrySet()) {
            toPut.put(e.getKey(), Column10.of(e.getValue()));
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

    public void deleteColumn0(AllValueTypesTestRow row) {
        deleteColumn0(ImmutableSet.of(row));
    }

    public void deleteColumn0(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c0");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn1(AllValueTypesTestRow row) {
        deleteColumn1(ImmutableSet.of(row));
    }

    public void deleteColumn1(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c1");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn2(AllValueTypesTestRow row) {
        deleteColumn2(ImmutableSet.of(row));
    }

    public void deleteColumn2(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c2");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn3(AllValueTypesTestRow row) {
        deleteColumn3(ImmutableSet.of(row));
    }

    public void deleteColumn3(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c3");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn4(AllValueTypesTestRow row) {
        deleteColumn4(ImmutableSet.of(row));
    }

    public void deleteColumn4(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c4");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn5(AllValueTypesTestRow row) {
        deleteColumn5(ImmutableSet.of(row));
    }

    public void deleteColumn5(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c5");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn6(AllValueTypesTestRow row) {
        deleteColumn6(ImmutableSet.of(row));
    }

    public void deleteColumn6(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c6");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn7(AllValueTypesTestRow row) {
        deleteColumn7(ImmutableSet.of(row));
    }

    public void deleteColumn7(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c7");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn8(AllValueTypesTestRow row) {
        deleteColumn8(ImmutableSet.of(row));
    }

    public void deleteColumn8(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c8");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn9(AllValueTypesTestRow row) {
        deleteColumn9(ImmutableSet.of(row));
    }

    public void deleteColumn9(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c9");
        Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);
        t.delete(tableRef, cells);
    }

    public void deleteColumn10(AllValueTypesTestRow row) {
        deleteColumn10(ImmutableSet.of(row));
    }

    public void deleteColumn10(Iterable<AllValueTypesTestRow> rows) {
        byte[] col = PtBytes.toCachedBytes("c10");
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
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c0")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c1")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c10")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c2")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c3")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c4")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c5")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c6")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c7")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c8")));
        cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes("c9")));
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
        Multimap<AllValueTypesTestRow, AllValueTypesTestNamedColumnValue<?>> rowMap = HashMultimap.create();
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
    static String __CLASS_HASH = "6xLFlnVyHzB+tYXtqLsRTg==";
}
