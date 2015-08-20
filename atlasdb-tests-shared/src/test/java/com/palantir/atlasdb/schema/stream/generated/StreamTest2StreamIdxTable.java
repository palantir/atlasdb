/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.schema.stream.generated;

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


public final class StreamTest2StreamIdxTable implements
        AtlasDbDynamicMutableExpiringTable<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxColumn,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxColumnValue,
                                              StreamTest2StreamIdxTable.StreamTest2StreamIdxRowResult> {
    private final Transaction t;
    private final List<StreamTest2StreamIdxTrigger> triggers;
    private final static String tableName = "stream_test_2_stream_idx";

    static StreamTest2StreamIdxTable of(Transaction t) {
        return new StreamTest2StreamIdxTable(t, ImmutableList.<StreamTest2StreamIdxTrigger>of());
    }

    static StreamTest2StreamIdxTable of(Transaction t, StreamTest2StreamIdxTrigger trigger, StreamTest2StreamIdxTrigger... triggers) {
        return new StreamTest2StreamIdxTable(t, ImmutableList.<StreamTest2StreamIdxTrigger>builder().add(trigger).add(triggers).build());
    }

    static StreamTest2StreamIdxTable of(Transaction t, List<StreamTest2StreamIdxTrigger> triggers) {
        return new StreamTest2StreamIdxTable(t, triggers);
    }

    private StreamTest2StreamIdxTable(Transaction t, List<StreamTest2StreamIdxTrigger> triggers) {
        this.t = t;
        this.triggers = triggers;
    }

    public static String getTableName() {
        return tableName;
    }

    /**
     * <pre>
     * StreamTest2StreamIdxRow {
     *   {@literal Long id};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamIdxRow implements Persistable, Comparable<StreamTest2StreamIdxRow> {
        private final long id;

        public static StreamTest2StreamIdxRow of(long id) {
            return new StreamTest2StreamIdxRow(id);
        }

        private StreamTest2StreamIdxRow(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public static Function<StreamTest2StreamIdxRow, Long> getIdFun() {
            return new Function<StreamTest2StreamIdxRow, Long>() {
                @Override
                public Long apply(StreamTest2StreamIdxRow row) {
                    return row.id;
                }
            };
        }

        public static Function<Long, StreamTest2StreamIdxRow> fromIdFun() {
            return new Function<Long, StreamTest2StreamIdxRow>() {
                @Override
                public StreamTest2StreamIdxRow apply(Long row) {
                    return new StreamTest2StreamIdxRow(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] idBytes = EncodingUtils.encodeUnsignedVarLong(id);
            return EncodingUtils.add(idBytes);
        }

        public static final Hydrator<StreamTest2StreamIdxRow> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamIdxRow>() {
            @Override
            public StreamTest2StreamIdxRow hydrateFromBytes(byte[] __input) {
                int __index = 0;
                Long id = EncodingUtils.decodeUnsignedVarLong(__input, __index);
                __index += EncodingUtils.sizeOfUnsignedVarLong(id);
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
            StreamTest2StreamIdxRow other = (StreamTest2StreamIdxRow) obj;
            return Objects.equal(id, other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(StreamTest2StreamIdxRow o) {
            return ComparisonChain.start()
                .compare(this.id, o.id)
                .result();
        }
    }

    /**
     * <pre>
     * StreamTest2StreamIdxColumn {
     *   {@literal byte[] reference};
     * }
     * </pre>
     */
    public static final class StreamTest2StreamIdxColumn implements Persistable, Comparable<StreamTest2StreamIdxColumn> {
        private final byte[] reference;

        public static StreamTest2StreamIdxColumn of(byte[] reference) {
            return new StreamTest2StreamIdxColumn(reference);
        }

        private StreamTest2StreamIdxColumn(byte[] reference) {
            this.reference = reference;
        }

        public byte[] getReference() {
            return reference;
        }

        public static Function<StreamTest2StreamIdxColumn, byte[]> getReferenceFun() {
            return new Function<StreamTest2StreamIdxColumn, byte[]>() {
                @Override
                public byte[] apply(StreamTest2StreamIdxColumn row) {
                    return row.reference;
                }
            };
        }

        public static Function<byte[], StreamTest2StreamIdxColumn> fromReferenceFun() {
            return new Function<byte[], StreamTest2StreamIdxColumn>() {
                @Override
                public StreamTest2StreamIdxColumn apply(byte[] row) {
                    return new StreamTest2StreamIdxColumn(row);
                }
            };
        }

        @Override
        public byte[] persistToBytes() {
            byte[] referenceBytes = EncodingUtils.encodeSizedBytes(reference);
            return EncodingUtils.add(referenceBytes);
        }

        public static final Hydrator<StreamTest2StreamIdxColumn> BYTES_HYDRATOR = new Hydrator<StreamTest2StreamIdxColumn>() {
            @Override
            public StreamTest2StreamIdxColumn hydrateFromBytes(byte[] __input) {
                int __index = 0;
                byte[] reference = EncodingUtils.decodeSizedBytes(__input, __index);
                __index += EncodingUtils.sizeOfSizedBytes(reference);
                return of(reference);
            }
        };

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("reference", reference)
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
            StreamTest2StreamIdxColumn other = (StreamTest2StreamIdxColumn) obj;
            return Objects.equal(reference, other.reference);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(reference);
        }

        @Override
        public int compareTo(StreamTest2StreamIdxColumn o) {
            return ComparisonChain.start()
                .compare(this.reference, o.reference, UnsignedBytes.lexicographicalComparator())
                .result();
        }
    }

    public interface StreamTest2StreamIdxTrigger {
        public void putStreamTest2StreamIdx(Multimap<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> newRows);
    }

    /**
     * <pre>
     * Column name description {
     *   {@literal byte[] reference};
     * } 
     * Column value description {
     *   type: Long;
     * }
     * </pre>
     */
    public static final class StreamTest2StreamIdxColumnValue implements ColumnValue<Long> {
        private final StreamTest2StreamIdxColumn columnName;
        private final Long value;

        public static StreamTest2StreamIdxColumnValue of(StreamTest2StreamIdxColumn columnName, Long value) {
            return new StreamTest2StreamIdxColumnValue(columnName, value);
        }

        private StreamTest2StreamIdxColumnValue(StreamTest2StreamIdxColumn columnName, Long value) {
            this.columnName = columnName;
            this.value = value;
        }

        public StreamTest2StreamIdxColumn getColumnName() {
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

        public static Function<StreamTest2StreamIdxColumnValue, StreamTest2StreamIdxColumn> getColumnNameFun() {
            return new Function<StreamTest2StreamIdxColumnValue, StreamTest2StreamIdxColumn>() {
                @Override
                public StreamTest2StreamIdxColumn apply(StreamTest2StreamIdxColumnValue columnValue) {
                    return columnValue.getColumnName();
                }
            };
        }

        public static Function<StreamTest2StreamIdxColumnValue, Long> getValueFun() {
            return new Function<StreamTest2StreamIdxColumnValue, Long>() {
                @Override
                public Long apply(StreamTest2StreamIdxColumnValue columnValue) {
                    return columnValue.getValue();
                }
            };
        }
    }

    public static final class StreamTest2StreamIdxRowResult implements TypedRowResult {
        private final StreamTest2StreamIdxRow rowName;
        private final ImmutableSet<StreamTest2StreamIdxColumnValue> columnValues;

        public static StreamTest2StreamIdxRowResult of(RowResult<byte[]> rowResult) {
            StreamTest2StreamIdxRow rowName = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());
            Set<StreamTest2StreamIdxColumnValue> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long value = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                columnValues.add(StreamTest2StreamIdxColumnValue.of(col, value));
            }
            return new StreamTest2StreamIdxRowResult(rowName, ImmutableSet.copyOf(columnValues));
        }

        private StreamTest2StreamIdxRowResult(StreamTest2StreamIdxRow rowName, ImmutableSet<StreamTest2StreamIdxColumnValue> columnValues) {
            this.rowName = rowName;
            this.columnValues = columnValues;
        }

        @Override
        public StreamTest2StreamIdxRow getRowName() {
            return rowName;
        }

        public Set<StreamTest2StreamIdxColumnValue> getColumnValues() {
            return columnValues;
        }

        public static Function<StreamTest2StreamIdxRowResult, StreamTest2StreamIdxRow> getRowNameFun() {
            return new Function<StreamTest2StreamIdxRowResult, StreamTest2StreamIdxRow>() {
                @Override
                public StreamTest2StreamIdxRow apply(StreamTest2StreamIdxRowResult rowResult) {
                    return rowResult.rowName;
                }
            };
        }

        public static Function<StreamTest2StreamIdxRowResult, ImmutableSet<StreamTest2StreamIdxColumnValue>> getColumnValuesFun() {
            return new Function<StreamTest2StreamIdxRowResult, ImmutableSet<StreamTest2StreamIdxColumnValue>>() {
                @Override
                public ImmutableSet<StreamTest2StreamIdxColumnValue> apply(StreamTest2StreamIdxRowResult rowResult) {
                    return rowResult.columnValues;
                }
            };
        }
    }

    @Override
    public void delete(StreamTest2StreamIdxRow row, StreamTest2StreamIdxColumn column) {
        delete(ImmutableMultimap.of(row, column));
    }

    @Override
    public void delete(Iterable<StreamTest2StreamIdxRow> rows) {
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> toRemove = HashMultimap.create();
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> result = getRowsMultimap(rows);
        for (Entry<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> e : result.entries()) {
            toRemove.put(e.getKey(), e.getValue().getColumnName());
        }
        delete(toRemove);
    }

    @Override
    public void delete(Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> values) {
        t.delete(tableName, ColumnValues.toCells(values));
    }

    @Override
    public void put(StreamTest2StreamIdxRow rowName, Iterable<StreamTest2StreamIdxColumnValue> values, long duration, TimeUnit unit) {
        put(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(long duration, TimeUnit unit, StreamTest2StreamIdxRow rowName, StreamTest2StreamIdxColumnValue... values) {
        put(ImmutableMultimap.<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>builder().putAll(rowName, values).build(), duration, unit);
    }

    @Override
    public void put(Multimap<StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxColumnValue> values, long duration, TimeUnit unit) {
        t.useTable(tableName, this);
        t.put(tableName, ColumnValues.toCellValues(values, duration, unit));
        for (StreamTest2StreamIdxTrigger trigger : triggers) {
            trigger.putStreamTest2StreamIdx(values);
        }
    }

    public static ColumnSelection getColumnSelection(Collection<StreamTest2StreamIdxColumn> cols) {
        return ColumnSelection.create(Collections2.transform(cols, Persistables.persistToBytesFunction()));
    }

    public static ColumnSelection getColumnSelection(StreamTest2StreamIdxColumn... cols) {
        return getColumnSelection(Arrays.asList(cols));
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> get(Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> cells) {
        Set<Cell> rawCells = ColumnValues.toCells(cells);
        Map<Cell, byte[]> rawResults = t.get(tableName, rawCells);
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> rowMap = HashMultimap.create();
        for (Entry<Cell, byte[]> e : rawResults.entrySet()) {
            if (e.getValue().length > 0) {
                StreamTest2StreamIdxRow row = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsync(final Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumn> cells, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> call() {
                return get(cells);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    @Override
    public List<StreamTest2StreamIdxColumnValue> getRowColumns(StreamTest2StreamIdxRow row) {
        return getRowColumns(row, ColumnSelection.all());
    }

    @Override
    public List<StreamTest2StreamIdxColumnValue> getRowColumns(StreamTest2StreamIdxRow row, ColumnSelection columns) {
        byte[] bytes = row.persistToBytes();
        RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);
        if (rowResult == null) {
            return ImmutableList.of();
        } else {
            List<StreamTest2StreamIdxColumnValue> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());
            for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                ret.add(StreamTest2StreamIdxColumnValue.of(col, val));
            }
            return ret;
        }
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows) {
        return getRowsMultimapInternal(rows, ColumnSelection.all());
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows, ColumnSelection columns) {
        return getRowsMultimapInternal(rows, columns);
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsyncRowsMultimap(Iterable<StreamTest2StreamIdxRow> rows, ExecutorService exec) {
        return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);
    }

    @Override
    public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getAsyncRowsMultimap(final Iterable<StreamTest2StreamIdxRow> rows, final ColumnSelection columns, ExecutorService exec) {
        Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>> c =
                new Callable<Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue>>() {
            @Override
            public Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> call() {
                return getRowsMultimapInternal(rows, columns);
            }
        };
        return AsyncProxy.create(exec.submit(c), Multimap.class);
    }

    private Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowsMultimapInternal(Iterable<StreamTest2StreamIdxRow> rows, ColumnSelection columns) {
        SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);
        return getRowMapFromRowResults(results.values());
    }

    private static Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {
        Multimap<StreamTest2StreamIdxRow, StreamTest2StreamIdxColumnValue> rowMap = HashMultimap.create();
        for (RowResult<byte[]> result : rowResults) {
            StreamTest2StreamIdxRow row = StreamTest2StreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());
            for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {
                StreamTest2StreamIdxColumn col = StreamTest2StreamIdxColumn.BYTES_HYDRATOR.hydrateFromBytes(e.getKey());
                Long val = StreamTest2StreamIdxColumnValue.hydrateValue(e.getValue());
                rowMap.put(row, StreamTest2StreamIdxColumnValue.of(col, val));
            }
        }
        return rowMap;
    }

    public BatchingVisitableView<StreamTest2StreamIdxRowResult> getAllRowsUnordered() {
        return getAllRowsUnordered(ColumnSelection.all());
    }

    public BatchingVisitableView<StreamTest2StreamIdxRowResult> getAllRowsUnordered(ColumnSelection columns) {
        return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),
                new Function<RowResult<byte[]>, StreamTest2StreamIdxRowResult>() {
            @Override
            public StreamTest2StreamIdxRowResult apply(RowResult<byte[]> input) {
                return StreamTest2StreamIdxRowResult.of(input);
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
    static String __CLASS_HASH = "1fxdSuB3VY8bCUvQoYumiA==";
}
