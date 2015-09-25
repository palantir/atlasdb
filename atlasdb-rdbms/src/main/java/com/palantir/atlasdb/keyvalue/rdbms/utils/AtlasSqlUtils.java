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
package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

import javax.annotation.Nullable;

import org.apache.commons.lang.text.StrBuilder;
import org.skife.jdbi.v2.SQLStatement;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.util.Pair;

public class AtlasSqlUtils {
    private AtlasSqlUtils() { }

    public static final String USER_TABLE_PREFIX = "atlasdb_usr_table_";
    public static final String USR_TABLE(String tableName) {
        return USER_TABLE_PREFIX + tableName;
    }
    public static final String USR_TABLE(String tableName, String alias) {
        return USR_TABLE(tableName) + " " + alias;
    }

    private static final int CHUNK_SIZE = 200;
    public static final int MAX_TABLE_NAME_LEN = 128 + USER_TABLE_PREFIX.length();
    public static <V> void batch(Iterable<V> items, Function<Collection<V>, Void> runWithBatch) {
        for (List<V> chunk : Iterables.partition(items, CHUNK_SIZE)) {
            runWithBatch.apply(chunk);
        }
    }

    public static int getBatchSize(RangeRequest rangeRequest) {
        return rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
    }


    private static boolean throwableContainsMessage(Throwable e, String... messages) {
        for (Throwable ex = e; ex != null; ex = ex.getCause()) {
            for (String message : messages) {
                if (ex.getMessage().contains(message)) {
                    return true;
                }
            }
            if (ex == ex.getCause()) {
                return false;
            }
        }
        return false;
    }

    public static boolean isKeyAlreadyExistsException(Throwable e) {
        return throwableContainsMessage(e, "ORA-00001", "unique constraint");
    }

    public static <K, V> ListMultimap<K, V> listToListMultimap(List<Pair<K, V>> list) {
        ListMultimap<K, V> result = LinkedListMultimap.create();
        for (Pair<K, V> p : list) {
            // High cost, only check with assertions enabled
            assert !result.containsEntry(p.lhSide, p.rhSide);
            result.put(p.lhSide, p.rhSide);
        }
        return result;
    }

    public static <K, V> SetMultimap<K, V> listToSetMultimap(List<Pair<K, V>> list) {
        SetMultimap<K, V> result = HashMultimap.create();
        for (Pair<K, V> p : list) {
            Preconditions.checkArgument(!result.containsEntry(p.lhSide, p.rhSide));
            result.put(p.lhSide, p.rhSide);
        }
        return result;
    }

    public static <K, V> Map<K, V> listToMap(List<Pair<K, V>> list) {
        Map<K, V> result = Maps.newHashMap();
        for (Pair<K, V> p : list) {
            Preconditions.checkArgument(!result.containsKey(p.lhSide));
            result.put(p.lhSide, p.rhSide);
        }
        return result;
    }

    private static <K, V> SortedMap<K, Set<V>> listSortedMapToSetSortedMap(SortedMap<K, List<V>> map) {
        SortedMap<K, Set<V>> result = Maps.newTreeMap(map.comparator());
        for (Entry<K, List<V>> e : map.entrySet()) {
            result.put(e.getKey(), Sets.<V>newHashSet(e.getValue()));
        }
        return result;
    }

    public static <T> List<RowResult<Set<T>>> cellsToRows(ListMultimap<Cell, T> cells) {
        List<RowResult<Set<T>>> result = Lists.newArrayList();
        NavigableMap<byte[],SortedMap<byte[],List<T>>> s = Cells.breakCellsUpByRow(Multimaps.asMap(cells));
        for (Entry<byte[], SortedMap<byte[], List<T>>> e : s.entrySet()) {
            result.add(RowResult.create(e.getKey(), listSortedMapToSetSortedMap(e.getValue())));
        }
        return result;
    }

    public static <T> List<RowResult<T>> cellsToRows(Map<Cell, T> cells) {
        NavigableMap<byte[],SortedMap<byte[],T>> byRow = Cells.breakCellsUpByRow(cells);
        List<RowResult<T>> result = Lists.newArrayList();
        for (Entry<byte[], SortedMap<byte[], T>> e : byRow.entrySet()) {
            result.add(RowResult.create(e.getKey(), e.getValue()));
        }
        return result;
    }

    @Nullable
    public static byte[] generateToken(RangeRequest rangeRequest, byte[] lastRow) {
        if (RangeRequests.isTerminalRow(rangeRequest.isReverse(), lastRow)) {
            return null;
        }
        return RangeRequests.getNextStartRow(rangeRequest.isReverse(), lastRow);
    }

    public static String makeSlots(String prefix, int number) {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i<number; ++i) {
            builder.append(":").append(prefix).append(i);
            if (i + 1 < number) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }

    public static String makeSlots(String prefix, int number, int arity) {
        Preconditions.checkArgument(arity > 0);
        StrBuilder builder = new StrBuilder();
        for (int i=0; i<number; ++i) {
            builder.append("(");
            for (int j=0; j<arity; ++j) {
                builder.append(":").append(prefix).append(i).append("_").append(j);
                if (j + 1 < arity) {
                    builder.append(", ");
                }
            }
            builder.append(")");
            if (i + 1 < number) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }

    public static <T> void bindAll(SQLStatement<?> query, Iterable<T> values) {
        bindAll(query, values, 0);
    }

    public static <T> void bindAll(SQLStatement<?> query, Iterable<T> values, int startPosition) {
        int pos = startPosition;
        for (T value : values) {
            query.bind(pos++, value);
        }
    }

    public static void bindCells(SQLStatement<?> query, Iterable<Cell> cells) {
        bindCells(query, cells, 0);
    }

    public static void bindCells(SQLStatement<?> query, Iterable<Cell> cells, int startPosition) {
        int pos = startPosition;
        for (Cell cell : cells) {
            query.bind(pos++, cell.getRowName());
            query.bind(pos++, cell.getColumnName());
        }
    }

    public static void bindCellsValues(SQLStatement<?> query, Iterable<Entry<Cell, Value>> values) {
        bindCellsValues(query, values, 0);
    }

    public static void bindCellsValues(SQLStatement<?> query, Iterable<Entry<Cell, Value>> values, int startPosition) {
        int pos = startPosition;
        for (Entry<Cell, Value> entry : values) {
            query.bind(pos++, entry.getKey().getRowName());
            query.bind(pos++, entry.getKey().getColumnName());
            query.bind(pos++, entry.getValue().getTimestamp());
            query.bind(pos++, entry.getValue().getContents());
        }
    }

    public static void bindCellsValues(SQLStatement<?> query, Iterable<Entry<Cell, byte[]>> values, long timestamp) {
        bindCellsValues(query, values, timestamp, 0);
    }

    public static void bindCellsValues(SQLStatement<?> query, Iterable<Entry<Cell, byte[]>> values, long timestamp, int startPosition) {
        int pos = startPosition;
        for (Entry<Cell, byte[]> entry : values) {
            query.bind(pos++, entry.getKey().getRowName());
            query.bind(pos++, entry.getKey().getColumnName());
            query.bind(pos++, timestamp);
            query.bind(pos++, entry.getValue());
        }
    }

    public static void bindCellsTimestamps(SQLStatement<?> query, Iterable<Entry<Cell, Long>> values) {
        bindCellsTimestamps(query, values, 0);
    }

    public static void bindCellsTimestamps(SQLStatement<?> query, Iterable<Entry<Cell, Long>> values, int startPosition) {
        int pos = startPosition;
        for (Entry<Cell, Long> entry : values) {
            query.bind(pos++, entry.getKey().getRowName());
            query.bind(pos++, entry.getKey().getColumnName());
            query.bind(pos++, entry.getValue());
        }
    }
}
