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

import org.skife.jdbi.v2.SQLStatement;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
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

    public static final String USER_TABLE_PREFIX = "alamakota";
    public static final String USR_TABLE(String tableName) {
        return USER_TABLE_PREFIX + tableName;
    }
    public static final String USR_TABLE(String tableName, String alias) {
        return USR_TABLE(tableName) + " " + alias;
    }

    private static final int CHUNK_SIZE = 200;
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

    public static <K, V> SetMultimap<K, V> listToSetMultimap(List<Pair<K, V>> list) {
        SetMultimap<K, V> result = HashMultimap.create();
        for (Pair<K, V> p : list) {
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

    public static <T> Set<RowResult<Set<T>>> cellsToRows(SetMultimap<Cell, T> cells) {
        Set<RowResult<Set<T>>> result = Sets.newHashSet();
        NavigableMap<byte[], SortedMap<byte[], Set<T>>> s = Cells.breakCellsUpByRow(Multimaps.asMap(cells));
        for (Entry<byte[], SortedMap<byte[], Set<T>>> e : s.entrySet()) {
            result.add(RowResult.create(e.getKey(), e.getValue()));
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
    public static byte[] generateToken(RangeRequest rangeRequest, List<byte[]> rows) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        Preconditions.checkArgument(rows.size() > 0);
        byte[] lastRow = rows.get(rows.size() - 1);
        if (RangeRequests.isLastRowName(lastRow)) {
            return null;
        }
        return RangeRequests.getNextStartRow(rangeRequest.isReverse(), lastRow);
    }

    public static String makeSlots(String prefix, int number) {
        String result = "";
        for (int i=0; i<number; ++i) {
            result += ":" + prefix + i;
            if (i + 1 < number) {
                result += ", ";
            }
        }
        return result;
    }

    public static String makeSlots(String prefix, int number, int arity) {
        Preconditions.checkArgument(arity > 0);
        String result = "";
        for (int i=0; i<number; ++i) {
            result += "(";
            for (int j=0; j<arity; ++j) {
                result += ":" + prefix + i + "_" + j;
                if (j + 1 < arity) {
                    result += ", ";
                }
            }
            result += ")";
            if (i + 1 < number) {
                result += ", ";
            }
        }
        return result;
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
