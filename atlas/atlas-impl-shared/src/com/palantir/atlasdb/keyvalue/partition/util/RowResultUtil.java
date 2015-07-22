package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

public class RowResultUtil {

    public static Map<Cell, Value> getNewestValue(RowResult<Value> row) {

        Map<Cell, Value> result = Maps.newHashMap();

        for (Map.Entry<Cell, Value> e : row.getCells()) {
            final Cell cell = e.getKey();
            final Value val = e.getValue();
            if (!result.containsKey(cell) ||
                    result.get(cell).getTimestamp() < val.getTimestamp()) {
                result.put(cell, val);
            }
        }

        return result;

    }

    public static RowResult<Value> mergeResults(PeekingIterator<RowResult<Value>> it) {
        Preconditions.checkArgument(it.hasNext());

        byte[] row = it.peek().getRowName();
        final Map<Cell, Value> mapResult = Maps.newHashMap();

        while (it.hasNext()) {
            if (Arrays.equals(it.peek().getRowName(), row) == false) {
                break;
            }
            final Map<Cell, Value> newestCurrentResult = getNewestValue(it.next());
            for (Map.Entry<Cell, Value> e : newestCurrentResult.entrySet()) {
                final Cell cell = e.getKey();
                final Value val = e.getValue();
                if (!mapResult.containsKey(cell) || mapResult.get(cell).getTimestamp() < val.getTimestamp()) {
                    mapResult.put(cell, val);
                }
            }
        }

        final TreeMap<byte[], Value> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (Map.Entry<Cell, Value> e : mapResult.entrySet()) {
            result.put(e.getKey().getColumnName(), e.getValue());
        }

        return RowResult.create(row, result);
    }

}