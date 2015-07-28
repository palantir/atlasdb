package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
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

    /* Get all values for the row of the first returned value. Return the newest value.
     *
     */
    public static RowResult<Value> mergeResults(PeekingIterator<RowResult<Value>> it) {
        Preconditions.checkArgument(it.hasNext());

        byte[] row = it.peek().getRowName();
        final SortedMap<byte[], Value> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        int failCount = 0;

        while (it.hasNext() && Arrays.equals(it.peek().getRowName(), row)) {
            try {
                failCount++;
                for (Map.Entry<Cell, Value> e : it.next().getCells()) {
                    final byte[] col = e.getKey().getColumnName();
                    if (!result.containsKey(col)
                            || result.get(col).getTimestamp() < e.getValue().getTimestamp()) {
                        result.put(col, e.getValue());
                    }
                }
                failCount--;
            } catch (Exception e) {
                System.err.println("Could not read for rangeRequest.");
            }
            // TODO
            if (failCount > 1) {
                throw new RuntimeException("Could not get enough reads.");
            }
        }
        return RowResult.create(row, result);
    }

    // Any failure will cause an exception
    public static RowResult<Set<Value>> allResults(PeekingIterator<RowResult<Set<Value>>> it) {
        Preconditions.checkArgument(it.hasNext());

        final byte[] row = it.peek().getRowName();
        SortedMap<byte[], Set<Value>> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        while (it.hasNext() && Arrays.equals(row, it.peek().getRowName())) {
            RowResult<Set<Value>> kvsResult = it.next();
            for (Map.Entry<Cell, Set<Value>> e : kvsResult.getCells()) {
                final byte[] col = e.getKey().getColumnName();
                if (!result.containsKey(col)) {
                    result.put(col, Sets.<Value>newHashSet());
                }
                result.get(col).addAll(e.getValue());
            }
        }
        return RowResult.create(row, result);
    }

    // Any failure will cause an exception
    public static RowResult<Set<Long>> allTimestamps(PeekingIterator<RowResult<Set<Long>>> it) {
        Preconditions.checkArgument(it.hasNext());

        final byte[] row = it.peek().getRowName();
        final SortedMap<byte[], Set<Long>> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        while (it.hasNext() && Arrays.equals(row, it.peek().getRowName())) {
            RowResult<Set<Long>> kvsResult = it.next();
            for (Map.Entry<Cell, Set<Long>> e : kvsResult.getCells()) {
                if (!result.containsKey(e.getKey().getColumnName())) {
                    result.put(e.getKey().getColumnName(), Sets.<Long>newHashSet());
                }
                result.get(e.getKey().getColumnName()).addAll(e.getValue());
            }
        }
        return RowResult.create(row, result);
    }
}