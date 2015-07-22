package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

public class RowResultComparator implements Comparator<RowResult<Value>> {

    @Override
    public int compare(RowResult<Value> o1, RowResult<Value> o2) {
        final int cmpRow = UnsignedBytes.lexicographicalComparator().compare(
                o1.getRowName(),
                o2.getRowName());
        if (cmpRow != 0) {
            return cmpRow;
        }
        return Integer.compare(o1.hashCode(), o2.hashCode());
    }

    private static RowResultComparator instance;

    private RowResultComparator() {
    }

    public static RowResultComparator Instance() {
        if (instance == null) {
            instance = new RowResultComparator();
        }
        return instance;
    }
}
