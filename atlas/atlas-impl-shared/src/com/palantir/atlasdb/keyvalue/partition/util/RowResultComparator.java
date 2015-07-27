package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;

public class RowResultComparator implements Comparator<RowResult<?>> {

    @Override
    public int compare(RowResult<?> o1, RowResult<?> o2) {
        final int cmpRow = UnsignedBytes.lexicographicalComparator().compare(
                o1.getRowName(),
                o2.getRowName());
        if (cmpRow != 0) {
            return cmpRow;
        }
        return Integer.compare(o1.hashCode(), o2.hashCode());
    }

    private RowResultComparator() {
    }

    private static RowResultComparator instance;

    public static RowResultComparator instance() {
        if (instance == null) {
            instance = new RowResultComparator();
        }
        return instance;
    }
}
