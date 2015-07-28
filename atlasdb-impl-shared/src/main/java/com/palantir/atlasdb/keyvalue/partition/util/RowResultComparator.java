package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;

public class RowResultComparator implements Comparator<RowResult<?>> {

    @Override
    public int compare(RowResult<?> o1, RowResult<?> o2) {
        return UnsignedBytes.lexicographicalComparator().compare(o1.getRowName(), o2.getRowName());
    }

    private RowResultComparator() {
    }

    private static RowResultComparator instance;

    public static RowResultComparator instance() {
        RowResultComparator ret = instance;
        if (ret == null) {
            ret = new RowResultComparator();
            instance = ret;
        }
        return ret;
    }
}
