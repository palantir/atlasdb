package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public class RangeComparator implements Comparator<RangeRequest> {

    @Override
    public int compare(RangeRequest r1, RangeRequest r2) {
        if (UnsignedBytes.lexicographicalComparator().compare(
                r1.getStartInclusive(),
                r2.getStartInclusive()) == 0) {
            return Integer.compare(r1.hashCode(), r2.hashCode());
        }
        return UnsignedBytes.lexicographicalComparator().compare(
                r1.getStartInclusive(),
                r2.getStartInclusive());
    }

    private RangeComparator() {
    }

    private static RangeComparator instance;

    public static RangeComparator Instance() {
        if (instance == null) {
            instance = new RangeComparator();
        }
        return instance;
    }
}
