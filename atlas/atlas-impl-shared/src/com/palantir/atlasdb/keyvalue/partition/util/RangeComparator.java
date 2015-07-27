package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public class RangeComparator implements Comparator<RangeRequest> {

    @Override
    public int compare(RangeRequest r1, RangeRequest r2) {
        Preconditions.checkArgument(
                // Case 1: Sample interval
                r1.equals(r2) ||
                // Case 2: Non-overlapping intervals
                (
                        Math.signum(UnsignedBytes.lexicographicalComparator().compare(r1.getStartInclusive(), r2.getStartInclusive()))
                            == Math.signum(UnsignedBytes.lexicographicalComparator().compare(r1.getEndExclusive(), r2.getEndExclusive()))
                        && UnsignedBytes.lexicographicalComparator().compare(r1.getStartInclusive(), r2.getStartInclusive()) != 0
                        && r1.isReverse() == r2.isReverse()
                )
        );
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
