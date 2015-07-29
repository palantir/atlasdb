package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;

public class ConsistentRingRangeComparator implements Comparator<ConsistentRingRangeRequest> {

    @Override
    public int compare(ConsistentRingRangeRequest cr1, ConsistentRingRangeRequest cr2) {
        final RangeRequest r1 = cr1.get();
        final RangeRequest r2 = cr2.get();
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

    private ConsistentRingRangeComparator() {
    }

    private static ConsistentRingRangeComparator instance;

    public static ConsistentRingRangeComparator instance() {
        ConsistentRingRangeComparator ret = instance;
        if (ret == null) {
            ret = new ConsistentRingRangeComparator();
            instance = ret;
        }
        return ret;
    }
}
