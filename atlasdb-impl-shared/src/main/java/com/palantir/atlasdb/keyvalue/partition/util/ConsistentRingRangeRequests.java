package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;


public class ConsistentRingRangeRequests {

     private static class ConsistentRingRangeRequestComparator implements Comparator<ConsistentRingRangeRequest> {

        @Override
        public int compare(ConsistentRingRangeRequest o1, ConsistentRingRangeRequest o2) {
            Preconditions.checkArgument(!o1.get().isReverse());
            Preconditions.checkArgument(!o2.get().isReverse());
            Comparator<byte[]> lexCmp = UnsignedBytes.lexicographicalComparator();
            RangeRequest r1 = o1.get();
            RangeRequest r2 = o2.get();
            int cmpStart = lexCmp.compare(r1.getStartInclusive(), r2.getStartInclusive());
            int cmpEnd;
            if (r1.getEndExclusive().length > 0 && r2.getEndExclusive().length > 0) {
                cmpEnd = lexCmp.compare(r1.getEndExclusive(), r2.getEndExclusive());
            } else {
                if (r1.getEndExclusive().length > 0) {
                    // r1 is bounded and r2 is unbounded -> r1 < r2
                    cmpEnd = -1;
                } else if (r2.getEndExclusive().length > 0) {
                    cmpEnd = 1;
                } else {
                    // Both are unbounded
                    cmpEnd = 0;
                }
            }
            Preconditions.checkArgument(Math.signum(cmpStart) == Math.signum(cmpEnd));
            return cmpStart;
        }

        private static ConsistentRingRangeRequestComparator instance;
        public static ConsistentRingRangeRequestComparator instance() {
            if (instance == null) {
                ConsistentRingRangeRequestComparator ret = new ConsistentRingRangeRequestComparator();
                instance = ret;
            }
            return instance;
        }

     }

     public static Comparator<ConsistentRingRangeRequest> getCompareByStartRow() {
         return ConsistentRingRangeRequestComparator.instance();
     }

     private ConsistentRingRangeRequests() {
     }
}
