package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;


public class ConsistentRingRangeComparator implements Comparator<ConsistentRingRangeRequest> {

     // Handle the special case of unbounded range.
     public static int compareBytes(byte[] b1, byte[] b2, boolean reverse) {
         if (b1.length > 0 && b2.length > 0) {
             return UnsignedBytes.lexicographicalComparator().compare(b1, b2);
         }
         if (b1.length == 0) {
             if (b2.length == 0) {
                 return 0;
             } else {
                 return reverse ? -1 : 1;
             }
         }
         return reverse ? 1 : -1;
     }

     @Override
     public int compare(ConsistentRingRangeRequest cr1, ConsistentRingRangeRequest cr2) {
         final RangeRequest r1 = cr1.get();
         final RangeRequest r2 = cr2.get();
         if (r1.equals(r2)) {
             return 0;
         }
         // Same orientation
         Preconditions.checkArgument(r1.isReverse() == r2.isReverse());
         boolean reverse = r1.isReverse();
         int cmpStart = compareBytes(r1.getStartInclusive(), r2.getStartInclusive(), reverse);
         int cmpEnd = compareBytes(r1.getEndExclusive(), r2.getEndExclusive(), reverse);
         // Non-overlapping (both checks below)
         Preconditions.checkArgument(Math.signum(cmpStart) == Math.signum(cmpEnd));
         final RangeRequest smallerRange = cmpStart <= 0 ? r1 : r2;
         final RangeRequest biggerRange = cmpStart <= 0 ? r2 : r1;
         int cmpDist = compareBytes(
                 smallerRange.getEndExclusive(),
                 biggerRange.getStartInclusive(),
                 reverse);
         Preconditions.checkArgument(cmpDist <= 0);
         // Validation completed
         if (cmpStart == 0) {
             return Integer.compare(r1.hashCode(), r2.hashCode());
         }
         return cmpStart;
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
