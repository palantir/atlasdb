/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timestamp;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.PartitionedTimestamps;

/**
 * Utilities for manipulating {@link TimestampRange} objects.
 */
public final class TimestampRanges {
    private TimestampRanges() {
        // utility
    }

    /**
     * Returns all timestamps in given residue class for given modulus in provided timestamp range. Timestamps are
     * represented by {@link PartitionedTimestamps}.
     *
     * @param residue desired residue class of the timestamps returned
     * @param modulus modulus used to partition numbers into residue classes
     * @return {@link PartitionedTimestamps} satisfying the residue class condition
     */
    public static PartitionedTimestamps getPartitionedTimestamps(TimestampRange range, int residue, int modulus) {
        checkModulusAndResidue(residue, modulus);

        long startTimestamp = getLowestTimestampMatchingModulus(range.getLowerBound(), residue, modulus);
        long endTimestamp = range.getUpperBound() % modulus == residue
                ? range.getUpperBound()
                : getLowestTimestampMatchingModulus(range.getUpperBound(), residue, modulus) - modulus;

        if (startTimestamp > endTimestamp) {
            return ImmutablePartitionedTimestamps.builder()
                    .start(range.getLowerBound())
                    .interval(modulus)
                    .count(0)
                    .build();
        }

        int count = (int) ((endTimestamp - startTimestamp) / modulus) + 1;

        return ImmutablePartitionedTimestamps.builder()
                .start(startTimestamp)
                .interval(modulus)
                .count(count)
                .build();
    }

    private static long getLowestTimestampMatchingModulus(long lowerBound, int residue, int modulus) {
        long lowerBoundResidue = LongMath.mod(lowerBound, modulus);
        long shift = residue < lowerBoundResidue ? modulus + residue - lowerBoundResidue : residue - lowerBoundResidue;
        return lowerBound + shift;
    }

    private static void checkModulusAndResidue(int residue, int modulus) {
        Preconditions.checkArgument(modulus > 0, "Modulus should be positive, but found %s.", modulus);
        Preconditions.checkArgument(
                Math.abs((long) residue) < modulus,
                "Absolute value of residue %s equals or exceeds modulus %s - no solutions",
                residue,
                modulus);
    }
}
