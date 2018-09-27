/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timestamp;

import java.io.Serializable;
import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * A TimestampRange represents an inclusive range of longs.
 *
 * @author bdorne
 */
@SuppressWarnings("checkstyle:FinalClass") // Don't want to break the API
public class TimestampRange implements Serializable {
    private static final long serialVersionUID = 1L;

    private long lower;
    private long upper;

    private TimestampRange(long lowerBound, long upperBound) {
        this.lower = lowerBound;
        this.upper = upperBound;
    }

    /**
     * Constructs a TimestampRange, inclusive to both longs. The lower bound
     * and upper bound of the range can be passed in either order.
     *
     * @param boundOne first number in range, inclusive
     * @param boundTwo second number in range, inclusive
     * @return a new inclusive TimestampRange
     */
    @JsonCreator
    public static TimestampRange createInclusiveRange(@JsonProperty("lowerBound") long boundOne,
                                                      @JsonProperty("upperBound") long boundTwo) {
        if (boundTwo < boundOne) {
            return new TimestampRange(boundTwo, boundOne);
        } else {
            return new TimestampRange(boundOne, boundTwo);
        }
    }

    /**
     * Returns the lower bound of this TimestampRange (inclusive).
     *
     * @return the lower bound of the TimestampRange
     */
    public long getLowerBound() {
        return lower;
    }

    /**
     * Returns the upper bound of this TimestampRange (inclusive).
     *
     * @return the upper bound of the TimestampRange
     */
    public long getUpperBound() {
        return upper;
    }

    public long size() {
        // Need to add 1 as both bounds are inclusive
        return upper - lower + 1;
    }

    /**
     * Returns a timestamp in the provided timestamp range that has the provided residue class modulo the provided
     * modulus.
     *
     * We do not make any guarantees on which timestamp is provided if multiple timestamps in the range reside
     * in the provided residue class. For example, if the TimestampRange is from 1 to 5 inclusive and we want a
     * timestamp with residue 1 modulo 2, this method may return any of 1, 3 and 5.
     *
     * If the timestamp range does not contain a timestamp matching the criteria, returns empty.
     *
     * @param residue desired residue class of the timestamp returned
     * @param modulus modulus used to partition numbers into residue classes
     * @return a timestamp in the given range in the relevant residue class modulo modulus
     * @throws IllegalArgumentException if residue >= modulus
     */
    public OptionalLong getTimestampMatchingModulus(int residue, int modulus) {
        Preconditions.checkArgument(residue < modulus,
                "Residue %s is less than modulus %s - no solutions",
                residue,
                modulus);

        long lowerBoundResidue = lower % modulus;
        long shift = residue < lowerBoundResidue ? modulus + residue - lowerBoundResidue :
                residue - lowerBoundResidue;
        long candidate = lower + shift;

        return upper >= candidate
                ? OptionalLong.of(candidate)
                : OptionalLong.empty();
    }
}
