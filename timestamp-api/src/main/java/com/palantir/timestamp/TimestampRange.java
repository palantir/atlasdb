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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

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
    public static TimestampRange createInclusiveRange(
            @JsonProperty("lowerBound") long boundOne, @JsonProperty("upperBound") long boundTwo) {
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
     * Determines if this {@link TimestampRange} contains the provided timestamp.
     *
     * @param timestamp timestamp to check for containment
     * @return true if and only if the timestamp is contained in this timestamp range
     */
    public boolean contains(long timestamp) {
        return lower <= timestamp && timestamp <= upper;
    }

    @Override
    public String toString() {
        return "TimestampRange{" + "lower=" + lower + ", upper=" + upper + '}';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TimestampRange that = (TimestampRange) other;
        return lower == that.lower && upper == that.upper;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lower, upper);
    }
}
