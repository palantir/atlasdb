/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
