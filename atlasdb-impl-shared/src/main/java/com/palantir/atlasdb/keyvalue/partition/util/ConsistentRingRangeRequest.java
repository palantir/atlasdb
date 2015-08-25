/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.util;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.annotation.Immutable;

@Immutable public class ConsistentRingRangeRequest {
    private final RangeRequest rangeRequest;
    public RangeRequest get() {
        return rangeRequest;
    }
    private ConsistentRingRangeRequest(RangeRequest rangeRequest) {
        this.rangeRequest = rangeRequest;
    }
    public static ConsistentRingRangeRequest of(RangeRequest rangeRequest) {
        return new ConsistentRingRangeRequest(rangeRequest);
    }
    @Override
    public String toString() {
        return "CRRR=[" + rangeRequest + "]";
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof ConsistentRingRangeRequest == false) {
            return false;
        }
        ConsistentRingRangeRequest otherCrrr = (ConsistentRingRangeRequest) other;
        return get().equals(otherCrrr.get());
    }
}
