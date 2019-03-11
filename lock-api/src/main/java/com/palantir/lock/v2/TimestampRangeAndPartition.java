/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.util.stream.LongStream;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.client.SharedConstants;
import com.palantir.timestamp.TimestampRange;

@Value.Immutable
@JsonSerialize(as = ImmutableTimestampRangeAndPartition.class)
@JsonDeserialize(as = ImmutableTimestampRangeAndPartition.class)
public interface TimestampRangeAndPartition {
    @Value.Parameter
    TimestampRange range();

    @Value.Parameter
    int partition();

    @JsonIgnore
    default long[] getStartTimestamps() {
        return LongStream.rangeClosed(range().getLowerBound(), range().getUpperBound())
                .filter(timestamp -> timestamp % SharedConstants.V2_TRANSACTION_NUM_PARTITIONS == partition())
                .toArray();
    }

    static TimestampRangeAndPartition of(TimestampRange range, int partition) {
        return ImmutableTimestampRangeAndPartition.of(range, partition);
    }
}
