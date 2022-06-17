/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSerializableTimestampSet.class)
@JsonDeserialize(as = ImmutableSerializableTimestampSet.class)
public interface SerializableTimestampSet {
    @Value.Parameter
    Set<Range<Long>> longRanges();

    @Value.Lazy
    default RangeSet<Long> rangeSetView() {
        RangeSet<Long> userRangeSet = TreeRangeSet.create();
        for (Range<Long> longRange : longRanges()) {
            userRangeSet.add(longRange);
        }
        return userRangeSet;
    }

    static SerializableTimestampSet copyWithRange(SerializableTimestampSet base, Range<Long> newRange) {
        RangeSet<Long> newRangeSet = TreeRangeSet.create(base.rangeSetView());
        newRangeSet.add(newRange);
        return ImmutableSerializableTimestampSet.builder()
                .longRanges(newRangeSet.asRanges())
                .build();
    }
}
