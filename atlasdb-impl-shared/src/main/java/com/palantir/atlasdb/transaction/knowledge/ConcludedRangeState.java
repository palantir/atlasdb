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

package com.palantir.atlasdb.transaction.knowledge;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableConcludedRangeState.class)
@JsonDeserialize(as = ImmutableConcludedRangeState.class)
@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public interface ConcludedRangeState {

    @Value.Parameter
    Long minimumConcludeableTimestamp();

    @Value.Derived
    default Range<Long> minimumConcludeableTimestampRange() {
        return Range.atLeast(minimumConcludeableTimestamp());
    }

    @Value.Parameter
    RangeSet<Long> timestampRanges();

    default boolean encloses(Range<Long> timestampRange) {
        return timestampRanges().encloses(timestampRange);
    }

    default ConcludedRangeState copyAndSetMinimumConcludeableTimestamp(long timestamp) {
        return ImmutableConcludedRangeState.builder()
                .from(this)
                .minimumConcludeableTimestamp(timestamp)
                .build();
    }

    default ConcludedRangeState copyAndAdd(Range<Long> additionalTimestampRange) {
        return copyAndAdd(ImmutableSet.of(additionalTimestampRange));
    }

    default ConcludedRangeState copyAndAdd(Set<Range<Long>> additionalTimestampRanges) {
        Set<Range<Long>> rangesToSupplement = additionalTimestampRanges.stream()
                .filter(Predicate.not(timestampRanges()::encloses))
                .filter(minimumConcludeableTimestampRange()::isConnected)
                .map(minimumConcludeableTimestampRange()::intersection)
                .filter(Predicate.not(Range::isEmpty))
                .collect(Collectors.toSet());

        if (rangesToSupplement.isEmpty()) {
            return this;
        }

        return ImmutableConcludedRangeState.builder()
                .timestampRanges(
                        ImmutableRangeSet.unionOf(Sets.union(timestampRanges().asRanges(), rangesToSupplement)))
                .minimumConcludeableTimestamp(minimumConcludeableTimestamp())
                .build();
    }

    static ConcludedRangeState singleRange(Range<Long> timestampRange, long minimumTimestamp) {
        return ImmutableConcludedRangeState.builder()
                .timestampRanges(ImmutableRangeSet.of(timestampRange))
                .minimumConcludeableTimestamp(minimumTimestamp)
                .build();
    }

    static ConcludedRangeState initRanges(Set<Range<Long>> timestampRanges, long minimumTimestamp) {
        return ImmutableConcludedRangeState.builder()
                .minimumConcludeableTimestamp(minimumTimestamp)
                .timestampRanges(ImmutableRangeSet.unionOf(timestampRanges))
                .build();
    }

    static ConcludedRangeState empty() {
        return ImmutableConcludedRangeState.builder()
                .timestampRanges(ImmutableRangeSet.of())
                .minimumConcludeableTimestamp(TransactionConstants.LOWEST_POSSIBLE_START_TS)
                .build();
    }
}
