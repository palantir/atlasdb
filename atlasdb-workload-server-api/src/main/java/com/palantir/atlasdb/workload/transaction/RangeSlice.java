/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.transaction;

import com.google.common.collect.Range;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface RangeSlice {
    Optional<Integer> startInclusive();

    Optional<Integer> endExclusive();

    default boolean contains(int column) {
        return asGuavaRange().contains(column);
    }

    default Range<Integer> asGuavaRange() {
        if (startInclusive().isEmpty()) {
            if (endExclusive().isEmpty()) {
                return Range.all();
            } else {
                return Range.lessThan(endExclusive().get());
            }
        } else {
            if (endExclusive().isEmpty()) {
                return Range.atLeast(startInclusive().get());
            } else {
                return Range.closedOpen(
                        startInclusive().get(), endExclusive().get());
            }
        }
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                startInclusive().isEmpty()
                        || endExclusive().isEmpty()
                        || startInclusive().get() <= endExclusive().get(),
                "Start must be less than or equal to end",
                SafeArg.of("startInclusive", startInclusive()),
                SafeArg.of("endExclusive", endExclusive()));
    }

    static ImmutableRangeSlice.Builder builder() {
        return ImmutableRangeSlice.builder();
    }
}
