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
public interface ColumnRangeSelection {
    Optional<Integer> startColumnInclusive();

    Optional<Integer> endColumnExclusive();

    default boolean contains(int column) {
        return asGuavaRange().contains(column);
    }

    default Range<Integer> asGuavaRange() {
        if (startColumnInclusive().isEmpty()) {
            if (endColumnExclusive().isEmpty()) {
                return Range.all();
            } else {
                return Range.lessThan(endColumnExclusive().get());
            }
        } else {
            if (endColumnExclusive().isEmpty()) {
                return Range.atLeast(startColumnInclusive().get());
            } else {
                return Range.closedOpen(
                        startColumnInclusive().get(), endColumnExclusive().get());
            }
        }
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                startColumnInclusive().isEmpty()
                        || endColumnExclusive().isEmpty()
                        || startColumnInclusive().get() <= endColumnExclusive().get(),
                "Start column must be less than or equal to end column",
                SafeArg.of("startColumnInclusive", startColumnInclusive()),
                SafeArg.of("endColumnExclusive", endColumnExclusive()));
    }

    static ImmutableColumnRangeSelection.Builder builder() {
        return ImmutableColumnRangeSelection.builder();
    }
}
