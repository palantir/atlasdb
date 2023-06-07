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

import java.util.Optional;

public final class ColumnRangeSelection {
    private final RangeSlice rangeSlice;

    public ColumnRangeSelection(RangeSlice rangeSlice) {
        this.rangeSlice = rangeSlice;
    }

    public Optional<Integer> startColumnInclusive() {
        return rangeSlice.startInclusive();
    }

    public Optional<Integer> endColumnExclusive() {
        return rangeSlice.endExclusive();
    }

    public boolean contains(int column) {
        return rangeSlice.contains(column);
    }

    public static ColumnRangeSelection.Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<Integer> startColumnInclusive = Optional.empty();
        private Optional<Integer> endColumnExclusive = Optional.empty();

        private Builder() {
            // Use the static factory method
        }

        public Builder startColumnInclusive(int startColumnInclusive) {
            this.startColumnInclusive = Optional.of(startColumnInclusive);
            return this;
        }

        public Builder endColumnExclusive(int endColumnExclusive) {
            this.endColumnExclusive = Optional.of(endColumnExclusive);
            return this;
        }

        public ColumnRangeSelection build() {
            return new ColumnRangeSelection(ImmutableRangeSlice.builder()
                    .startInclusive(startColumnInclusive)
                    .endExclusive(endColumnExclusive)
                    .build());
        }
    }
}
