/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup.transaction;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FullyBoundedTimestampRange {
    abstract Range<Long> delegate();

    @Value.Lazy
    public long inclusiveLowerBound() {
        return delegate().lowerBoundType().equals(BoundType.CLOSED)
                ? delegate().lowerEndpoint()
                : delegate().lowerEndpoint() + 1;
    }

    @Value.Lazy
    public long inclusiveUpperBound() {
        return delegate().upperBoundType().equals(BoundType.CLOSED)
                ? delegate().upperEndpoint()
                : delegate().upperEndpoint() - 1;
    }

    @Value.Lazy
    public boolean contains(long timestamp) {
        return delegate().contains(timestamp);
    }

    @Value.Check
    void isFullyBounded() {
        Preconditions.checkState(
                delegate().hasLowerBound() && delegate().hasUpperBound(),
                "Timestamp range is unbounded on at least one end",
                SafeArg.of("range", delegate()));
    }

    public static FullyBoundedTimestampRange of(Range<Long> delegate) {
        return ImmutableFullyBoundedTimestampRange.builder().delegate(delegate).build();
    }
}
