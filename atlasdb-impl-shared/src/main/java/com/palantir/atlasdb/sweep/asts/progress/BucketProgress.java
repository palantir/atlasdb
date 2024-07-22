/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.progress;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBucketProgress.class)
@JsonDeserialize(as = ImmutableBucketProgress.class)
public interface BucketProgress extends Comparable<BucketProgress> {
    long timestampOffset();

    long cellOffset();

    @Value.Check
    default void check() {
        Preconditions.checkState(
                timestampOffset() >= 0,
                "Timestamp offset must be non-negative",
                SafeArg.of("timestampOffset", timestampOffset()));
        Preconditions.checkState(
                timestampOffset() < SweepQueueUtils.TS_FINE_GRANULARITY,
                "Timestamp offset should not exceed the granularity of a fine partition.",
                SafeArg.of("timestampOffset", timestampOffset()));

        Preconditions.checkState(
                cellOffset() >= 0, "Timestamp offset must be non-negative", SafeArg.of("cellOffset", cellOffset()));
    }

    @Override
    default int compareTo(BucketProgress other) {
        int timestampOffsetComparison = Long.compare(timestampOffset(), other.timestampOffset());
        if (timestampOffsetComparison != 0) {
            return timestampOffsetComparison;
        } else {
            return Long.compare(cellOffset(), other.cellOffset());
        }
    }

    static BucketProgress createForTimestampOffset(long timestamp) {
        return ImmutableBucketProgress.builder()
                .timestampOffset(timestamp)
                .cellOffset(0L)
                .build();
    }

    static ImmutableBucketProgress.Builder builder() {
        return ImmutableBucketProgress.builder();
    }
}
