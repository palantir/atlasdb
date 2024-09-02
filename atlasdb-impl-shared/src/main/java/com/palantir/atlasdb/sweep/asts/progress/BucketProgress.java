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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBucketProgress.class)
@JsonDeserialize(as = ImmutableBucketProgress.class)
/**
 * Describes partial progress of Sweep within the context of a bucket.
 */
public interface BucketProgress {
    /**
     * Within this bucket, timestamps starting from 0 up to {@link #timestampProgress()} inclusive have been fully swept.
     * -1 can be used to indicate that no timestamps are fully swept yet (e.g., if we are just starting this bucket,
     * or if we want to express partial progress within the cells at the timestamp at the head of this bucket).
     */
    long timestampProgress();

    /**
     * Considering the cells at timestamp equivalent to {@link #timestampProgress()} + 1, the cells up to index
     * {@link #cellProgressForNextTimestamp()} have been swept. -1 can be used to indicate that no cells at the
     * next timestamp have been swept yet.
     */
    long cellProgressForNextTimestamp();

    @Value.Check
    default void check() {
        Preconditions.checkState(
                timestampProgress() >= -1,
                "Timestamp progress must be non-negative, or -1 (to indicate no timestamps are fully swept yet)",
                SafeArg.of("timestampProgress", timestampProgress()));

        Preconditions.checkState(
                cellProgressForNextTimestamp() >= -1,
                "Cell progress for next timestamp must be non-negative, or -1 (to indicate no cells at the following"
                        + " timestamp have been swept yet)",
                SafeArg.of("cellProgressForNextTimestamp", cellProgressForNextTimestamp()));
    }

    static BucketProgress createForTimestampProgress(long timestamp) {
        return ImmutableBucketProgress.builder()
                .timestampProgress(timestamp)
                .cellProgressForNextTimestamp(-1L)
                .build();
    }

    static ImmutableBucketProgress.Builder builder() {
        return ImmutableBucketProgress.builder();
    }
}
