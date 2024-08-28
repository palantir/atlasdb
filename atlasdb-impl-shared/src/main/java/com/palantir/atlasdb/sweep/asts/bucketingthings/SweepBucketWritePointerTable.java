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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable.TimestampRange;
import org.immutables.value.Value;

// TODO: An equally poor name?
public interface SweepBucketWritePointerTable {
    void updateHighestBucketNumber(long expectedBucketNumber, long newBucketNumber);

    void updateLastTimestampForBucket(TimestampForBucket original, TimestampForBucket updated);

    long getHighestBucketNumber();

    TimestampForBucket getLastTimestampForBucket();

    @Value.Immutable
    interface TimestampForBucket {
        long bucketIdentifier();

        TimestampRange timestampRange();

        SweepBucketState state();
    }

    // A state machine. We go from START X -> OPEN -> WAITING_CLOSE -> CLOSE_FROM_OPEN -> START X + 1
    // Or, START X -> IMMEDIATE_CLOSE -> START X + 1

    enum SweepBucketState {
        START,
        OPEN,
        WAITING_CLOSE,
        CLOSE_FROM_OPEN,
        IMMEDIATE_CLOSE,
    }
}
