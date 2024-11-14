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

import com.palantir.atlasdb.sweep.asts.TimestampRange;
import java.util.Optional;

public interface SweepBucketRecordsTable {
    /**
     * Returns a {@link TimestampRange} for the given bucket identifier, if one exists. If the record is present, then
     * the bucket is definitely closed. If the record is not present, the bucket is either open or closed (the record
     * may simply not have been written yet).
     */
    Optional<TimestampRange> getTimestampRangeRecord(long bucketIdentifier);

    void putTimestampRangeRecord(long bucketIdentifier, TimestampRange timestampRange);

    void deleteTimestampRangeRecord(long bucketIdentifier);
}
