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

import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value.Immutable;

public interface SweepBucketsTable {
    Optional<SweepableBucket> getSweepableBucket(Bucket bucket);

    Set<SweepableBucket> getSweepableBuckets(Set<Bucket> startBuckets);

    void putTimestampRangeForBucket(Bucket bucket, TimestampRange timestampRange);

    @Immutable
    interface TimestampRange {
        long startInclusive();

        long endExclusive(); // Should this be inclusive?
    }
}
