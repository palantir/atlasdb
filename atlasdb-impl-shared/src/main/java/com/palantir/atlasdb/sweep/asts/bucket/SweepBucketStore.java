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

package com.palantir.atlasdb.sweep.asts.bucket;

import java.util.List;
import java.util.Optional;

public interface SweepBucketStore {
    /**
     * Retrieves the range of timestamps associated with the given bucket identifier, or an empty optional if the
     * bucket does not exist.
     */
    Optional<SweepableBucketRange> getSweepBucketRange(long bucketIdentifier);

    /**
     * Retrieves a list of buckets corresponding to the lowest buckets the sweep bucket store knows about, that are
     * at least as high as the provided AtlasDB timestamp.
     */
    List<Long> getFirstLiveBuckets(long lowerBoundTimestamp);

    /**
     * Closes the currently open bucket at the provided timestamp, if it is greater than the start timestamp of the
     * bucket. If it is not, is a no-op.
     */
    void appendSweepBucketRange(long timestamp);

    /**
     * Deletes a sweep bucket from the sweep bucket store. The onus is on the user to ensure that this is a safe
     * operation, and externally referenced state relating to the bucket in question has already been cleaned up.
     */
    void deleteSweepBucket(long bucketIdentifier);
}
