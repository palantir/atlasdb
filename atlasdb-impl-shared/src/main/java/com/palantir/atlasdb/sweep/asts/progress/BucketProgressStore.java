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

import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import java.util.Optional;

public interface BucketProgressStore {
    Optional<BucketProgress> getBucketProgress(SweepableBucket bucket);

    /**
     * If this method returned successfully, it is guaranteed that the stored bucket progress for the relevant bucket
     * is at least the minimum. There is no guarantee that any writes are actually performed (e.g., if the bucket
     * progress is already greater than the provided minimum).
     */
    void updateBucketProgressToAtLeast(SweepableBucket bucket, BucketProgress minimum);

    /**
     * Deletes the stored bucket progress for the relevant bucket. A consequence of this is that subsequent calls
     * to {@link #getBucketProgress(SweepableBucket)} may return empty, which could indicate that the bucket has not
     * been swept yet.
     *
     * This method is only expected to be used for cleanup, once the system is in a state where the relevant bucket
     * will no longer be considered for sweeping (short of specific manual actions).
     */
    void deleteBucketProgress(SweepableBucket bucket);
}
