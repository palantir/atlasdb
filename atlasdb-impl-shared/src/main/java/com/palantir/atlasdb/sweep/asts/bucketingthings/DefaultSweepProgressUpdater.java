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

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepableTimestamps;

public final class DefaultSweepProgressUpdater {
    private final ShardProgress shardProgressStore;
    private final SweepableTimestamps sweepableTimestampsStore;
    private final SweepBucketPointerTable sweepBucketPointerTable;

    private DefaultSweepProgressUpdater(
            ShardProgress shardProgressStore,
            SweepableTimestamps sweepableTimestampsStore,
            SweepBucketPointerTable sweepBucketPointerTable) {
        this.shardProgressStore = shardProgressStore;
        this.sweepableTimestampsStore = sweepableTimestampsStore;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    public static DefaultSweepProgressUpdater create(
            ShardProgress shardProgressStore,
            SweepableTimestamps sweepableTimestampsStore,
            SweepBucketPointerTable sweepBucketPointerTable) {
        return new DefaultSweepProgressUpdater(shardProgressStore, sweepableTimestampsStore, sweepBucketPointerTable);
    }

    public void updateProgress(ShardAndStrategy shardAndStrategy) {
        // TODO: Find the last unswept timestamp by loading the _first_ live bucket, being careful of tombstones, using
        // the current timestamp? This seems messy
        // It feels we need to hupdate swep bucket pointers last, and actually use that to load the last swept
        // timestamp.
        // Since we otherwise don't update shard progress?
    }
}
