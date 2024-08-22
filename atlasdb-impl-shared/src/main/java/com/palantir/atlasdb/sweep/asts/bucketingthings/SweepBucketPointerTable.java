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
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import java.util.Set;

// TODO: This feels like a terrible name, but oddly I cannot think of another one.
//  StartingBucketsForShardStartStore?
public interface SweepBucketPointerTable {
    Set<Bucket> getStartingBucketsForShards(Set<ShardAndStrategy> shardAndStrategies);

    void updateStartingBucketForShardAndStrategy(Bucket newStartingBucket);
}
