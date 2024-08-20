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

package com.palantir.atlasdb.sweep.asts;

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.logsafe.Safe;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Bucket implements Comparable<Bucket> {
    @Value.Parameter
    public abstract ShardAndStrategy shardAndStrategy();

    // It's really just the fine partition, but we make it opaque so we can change it in the future
    @Value.Parameter
    public abstract long bucketIdentifier();

    @Safe
    @Override
    public String toString() {
        return shardAndStrategy().toText() + " and partition " + bucketIdentifier();
    }

    @Override
    public int compareTo(Bucket other) {
        int shardComparison = Integer.compare(
                shardAndStrategy().shard(), other.shardAndStrategy().shard());
        if (shardComparison != 0) {
            return shardComparison;
        }
        return Long.compare(bucketIdentifier(), other.bucketIdentifier());
        // We're explicitly not comparing TimestampRange.
    }

    public static Bucket of(ShardAndStrategy shardAndStrategy, long bucketIdentifier) {
        return ImmutableBucket.of(shardAndStrategy, bucketIdentifier);
    }
}
