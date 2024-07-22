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
import java.util.function.Consumer;
import org.immutables.value.Value;

public interface SweepStateCoordinator {
    SweepOutcome tryRunTaskWithBucket(Consumer<SweepableBucket> task);

    enum SweepOutcome {
        NOTHING_AVAILABLE,
        NOTHING_TO_SWEEP,
        SWEPT;
    }

    @Value.Immutable
    @Safe
    abstract class SweepableBucket {
        @Value.Parameter
        public abstract ShardAndStrategy shardAndStrategy();

        // It's really just the fine partition, but we make it opaque so we can change it in the future
        // TODO: consider wrapping the long if we _do_ want to make it even more opaque
        // That said, code is going to depend on this being the fine partition ID anyway..., unless
        // we do box it and have a method that's something like finePartitionId()
        @Value.Parameter
        public abstract long bucketIdentifier();

        @Safe
        @Override
        public String toString() {
            return shardAndStrategy().toText() + " and partition " + bucketIdentifier();
        }

        public static SweepableBucket of(ShardAndStrategy shardAndStrategy, long bucketIdentifier) {
            return ImmutableSweepableBucket.of(shardAndStrategy, bucketIdentifier);
        }
    }
}
