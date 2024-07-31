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

import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;

public final class DefaultShardedSweepTimestampManager implements ShardedSweepTimestampManager {
    private final SpecialTimestampsSupplier specialTimestampsSupplier;
    private final ShardProgress shardProgress;

    private DefaultShardedSweepTimestampManager(
            SpecialTimestampsSupplier specialTimestampsSupplier, ShardProgress shardProgress) {
        this.specialTimestampsSupplier = specialTimestampsSupplier;
        this.shardProgress = shardProgress;
    }

    public static ShardedSweepTimestampManager create(
            SpecialTimestampsSupplier specialTimestampsSupplier, ShardProgress progress) {
        return new DefaultShardedSweepTimestampManager(specialTimestampsSupplier, progress);
    }

    @Override
    public SweepTimestamps getSweepTimestamps(ShardAndStrategy shardAndStrategy) {
        return SweepTimestamps.builder()
                .sweepTimestamp(Sweeper.of(shardAndStrategy).getSweepTimestamp(specialTimestampsSupplier))
                .lastSweptTimestamp(shardProgress.getLastSweptTimestamp(shardAndStrategy))
                .build();
    }
}
