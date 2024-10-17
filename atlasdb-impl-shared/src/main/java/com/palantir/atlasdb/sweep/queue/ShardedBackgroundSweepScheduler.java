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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.lock.v2.TimelockService;
import java.util.function.Supplier;

final class ShardedBackgroundSweepScheduler implements BackgroundSweeper {
    private final ShardedStrategySpecificBackgroundSweepScheduler conservativeScheduler;
    private final ShardedStrategySpecificBackgroundSweepScheduler thoroughScheduler;

    private ShardedBackgroundSweepScheduler(
            ShardedStrategySpecificBackgroundSweepScheduler conservativeScheduler,
            ShardedStrategySpecificBackgroundSweepScheduler thoroughScheduler) {
        this.conservativeScheduler = conservativeScheduler;
        this.thoroughScheduler = thoroughScheduler;
    }

    public static ShardedBackgroundSweepScheduler create(
            int numConservativeThreads,
            int numThoroughThreads,
            TimelockService timeLock,
            SweepQueue queue,
            NumberOfShardsProvider numberOfShardsProvider,
            SingleBatchSweeper sweeper,
            SpecialTimestampsSupplier timestampsSupplier,
            TargetedSweepMetrics metrics,
            Supplier<TargetedSweepRuntimeConfig> runtime) {
        ShardedStrategySpecificBackgroundSweepScheduler conservativeScheduler =
                ShardedStrategySpecificBackgroundSweepScheduler.create(
                        numConservativeThreads,
                        SweeperStrategy.CONSERVATIVE,
                        timeLock,
                        queue,
                        numberOfShardsProvider,
                        sweeper,
                        timestampsSupplier,
                        metrics,
                        runtime);
        ShardedStrategySpecificBackgroundSweepScheduler thoroughScheduler =
                ShardedStrategySpecificBackgroundSweepScheduler.create(
                        numThoroughThreads,
                        SweeperStrategy.THOROUGH,
                        timeLock,
                        queue,
                        numberOfShardsProvider,
                        sweeper,
                        timestampsSupplier,
                        metrics,
                        runtime);
        return new ShardedBackgroundSweepScheduler(conservativeScheduler, thoroughScheduler);
    }

    @Override
    public void runInBackground() {
        conservativeScheduler.scheduleBackgroundThreads();
        thoroughScheduler.scheduleBackgroundThreads();
    }

    @Override
    public void shutdown() {
        conservativeScheduler.close();
        thoroughScheduler.close();
    }
}
