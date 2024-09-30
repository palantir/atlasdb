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
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.function.LongSupplier;

public class DefaultGlobalSweepProgressUpdatingTask implements GlobalSweepProgressUpdatingTask {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultGlobalSweepProgressUpdatingTask.class);

    private final LongSupplier sweepTimestampSupplier;
    private final SweepQueueCleaner sweepQueueCleaner;

    public DefaultGlobalSweepProgressUpdatingTask(
            LongSupplier sweepTimestampSupplier,
            SweepQueueCleaner sweepQueueCleaner) {
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.sweepQueueCleaner = sweepQueueCleaner;
    }

    @Override
    public void updateProgress(ShardAndStrategy shardAndStrategy) {
        long currentSweepTimestamp = sweepTimestampSupplier.getAsLong();
        sweepQueueCleaner.clean(shardAndStrategy, progressUpdate.completedFinePartitions(), progressUpdate.progressTimestamp());
    }
}
