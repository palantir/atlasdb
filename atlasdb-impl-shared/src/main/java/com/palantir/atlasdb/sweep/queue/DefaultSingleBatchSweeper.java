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

import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public class DefaultSingleBatchSweeper implements SingleBatchSweeper {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSingleBatchSweeper.class);
    private final TargetedSweepMetrics metrics;
    private final ShardProgress progress;
    private final AbandonedTransactionConsumer abandonedTransactionConsumer;

    private final SweepQueueReader reader;
    private final SweepQueueDeleter deleter;
    private final SweepQueueCleaner cleaner;

    public DefaultSingleBatchSweeper(
            TargetedSweepMetrics metrics,
            ShardProgress progress,
            AbandonedTransactionConsumer abandonedTransactionConsumer,
            SweepQueueReader reader,
            SweepQueueDeleter deleter,
            SweepQueueCleaner cleaner) {
        this.metrics = metrics;
        this.progress = progress;
        this.abandonedTransactionConsumer = abandonedTransactionConsumer;
        this.reader = reader;
        this.deleter = deleter;
        this.cleaner = cleaner;
    }

    /**
     * Sweep the next batch for the shard and strategy specified by shardStrategy, with the sweep timestamp sweepTs.
     * After successful deletes, the persisted information about the writes is removed, and progress is updated
     * accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @param sweepTs       sweep timestamp, the upper limit to the start timestamp of writes to sweep
     * @return number of cells that were swept
     */
    @Override
    public long sweepNextBatch(ShardAndStrategy shardStrategy, long sweepTs) {
        metrics.updateSweepTimestamp(shardStrategy, sweepTs);
        long lastSweptTs = progress.getLastSweptTimestamp(shardStrategy);

        if (lastSweptTs + 1 >= sweepTs) {
            return 0L;
        }

        log.debug(
                "Beginning iteration of targeted sweep for {}, and sweep timestamp {}. Last previously swept "
                        + "timestamp for this shard and strategy was {}.",
                SafeArg.of("shardStrategy", shardStrategy.toText()),
                SafeArg.of("sweepTs", sweepTs),
                SafeArg.of("lastSweptTs", lastSweptTs));

        SweepBatchWithPartitionInfo batchWithInfo = reader.getNextBatchToSweep(shardStrategy, lastSweptTs, sweepTs);
        SweepBatch sweepBatch = batchWithInfo.sweepBatch();

        // The order must not be changed without considering correctness of txn4
        abandonedTransactionConsumer.accept(sweepBatch.abortedTimestamps());

        // Update last seen commit timestamp
        progress.updateLastSeenCommitTimestamp(shardStrategy, sweepBatch.lastSeenCommitTimestamp());
        metrics.updateLastSeenCommitTs(sweepBatch.lastSeenCommitTimestamp());

        deleter.sweep(sweepBatch.writes(), Sweeper.of(shardStrategy));
        metrics.registerEntriesReadInBatch(shardStrategy, sweepBatch.entriesRead());

        if (!sweepBatch.isEmpty()) {
            log.debug(
                    "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardStrategy.toText()));
        }

        cleaner.cleanAndUpdateProgress(
                shardStrategy,
                batchWithInfo.partitionsForPreviousLastSweptTs(lastSweptTs),
                sweepBatch.lastSweptTimestamp(),
                sweepBatch.dedicatedRows());

        metrics.updateNumberOfTombstones(shardStrategy, sweepBatch.writes().size());

        if (sweepBatch.isEmpty()) {
            metrics.registerOccurrenceOf(shardStrategy, SweepOutcome.NOTHING_TO_SWEEP);
        } else {
            metrics.registerOccurrenceOf(shardStrategy, SweepOutcome.SUCCESS);
        }

        return sweepBatch.entriesRead();
    }
}
