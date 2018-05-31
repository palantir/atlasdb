/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

@SuppressWarnings({"FinalClass", "Not final for mocking in tests"})
public class TargetedSweeper implements MultiTableSweepQueueWriter {
    private static final Logger log = LoggerFactory.getLogger(TargetedSweeper.class);
    private final Supplier<Boolean> runSweep;
    private final Supplier<Integer> shardsConfig;

    private SweepQueue queue;
    private SpecialTimestampsSupplier timestampsSupplier;
    private BackgroundSweepScheduler conservativeScheduler;
    private BackgroundSweepScheduler thoroughScheduler;

    private volatile boolean isInitialized = false;

    private TargetedSweeper(Supplier<Boolean> runSweep, Supplier<Integer> shardsConfig,
            int conservativeThreads, int thoroughThreads) {
        this.runSweep = runSweep;
        this.shardsConfig = shardsConfig;
        this.conservativeScheduler = new BackgroundSweepScheduler(conservativeThreads,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        this.thoroughScheduler = new BackgroundSweepScheduler(thoroughThreads,
                TableMetadataPersistence.SweepStrategy.THOROUGH);
    }

    /**
     * Creates a targeted sweeper, without initializing any of the necessary resources. You must call the
     * {@link #initialize(SpecialTimestampsSupplier, KeyValueService)} method before any writes can be made to the
     * sweep queue, or before sweeping.
     *
     * @param enabled live reloadable config controlling whether background threads should perform targeted sweep.
     * @param shardsConfig live reloadable config specifying the desired number of shards. Since the number of shards
     * must never be reduced, this will be ignored if the persisted number of shards is greater.
     * @param conservativeThreads number of conservative threads to use for background targeted sweep.
     * @param thoroughThreads number of thorough threads to use for background targeted sweep.
     * @return returns an uninitialized targeted sweeper.
     */
    public static TargetedSweeper createUninitialized(Supplier<Boolean> enabled, Supplier<Integer> shardsConfig,
            int conservativeThreads, int thoroughThreads) {
        return new TargetedSweeper(enabled, shardsConfig, conservativeThreads, thoroughThreads);
    }

    /**
     * This method initializes all the resources necessary for the targeted sweeper. This method should only be called
     * once the kvs is ready.
     *
     * @param timestamps supplier of unreadable and immutable timestamps.
     * @param kvs key value service that must be already initialized.
     */
    public void initialize(SpecialTimestampsSupplier timestamps, KeyValueService kvs) {
        if (isInitialized) {
            return;
        }
        Preconditions.checkState(kvs.isInitialized(),
                "Attempted to initialize targeted sweeper with an uninitialized backing KVS.");
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
        queue = SweepQueue.create(kvs, shardsConfig);
        timestampsSupplier = timestamps;
        conservativeScheduler.scheduleBackgroundThreads();
        thoroughScheduler.scheduleBackgroundThreads();
        isInitialized = true;
    }

    @Override
    public void callbackInit(SerializableTransactionManager txManager) {
        initialize(SpecialTimestampsSupplier.create(txManager), txManager.getKeyValueService());
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        assertInitialized();
        queue.enqueue(writes);
    }

    /**
     * Sweeps the next batch for the given shard and strategy. If the sweep is successful, we delete the processed
     * writes from the sweep queue and then update the sweep queue progress accordingly.
     *
     * @param shardStrategy shard and strategy to use
     */
    @VisibleForTesting
    public void sweepNextBatch(ShardAndStrategy shardStrategy) {
        assertInitialized();
        if (!runSweep.get()) {
            return;
        }
        long maxTsExclusive = Sweeper.of(shardStrategy).getSweepTimestamp(timestampsSupplier);
        queue.sweepNextBatch(shardStrategy, maxTsExclusive);
    }

    @Override
    public void close() throws Exception {
        conservativeScheduler.close();
        thoroughScheduler.close();
    }

    private void assertInitialized() {
        if (!isInitialized) {
            throw new NotInitializedException("Targeted Sweeper");
        }
    }

    private class BackgroundSweepScheduler implements AutoCloseable {
        private final int numThreads;
        private final TableMetadataPersistence.SweepStrategy sweepStrategy;
        private final AtomicLong counter = new AtomicLong(0);
        private ScheduledExecutorService executorService;

        private BackgroundSweepScheduler(int numThreads, TableMetadataPersistence.SweepStrategy sweepStrategy) {
            this.numThreads = numThreads;
            this.sweepStrategy = sweepStrategy;
        }

        private void scheduleBackgroundThreads() {
            if (numThreads > 0) {
                executorService = PTExecutors
                        .newScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory("Targeted Sweep", true));
                for (int i = 0; i < numThreads; i++) {
                    executorService.scheduleWithFixedDelay(this::runOneIteration, 1, 5, TimeUnit.SECONDS);
                }
            }
        }

        private void runOneIteration() {
            ShardAndStrategy shardStrategy = ShardAndStrategy.of(getShardAndIncrement(), sweepStrategy);
            try {
                sweepNextBatch(shardStrategy);
            } catch (Throwable th) {
                log.error("Targeted sweep for {} failed and will be retried later.",
                        SafeArg.of("shardStrategy", shardStrategy.toText()), th);
            }
        }
        
        private int getShardAndIncrement() {
            return queue.modShards(counter.getAndIncrement());
        }

        @Override
        public void close() {
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }
}
