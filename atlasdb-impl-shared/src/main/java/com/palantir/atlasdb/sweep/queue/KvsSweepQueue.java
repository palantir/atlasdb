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
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

public class KvsSweepQueue implements MultiTableSweepQueueWriter {
    private final Supplier<Boolean> runSweep;
    private final Supplier<Integer> shardsConfig;
    int conservativeThreads;
    int thoroughThreads;

    @VisibleForTesting
    KvsSweepQueueTables tables;
    private KvsSweepDeleter deleter;
    private KvsSweepQueueScrubber scrubber;
    private SweepTimestampProvider timestampProvider;
    private ScheduledExecutorService conservativeScheduler;
    private ScheduledExecutorService thoroughScheduler;
    private final AtomicLong conservativeCounter = new AtomicLong(0);
    private final AtomicLong thoroughCounter = new AtomicLong(0);

    private KvsSweepQueue(Supplier<Boolean> runSweep, Supplier<Integer> shardsConfig,
            int conservativeThreads, int thoroughThreads) {
        this.runSweep = runSweep;
        this.shardsConfig = shardsConfig;
        this.conservativeThreads = conservativeThreads;
        this.thoroughThreads = thoroughThreads;
    }

    public static KvsSweepQueue createUninitialized(Supplier<Boolean> enabled, Supplier<Integer> shardsConfig,
            int conservativeThreads, int thoroughThreads) {
        return new KvsSweepQueue(enabled, shardsConfig, conservativeThreads, thoroughThreads);
    }

    @VisibleForTesting
    @Deprecated
    public static KvsSweepQueue createInitializedForTest(KeyValueService kvs, int shards,
            LongSupplier unreadable, LongSupplier immutable) {
        KvsSweepQueue queue = KvsSweepQueue.createUninitialized(() -> true, () -> shards, 0, 0);
        queue.initialize(new SweepTimestampProvider(unreadable, immutable), kvs);
        return queue;
    }

    public void initialize(SweepTimestampProvider provider, KeyValueService kvs) {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
        tables = KvsSweepQueueTables.create(kvs, shardsConfig);
        deleter = new KvsSweepDeleter(kvs);
        scrubber = new KvsSweepQueueScrubber(tables);
        timestampProvider = provider;
        createAndStartBackgroundThreads();
    }

    @Override
    public void callbackInit(SerializableTransactionManager txManager) {
        initialize(SweepTimestampProvider.create(txManager), txManager.getKeyValueService());
    }

    private void createAndStartBackgroundThreads() {
        if (conservativeThreads > 0) {
            conservativeScheduler = PTExecutors.newScheduledThreadPoolExecutor(conservativeThreads,
                    new NamedThreadFactory("Conservative Targeted Sweep", false));
            for (int i = 0; i < conservativeThreads; i++) {
                conservativeScheduler.scheduleWithFixedDelay(
                        () -> sweepNextBatch(ShardAndStrategy.conservative(getNextShard(conservativeCounter))),
                        1, 5, TimeUnit.SECONDS);
            }
        }
        if (thoroughThreads > 0) {
            thoroughScheduler = PTExecutors.newScheduledThreadPoolExecutor(thoroughThreads,
                    new NamedThreadFactory("Thorough Targeted Sweep", false));
            for (int i = 0; i < conservativeThreads; i++) {
                thoroughScheduler.scheduleWithFixedDelay(
                        () -> sweepNextBatch(ShardAndStrategy.thorough(getNextShard(thoroughCounter))),
                        1, 5, TimeUnit.SECONDS);
            }
        }
    }

    int getNextShard(AtomicLong counter) {
        return tables.modShards(counter.getAndIncrement());
    }
    @Override
    public void enqueue(List<WriteInfo> writes) {
        tables.enqueue(writes);
    }

    @VisibleForTesting
    public void sweepNextBatch(ShardAndStrategy shardStrategy) {
        if (!runSweep.get()) {
            return;
        }
        Sweeper sweeper = Sweeper.of(shardStrategy.strategy()).get();
        long lastSweptTimestamp = tables.getLastSweptTimestamp(shardStrategy);
        long maxTsExclusive = timestampProvider.getSweepTimestamp(sweeper);
        SweepBatch sweepBatch = tables.getNextBatchAndSweptTimestamp(shardStrategy, lastSweptTimestamp, maxTsExclusive);

        deleter.sweep(sweepBatch.writes(), sweeper);
        scrubber.scrub(shardStrategy, lastSweptTimestamp, sweepBatch.lastSweptTimestamp());
    }

    @Override
    public void close() {
        if (conservativeScheduler != null) {
            conservativeScheduler.shutdown();
        }
        if (thoroughScheduler != null) {
            thoroughScheduler.shutdown();
        }
    }
}
