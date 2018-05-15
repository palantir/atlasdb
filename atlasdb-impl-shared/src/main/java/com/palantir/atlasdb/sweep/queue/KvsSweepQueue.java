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
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class KvsSweepQueue implements MultiTableSweepQueueWriter {
    private final Supplier<Boolean> runSweep;
    private final Supplier<Integer> shardsConfig;
    @VisibleForTesting
    KvsSweepQueueTables tables;
    private KvsSweepDeleter deleter;
    private KvsSweepQueueScrubber scrubber;
    private SweepTimestampProvider timestampProvider;

    private KvsSweepQueue(Supplier<Boolean> runSweep, Supplier<Integer> shardsConfig) {
        this.runSweep = runSweep;
        this.shardsConfig = shardsConfig;
    }

    public static KvsSweepQueue createUninitialized(Supplier<Boolean> enabled, Supplier<Integer> shardsConfig) {
        return new KvsSweepQueue(enabled, shardsConfig);
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
        // todo(gmaretic): magic
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        tables.enqueue(writes);
    }

    @VisibleForTesting
    void sweepNextBatch(ShardAndStrategy shardStrategy) {
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
}
