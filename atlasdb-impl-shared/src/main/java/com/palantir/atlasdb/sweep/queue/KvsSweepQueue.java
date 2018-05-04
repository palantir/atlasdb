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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class KvsSweepQueue implements MultiTableSweepQueueWriter {
    private final Supplier<Boolean> runSweep;
    private final Supplier<Integer> numShards;
    private KvsSweepQueuePersister writer;
    private Map<ShardAndStrategy, KvsSweepQueueReader> shardSpecificReaders;
    private Map<TableMetadataPersistence.SweepStrategy, KvsSweepDeleter> strategySpecificDeleters;
    private SweepTimestampProvider timestampProvider;

    private KvsSweepQueue(Supplier<Boolean> runSweep, Supplier<Integer> numShards) {
        this.runSweep = runSweep;
        // todo(gmaretic): this needs to replace the constant
        this.numShards = numShards;
    }

    public static KvsSweepQueue createUninitialized(Supplier<Boolean> enabled, Supplier<Integer> shards) {
        return new KvsSweepQueue(enabled, shards);
    }

    public void initialize(SweepTimestampProvider provider, KeyValueService kvs) {
        timestampProvider = provider;
        writer = KvsSweepQueuePersister.create(kvs);
        shardSpecificReaders = createReaders(kvs);
        strategySpecificDeleters = createDeleters(kvs);
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
        createAndStartBackgroundThreads();
    }

    @Override
    public void callbackInit(SerializableTransactionManager txManager) {
        initialize(SweepTimestampProvider.create(txManager), txManager.getKeyValueService());
    }

    private Map<ShardAndStrategy, KvsSweepQueueReader> createReaders(KeyValueService kvs) {
        long shards = numShards.get();
        Map<ShardAndStrategy, KvsSweepQueueReader> readers = new HashMap<>();
        for (int i = 0; i < shards; i++) {
            ShardAndStrategy conservative = ShardAndStrategy.conservative(i);
            ShardAndStrategy thorough = ShardAndStrategy.thorough(i);
            readers.put(conservative, new KvsSweepQueueReader(kvs, conservative));
            readers.put(thorough, new KvsSweepQueueReader(kvs, thorough));
        }
        return readers;
    }

    private Map<TableMetadataPersistence.SweepStrategy, KvsSweepDeleter> createDeleters(KeyValueService kvs) {
        return ImmutableMap.of(TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                new KvsSweepDeleter(kvs, Sweeper.CONSERVATIVE),
                TableMetadataPersistence.SweepStrategy.THOROUGH,
                new KvsSweepDeleter(kvs, Sweeper.THOROUGH));
    }

    private void createAndStartBackgroundThreads() {
        // todo(gmaretic): magic
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        writer.enqueue(writes);
    }

    @VisibleForTesting
    void sweepNextBatch(ShardAndStrategy shardAndStrategy) {
        if (!runSweep.get()) {
            return;
        }
        KvsSweepDeleter deleter = strategySpecificDeleters.get(shardAndStrategy.strategy());
        shardSpecificReaders.get(shardAndStrategy)
                .consumeNextBatch(deleter::sweep, timestampProvider.getSweepTimestamp(deleter.getSweeper()));
    }

    // todo(gmaretic): remove once scrubbers are implemented
    @VisibleForTesting
    void setScrubbers(KvsSweepQueueScrubber scrubber) {
        strategySpecificDeleters.values().forEach(kvsSweepDeleter -> kvsSweepDeleter.scrubber = scrubber);
    }
}
