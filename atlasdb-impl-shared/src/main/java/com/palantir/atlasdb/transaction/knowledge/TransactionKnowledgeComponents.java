/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.palantir.atlasdb.internalschema.InternalSchemaInstallConfig;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.ConcludedTransactionsUpdaterTask;
import com.palantir.atlasdb.sweep.queue.LastSeenCommitTsLoader;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.transaction.knowledge.coordinated.CoordinationAwareKnownConcludedTransactionsStore;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.immutables.value.Value;

@Value.Immutable
public interface TransactionKnowledgeComponents {
    KnownConcludedTransactions concluded();

    KnownAbandonedTransactions abandoned();

    Supplier<Long> lastSeenCommitSupplier();

    static TransactionKnowledgeComponents createForTests(
            KeyValueService kvs, TaggedMetricRegistry metricRegistry, TransactionSchemaManager schemaManager) {
        return create(kvs, metricRegistry, InternalSchemaInstallConfig.getDefault(), () -> true, schemaManager);
    }

    static TransactionKnowledgeComponents create(
            KeyValueService kvs,
            TaggedMetricRegistry metricRegistry,
            InternalSchemaInstallConfig config,
            BooleanSupplier isInitialized,
            TransactionSchemaManager transactionSchemaManager) {
        ShardProgress shardProgress = new ShardProgress(kvs);
        LastSeenCommitTsLoader lastSeenCommitTsLoader = new LastSeenCommitTsLoader(shardProgress, isInitialized);
        KnownConcludedTransactions concluded =
                KnownConcludedTransactionsImpl.create(KnownConcludedTransactionsStore.create(kvs), metricRegistry);
        scheduleConcludedStoreUpdaterTask(transactionSchemaManager, concluded, shardProgress, isInitialized);

        return ImmutableTransactionKnowledgeComponents.builder()
                .concluded(concluded)
                .abandoned(KnownAbandonedTransactionsImpl.create(
                        concluded, new AbandonedTimestampStoreImpl(kvs), metricRegistry, config))
                .lastSeenCommitSupplier(lastSeenCommitTsLoader::getLastSeenCommitTs)
                .build();
    }

    private static void scheduleConcludedStoreUpdaterTask(
            TransactionSchemaManager transactionSchemaManager,
            KnownConcludedTransactions concluded,
            ShardProgress shardProgress,
            BooleanSupplier isInitialized) {
        CoordinationAwareKnownConcludedTransactionsStore concludedTransactionsStore =
                new CoordinationAwareKnownConcludedTransactionsStore(
                        transactionSchemaManager::getTimestampPartitioningMap, concluded);
        ConcludedTransactionsUpdaterTask.create(
                concludedTransactionsStore,
                shardProgress,
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("known-concluded-txn-updater-task")),
                isInitialized);
    }
}
