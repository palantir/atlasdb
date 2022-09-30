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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.internalschema.InternalSchemaInstallConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.SweepQueue.SweepQueueFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

@Value.Immutable
public interface TransactionKnowledgeComponents {
    KnownConcludedTransactions concluded();

    KnownAbortedTransactions aborted();

    Supplier<Long> lastSeenCommitSupplier();

    static TransactionKnowledgeComponents createForTests(KeyValueService kvs, TaggedMetricRegistry metricRegistry) {
        return create(kvs, metricRegistry, InternalSchemaInstallConfig.getDefault());
    }

    static TransactionKnowledgeComponents create(
            KeyValueService kvs, TaggedMetricRegistry metricRegistry, InternalSchemaInstallConfig config) {

        return ImmutableTransactionKnowledgeComponents.builder()
                .concluded(KnownConcludedTransactionsImpl.create(
                        KnownConcludedTransactionsStore.create(kvs), metricRegistry))
                .aborted(KnownAbortedTransactionsImpl.create(
                        KnownConcludedTransactionsImpl.create(
                                KnownConcludedTransactionsStore.create(kvs), metricRegistry),
                        new DefaultAbandonedTimestampStore(kvs),
                        metricRegistry,
                        config))
                .lastSeenCommitSupplier(Suppliers.memoizeWithExpiration(
                        () -> SweepQueueFactory.getGetLastSeenCommitTs(kvs), 20, TimeUnit.SECONDS))
                .build();
    }
}
