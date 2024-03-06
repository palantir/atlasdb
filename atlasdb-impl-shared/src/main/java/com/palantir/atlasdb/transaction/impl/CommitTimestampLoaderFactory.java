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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class CommitTimestampLoaderFactory {
    private final TimestampCache timestampCache;
    private final MetricsManager metricsManager;
    private final TimelockService timelockService;
    private final TransactionKnowledgeComponents transactionKnowledgeComponents;

    public CommitTimestampLoaderFactory(
            TimestampCache timestampCache,
            MetricsManager metricsManager,
            TimelockService timelockService,
            TransactionKnowledgeComponents transactionKnowledgeComponents) {
        this.timestampCache = timestampCache;
        this.metricsManager = metricsManager;
        this.timelockService = timelockService;
        this.transactionKnowledgeComponents = transactionKnowledgeComponents;
    }

    public CommitTimestampLoader createCommitTimestampLoader(
            LongSupplier snapshotTimestampSupplier,
            long immutableTimestamp,
            Optional<LockToken> immutableTimestampLock,
            Supplier<TransactionConfig> transactionConfigSupplier) {
        return new DefaultCommitTimestampLoader(
                timestampCache,
                immutableTimestampLock,
                snapshotTimestampSupplier::getAsLong,
                transactionConfigSupplier,
                metricsManager,
                timelockService,
                immutableTimestamp,
                transactionKnowledgeComponents);
    }
}
