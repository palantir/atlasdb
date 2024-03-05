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

import com.palantir.atlasdb.transaction.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class DefaultKeyValueSnapshotReaderFactory implements KeyValueSnapshotReaderFactory {
    private final TransactionKeyValueServiceManager transactionKeyValueServiceManager;
    private final TransactionService transactionService;
    private final boolean allowHiddenTableAccess;
    private final OrphanedSentinelDeleter orphanedSentinelDeleter;
    private final DeleteExecutor deleteExecutor;

    public DefaultKeyValueSnapshotReaderFactory(
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TransactionService transactionService,
            boolean allowHiddenTableAccess,
            OrphanedSentinelDeleter orphanedSentinelDeleter,
            DeleteExecutor deleteExecutor) {
        this.transactionKeyValueServiceManager = transactionKeyValueServiceManager;
        this.transactionService = transactionService;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.orphanedSentinelDeleter = orphanedSentinelDeleter;
        this.deleteExecutor = deleteExecutor;
    }

    @Override
    public String getType() {
        return "default";
    }

    @Override
    public KeyValueSnapshotReader createKeyValueSnapshotReader(TransactionContext transactionContext) {
        return new DefaultKeyValueSnapshotReader(
                transactionKeyValueServiceManager.getTransactionKeyValueService(
                        transactionContext.startTimestampSupplier()),
                transactionService,
                transactionContext.commitTimestampLoader(),
                allowHiddenTableAccess,
                // TODO (jkong): The allocations here feel wasteful. Should we have a cache of some kind?
                new ReadSentinelHandler(
                        transactionService,
                        transactionContext.transactionReadSentinelBehavior(),
                        orphanedSentinelDeleter),
                transactionContext.startTimestampSupplier(),
                (tableReference, timestampSupplier, allCellsReadAndPresent) -> transactionContext
                        .preCommitRequirementValidator()
                        .throwIfPreCommitRequirementsNotMetOnRead(
                                tableReference, timestampSupplier.getAsLong(), allCellsReadAndPresent),
                deleteExecutor,
                transactionContext.keyValueSnapshotEventRecorder());
    }
}
