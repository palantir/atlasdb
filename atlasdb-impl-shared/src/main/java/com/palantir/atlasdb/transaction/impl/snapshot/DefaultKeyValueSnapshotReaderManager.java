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

package com.palantir.atlasdb.transaction.impl.snapshot;

import com.palantir.atlasdb.cell.api.DataKeyValueService;
import com.palantir.atlasdb.cell.api.DataKeyValueServiceManager;
import com.palantir.atlasdb.transaction.api.DeleteExecutor;
import com.palantir.atlasdb.transaction.api.OrphanedSentinelDeleter;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReader;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.impl.ReadSentinelHandler;
import com.palantir.atlasdb.transaction.service.TransactionService;

public final class DefaultKeyValueSnapshotReaderManager implements KeyValueSnapshotReaderManager {
    private final DataKeyValueServiceManager dataKeyValueServiceManager;
    private final TransactionService transactionService;
    private final boolean allowHiddenTableAccess;
    private final OrphanedSentinelDeleter orphanedSentinelDeleter;
    private final DeleteExecutor deleteExecutor;

    public DefaultKeyValueSnapshotReaderManager(
            DataKeyValueServiceManager dataKeyValueServiceManager,
            TransactionService transactionService,
            boolean allowHiddenTableAccess,
            OrphanedSentinelDeleter orphanedSentinelDeleter,
            DeleteExecutor deleteExecutor) {
        this.dataKeyValueServiceManager = dataKeyValueServiceManager;
        this.transactionService = transactionService;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.orphanedSentinelDeleter = orphanedSentinelDeleter;
        this.deleteExecutor = deleteExecutor;
    }

    @Override
    public KeyValueSnapshotReader createKeyValueSnapshotReader(TransactionContext transactionContext) {
        DataKeyValueService dataKeyValueService =
                dataKeyValueServiceManager.getDataKeyValueService(transactionContext.startTimestampSupplier());
        return new DefaultKeyValueSnapshotReader(
                dataKeyValueService,
                transactionService,
                transactionContext.commitTimestampLoader(),
                allowHiddenTableAccess,
                // TODO (jkong): The allocations here feel wasteful. Should we have a cache of some kind?
                new ReadSentinelHandler(
                        dataKeyValueService,
                        transactionService,
                        transactionContext.transactionReadSentinelBehavior(),
                        orphanedSentinelDeleter),
                transactionContext.startTimestampSupplier(),
                transactionContext.readSnapshotValidator(),
                deleteExecutor,
                transactionContext.keyValueSnapshotMetricRecorder());
    }
}
