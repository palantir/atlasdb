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

import com.palantir.atlasdb.cell.api.DataKeyValueServiceManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.DelegatingDataKeyValueServiceManager;
import com.palantir.atlasdb.transaction.api.DeleteExecutor;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.impl.snapshot.DefaultKeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.service.TransactionService;

public interface TestKeyValueSnapshotReaderManagers {

    static KeyValueSnapshotReaderManager createForTests(
            KeyValueService keyValueService,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            DeleteExecutor deleteExecutor) {
        return new DefaultKeyValueSnapshotReaderManager(
                new DelegatingDataKeyValueServiceManager(keyValueService),
                transactionService,
                false,
                new DefaultOrphanedSentinelDeleter(sweepStrategyManager::get, deleteExecutor),
                deleteExecutor);
    }

    static KeyValueSnapshotReaderManager createForTests(
            DataKeyValueServiceManager dataKeyValueServiceManager,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            DeleteExecutor deleteExecutor) {
        return new DefaultKeyValueSnapshotReaderManager(
                dataKeyValueServiceManager,
                transactionService,
                false,
                new DefaultOrphanedSentinelDeleter(sweepStrategyManager::get, deleteExecutor),
                deleteExecutor);
    }
}
