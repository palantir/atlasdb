package com.palantir.atlas.impl;

import javax.inject.Singleton;

import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

public class HiddenTableTxManagerModule implements TxManagerModule {
    @Bind
    @Singleton
    ReentrantTransactionManager bindTxManager(KeyValueService keyValueService,
                                              ConflictDetectionManager conflictDetectionManager,
                                              TimestampService timestampService,
                                              LockClient lockClient,
                                              LockService lockService,
                                              TransactionService transactionService,
                                              Cleaner cleaner,
                                              AtlasConfig config) {
        SnapshotTransactionManager transactionManager = Connectors.createTxManager(
                keyValueService,
                conflictDetectionManager,
                SweepStrategyManagers.createDefault(keyValueService),
                timestampService,
                lockClient,
                lockService,
                transactionService,
                cleaner,
                config,
                true);
        return Connectors.wrapTxManager(transactionManager, config);
    }
}
