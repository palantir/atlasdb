package com.palantir.atlasdb.shell;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.timestamp.TimestampService;

public interface AtlasShellContextFactory {
    AtlasContext withShellAwareReadOnlyTransactionManagerFromDb(String host,
                                                                String port,
                                                                String sid,
                                                                String type,
                                                                String username,
                                                                String password);

    AtlasContext withSnapshotTransactionManagerFromDispatchPrefs();

    AtlasContext withSnapshotTransactionManagerFromDispatch(String host,
                                                            String port,
                                                            String user,
                                                            String pass);

    AtlasContext withShellAwareReadOnlyTransactionManagerInMemory();

    AtlasContext withSnapshotTransactionManagerInMemory();

    AtlasContext withShellAwareReadOnlyTransactionManager(KeyValueService keyValueService,
                                                          TransactionService transactionService);

    AtlasContext withSnapshotTransactionManager(KeyValueService keyValueService,
                                                TransactionService transactionService,
                                                LockClient lockClient,
                                                LockService lockService,
                                                TimestampService timestampService);
}
