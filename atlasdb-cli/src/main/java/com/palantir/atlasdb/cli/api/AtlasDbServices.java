package com.palantir.atlasdb.cli.api;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public interface AtlasDbServices {
    TimestampService getTimestampService();

    RemoteLockService getLockSerivce();

    KeyValueService getKeyValueService();

    SnapshotTransactionManager getTransactionManager();
}
