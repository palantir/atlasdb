package com.palantir.atlasdb.shell;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public interface AtlasContext {
    KeyValueService getKeyValueService();

    TransactionManager getTransactionManager();
}
