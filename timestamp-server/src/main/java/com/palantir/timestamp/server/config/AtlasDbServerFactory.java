package com.palantir.timestamp.server.config;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.TimestampService;

public interface AtlasDbServerFactory {
    KeyValueService getKeyValueService();

    Supplier<TimestampService> getTimestampSupplier();

    SerializableTransactionManager getTransactionManager();
}