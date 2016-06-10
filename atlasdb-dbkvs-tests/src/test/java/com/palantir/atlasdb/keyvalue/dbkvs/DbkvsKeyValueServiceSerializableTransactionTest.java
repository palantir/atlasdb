package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class DbkvsKeyValueServiceSerializableTransactionTest extends
        AbstractSerializableTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return DbKvs.create(DbkvsTestSuite.POSTGRES_KVS_CONFIG);
    }
}
