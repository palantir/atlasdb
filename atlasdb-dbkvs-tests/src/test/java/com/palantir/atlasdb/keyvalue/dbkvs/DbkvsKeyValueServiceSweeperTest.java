package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.cleaner.AbstractSweeperTest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;

public class DbkvsKeyValueServiceSweeperTest extends AbstractSweeperTest {
    @Override
    protected KeyValueService getKeyValueService() {
        return DbKvs.create(DbkvsTestSuite.POSTGRES_KVS_CONFIG);
    }
}
