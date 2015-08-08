package com.palantir.atlasdb.keyvalue.rdbms;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.common.concurrent.PTExecutors;

public class RdbmsKeyValueTest extends AbstractAtlasDbKeyValueServiceTest {

    KeyValueService kvs;

    @Override
    protected KeyValueService getKeyValueService() {
        if (kvs == null) {
            KeyValueService ret = new RdbmsKeyValueService(PTExecutors.newCachedThreadPool());
            ret.initializeFromFreshInstance();
            kvs = ret;
        }
        return kvs;
    }

}
