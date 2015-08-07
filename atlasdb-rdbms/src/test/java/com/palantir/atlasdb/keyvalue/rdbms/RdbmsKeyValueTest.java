package com.palantir.atlasdb.keyvalue.rdbms;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.common.concurrent.PTExecutors;

public class RdbmsKeyValueTest extends AbstractAtlasDbKeyValueServiceTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return new RdbmsKeyValueService(PTExecutors.newCachedThreadPool());
    }

}
