package com.palantir.atlasdb.keyvalue.impl.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;

public class PartitionedKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return null;
//        return PartitionedKeyValueService.create();
    }

}
