package com.palantir.atlasdb.keyvalue.impl.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.partition.FailableKeyValueServices;

public class PartitionedKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return FailableKeyValueServices.sampleFailingKeyValueService();
    }

}
