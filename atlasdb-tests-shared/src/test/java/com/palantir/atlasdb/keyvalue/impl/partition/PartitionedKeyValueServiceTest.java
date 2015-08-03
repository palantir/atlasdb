package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;

public class PartitionedKeyValueServiceTest extends AbstractAtlasDbKeyValueServiceTest {

    @Override
    protected KeyValueService getKeyValueService() {
        Set<KeyValueService> svcs = Sets.newHashSet();
        for (int i=0; i<5; ++i) {
            svcs.add(new InMemoryKeyValueService(false));
        }
        return PartitionedKeyValueService.create(svcs);
    }

}
