package com.palantir.atlasdb.keyvalue.impl.partition;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.FailableKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;

public class AvailabilityTest extends AbstractAtlasDbKeyValueServiceTest {

    KeyValueService kvs;
    public static final int NUM_SERVICES = 4;
    static final FailableKeyValueService[] services = new FailableKeyValueService[4];

    int numBroken = 0;

    @Override
    protected KeyValueService getKeyValueService() {
        if (kvs == null) {
            createServices();
        }
        return kvs;
    }

    private void createServices() {
        for (int i = 0; i < NUM_SERVICES; ++i) {
            FailableKeyValueService service = FailableKeyValueService.wrap(new InMemoryKeyValueService(
                    false));
            services[i] = service;
        }
//        services[0].stop();
//        services[1].stop();
        services[2].stop();
//        services[3].stop();
        kvs = PartitionedKeyValueService.create(ImmutableSet.<FailableKeyValueService> copyOf(services));
    }

}
