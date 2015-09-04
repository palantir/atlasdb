package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This cannot be serialized and should be used for test purposes only.
 * The advantage of this impl is that it can be run with local service
 * instances. You do not need to remote the services over HTTP.
 *
 * @author htarasiuk
 *
 */
public class InMemoryKeyValueEndpoint implements KeyValueEndpoint {

    transient KeyValueService kvs;
    transient final PartitionMapService pms;

    private InMemoryKeyValueEndpoint(KeyValueService kvs, PartitionMapService pms) {
        this.kvs = Preconditions.checkNotNull(kvs);
        this.pms = Preconditions.checkNotNull(pms);
    }

    public static InMemoryKeyValueEndpoint create(KeyValueService kvs, PartitionMapService pms) {
        return new InMemoryKeyValueEndpoint(kvs, pms);
    }

    @Override
    public KeyValueService keyValueService() {
        return kvs;
    }

    @Override
    public PartitionMapService partitionMapService() {
        return pms;
    }

    @Override
    public void registerPartitionMapVersion(Supplier<Long> clientVersionSupplier) {
        // No-op
    }

}
