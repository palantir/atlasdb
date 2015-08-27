package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

/**
 * This cannot be serialized and should be used for test purposes only.
 *
 * @author htarasiuk
 *
 */
public class InMemoryKeyValueEndpoint implements KeyValueEndpoint {

    final KeyValueService kvs;
    final PartitionMapService pms;

    private InMemoryKeyValueEndpoint(KeyValueService kvs, PartitionMapService pms) {
        this.kvs = kvs;
        this.pms = pms;
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



}
