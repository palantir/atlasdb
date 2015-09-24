package com.palantir.atlasdb.keyvalue.partition.api;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface DynamicPartitionMap extends PartitionMap {

    void addEndpoint(byte[] key, KeyValueService kvs, String rack);
    void removeEndpoint(byte[] key, KeyValueService kvs, String rack);
    
    /**
     * The initial version MUST be 0L!
     */
    long getVersion();

    /**
     * For testing purposes only. Will be removed soon.
     */
    @Deprecated void setVersion(long version);
}
