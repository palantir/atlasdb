package com.palantir.atlasdb.keyvalue.partition.api;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface DynamicPartitionMap extends PartitionMap {
    void addEndpoint(byte[] key, KeyValueService kvs, String rack);
    void removeEndpoint(byte[] key, KeyValueService kvs, String rack);

    // Block until all scheduled removals complete.
    void syncRemovedEndpoints();

    // Check if all scheduled removals are finished.
    boolean removalInProgress();
}
