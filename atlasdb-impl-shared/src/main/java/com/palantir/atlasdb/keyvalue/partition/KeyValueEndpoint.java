package com.palantir.atlasdb.keyvalue.partition;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

@JsonDeserialize(as=SimpleKeyValueEndpoint.class)
@JsonSerialize(as=SimpleKeyValueEndpoint.class)
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();
    void build(Supplier<Long> clientVersionSupplier);
}
