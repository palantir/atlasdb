package com.palantir.atlasdb.keyvalue.partition;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

//@JsonTypeInfo(use=Id.CLASS, property="@class")
@JsonDeserialize(as=SimpleKeyValueEndpoint.class)
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();
}
