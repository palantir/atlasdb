package com.palantir.atlasdb.keyvalue.partition;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

@JsonTypeInfo(use=Id.CLASS, property="@class")
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();
    <T> T run(Function<KeyValueService, T> task);
}
