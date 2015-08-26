package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface KeyValueEndpoint {
    <T> T run(Function<KeyValueService, T> task);
//    String getRack();
//    String getUri();
}
