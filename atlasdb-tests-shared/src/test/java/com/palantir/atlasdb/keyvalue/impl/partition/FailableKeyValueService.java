package com.palantir.atlasdb.keyvalue.impl.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface FailableKeyValueService {
    void shutdown();
    void resume();
    KeyValueService get();
}
