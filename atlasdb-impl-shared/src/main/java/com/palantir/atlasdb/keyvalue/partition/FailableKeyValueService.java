package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface FailableKeyValueService {
    void shutdown();
    void resume();
    KeyValueService get();
}
