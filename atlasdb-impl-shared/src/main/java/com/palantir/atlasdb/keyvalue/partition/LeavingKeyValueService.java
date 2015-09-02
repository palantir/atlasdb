package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class LeavingKeyValueService extends KeyValueServiceWithStatus {

    public LeavingKeyValueService(KeyValueService service) {
        super(service);
    }

    @Override
    public boolean shouldUseForRead() {
        return true;
    }

    @Override
    public boolean shouldCountForRead() {
        return true;
    }

    @Override
    public boolean shouldUseForWrite() {
        // Since it is still being used for reads
        return true;
    }

    @Override
    public boolean shouldCountForWrite() {
        return false;
    }

}
