package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class JoiningKeyValueService extends KeyValueServiceWithStatus {

    public JoiningKeyValueService(KeyValueService service) {
        super(service);
    }

    @Override
    public boolean shouldUseForRead() {
        return false;
    }

    @Override
    public boolean shouldCountForRead() {
        return false;
    }

    @Override
    public boolean shouldUseForWrite() {
        return true;
    }

    @Override
    public boolean shouldCountForWrite() {
        return false;
    }

}
