package com.palantir.atlasdb.keyvalue.partition;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class RegularKeyValueService extends KeyValueServiceWithStatus {

    public RegularKeyValueService(KeyValueService service) {
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
        return true;
    }

    @Override
    public boolean shouldCountForWrite() {
        return true;
    }

}
