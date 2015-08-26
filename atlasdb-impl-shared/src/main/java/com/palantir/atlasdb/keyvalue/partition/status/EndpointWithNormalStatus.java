package com.palantir.atlasdb.keyvalue.partition.status;

import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;

public class EndpointWithNormalStatus extends EndpointWithStatus {

    public EndpointWithNormalStatus(KeyValueEndpoint service) {
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
