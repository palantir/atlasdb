package com.palantir.atlasdb.keyvalue.partition.status;

import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;

public class EndpointWithJoiningStatus extends EndpointWithStatus {

    public EndpointWithJoiningStatus(KeyValueEndpoint service) {
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
