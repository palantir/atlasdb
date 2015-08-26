package com.palantir.atlasdb.keyvalue.partition.status;

import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;

public class EndpointWithLeavingStatus extends EndpointWithStatus {

    public EndpointWithLeavingStatus(KeyValueEndpoint service) {
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
