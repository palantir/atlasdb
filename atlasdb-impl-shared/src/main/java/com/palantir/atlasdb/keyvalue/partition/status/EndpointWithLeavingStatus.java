package com.palantir.atlasdb.keyvalue.partition.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;

public class EndpointWithLeavingStatus extends EndpointWithStatus {

    @JsonCreator
    public EndpointWithLeavingStatus(@JsonProperty("endpoint") KeyValueEndpoint endpoint) {
        super(endpoint);
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
