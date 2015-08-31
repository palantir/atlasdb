package com.palantir.atlasdb.keyvalue.partition.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;

public class EndpointWithJoiningStatus extends EndpointWithStatus {

    @JsonCreator
    public EndpointWithJoiningStatus(@JsonProperty("endpoint") KeyValueEndpoint endpoint) {
        super(endpoint);
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
