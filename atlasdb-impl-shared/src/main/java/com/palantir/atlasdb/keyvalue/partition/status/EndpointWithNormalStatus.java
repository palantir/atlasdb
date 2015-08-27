package com.palantir.atlasdb.keyvalue.partition.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;

public class EndpointWithNormalStatus extends EndpointWithStatus {

    @JsonCreator
    public EndpointWithNormalStatus(@JsonProperty("endpoint") KeyValueEndpoint endpoint) {
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
        return true;
    }

    @Override
    public boolean shouldCountForWrite() {
        return true;
    }

}
