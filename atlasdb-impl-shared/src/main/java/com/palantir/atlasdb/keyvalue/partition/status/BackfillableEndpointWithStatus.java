package com.palantir.atlasdb.keyvalue.partition.status;

import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;

public abstract class BackfillableEndpointWithStatus extends EndpointWithStatus {

    public BackfillableEndpointWithStatus(KeyValueEndpoint endpoint) {
        super(endpoint);
    }

    private boolean backfilled = false;

    public boolean backfilled() {
        return backfilled;
    }

    public void setBackfilled() {
        backfilled = true;
    }

}
