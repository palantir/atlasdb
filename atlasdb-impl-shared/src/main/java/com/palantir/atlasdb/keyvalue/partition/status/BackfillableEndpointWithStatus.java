package com.palantir.atlasdb.keyvalue.partition.status;

import com.google.common.base.Preconditions;
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
        Preconditions.checkState(!backfilled);
        backfilled = true;
    }

}
