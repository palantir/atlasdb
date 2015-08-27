package com.palantir.atlasdb.keyvalue.partition.status;

import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;


public abstract class EndpointWithStatus {
    final KeyValueEndpoint endpoint;
    public final KeyValueEndpoint get() {
        return endpoint;
    }
    public EndpointWithStatus(KeyValueEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public abstract boolean shouldUseForRead();
    public abstract boolean shouldCountForRead();
    public abstract boolean shouldUseForWrite();
    public abstract boolean shouldCountForWrite();

    public boolean shouldUseFor(boolean write) {
        if (write) {
            return shouldUseForWrite();
        }
        return shouldUseForRead();
    }

    public boolean shouldCountFor(boolean write) {
        if (write) {
            return shouldCountForWrite();
        }
        return shouldCountForRead();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " kvs=" + endpoint.hashCode();
    }

}
