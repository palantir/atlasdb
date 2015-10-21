package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonDeserialize(as = ImmutableWriteOpts.class)
@JsonSerialize(as = ImmutableWriteOpts.class)
@Value.Immutable
public abstract class WriteOpts {

    @Value.Default
    public boolean fsyncPut() {
        return true;
    }

    @Value.Default
    public boolean fsyncCommit() {
        return true;
    }
}
