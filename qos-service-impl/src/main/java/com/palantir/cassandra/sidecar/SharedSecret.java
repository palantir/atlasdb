/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.cassandra.sidecar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.immutables.value.Value;

/**
 * Copied from internal sls Cassandra project.
 */
@Value.Immutable
public abstract class SharedSecret {

    @JsonValue
    public abstract String secret();

    @JsonCreator
    public static SharedSecret valueOf(String sharedSecret) {
        return ImmutableSharedSecret.builder().secret(sharedSecret).build();
    }

    @Override
    public final String toString() {
        return secret();
    }

}
