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
public abstract class SharedSecretHeader {

    public abstract SharedSecret secretObj();

    @JsonCreator
    public static SharedSecretHeader valueOf(String header) {
        SharedSecret sharedSecret = SharedSecret.valueOf(header.replaceFirst("^Bearer ", "").trim());
        return ImmutableSharedSecretHeader.of(sharedSecret);
    }
    public static SharedSecretHeader of(SharedSecret sharedSecret) {
        return ImmutableSharedSecretHeader.builder().secretObj(sharedSecret).build();
    }

    /**
     * Generates the value for an HTTP Authorization header with this shared secret of the form
     * "Bearer &lt;secret-value&gt;".
     */
    @Override
    @JsonValue
    public String toString() {
        return "Bearer " + secretObj();
    }
}
