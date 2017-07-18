/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.remoting2.config.service.ServiceConfiguration;

@JsonDeserialize(as = ImmutableClusterConfiguration.class)
@JsonSerialize(as = ImmutableClusterConfiguration.class)
@Value.Immutable
public interface ClusterConfiguration {
    ServiceConfiguration cluster();

    @JsonProperty("local-server")
    String localServer();

    @Value.Check
    default void check() {
        Preconditions.checkState(cluster().uris().contains(localServer()),
                "The localServer '%s' must be included in the server entries: %s.", localServer(), cluster().uris());
    }
}
