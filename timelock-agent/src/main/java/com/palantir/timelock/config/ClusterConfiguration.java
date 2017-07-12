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
    @JsonProperty("local-server")
    String localServer();

    // in remoting3 this class is officially public API
    // TODO: do we want to live reload this? probably not? how do we make a timelock cluster bigger?
    ServiceConfiguration cluster();

    @Value.Check
    default void check() {
        Preconditions.checkArgument(cluster().uris().contains(localServer()),
                "The localServer '%s' must be included in the server entries: %s.", localServer(), cluster().uris());
    }
}
