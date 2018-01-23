/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.palantir.remoting.api.config.service.PartialServiceConfiguration;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = DefaultClusterConfiguration.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DefaultClusterConfiguration.class,
                name = DefaultClusterConfiguration.TYPE),
        @JsonSubTypes.Type(value = KubernetesClusterConfiguration.class,
                name = KubernetesClusterConfiguration.TYPE)})
public interface ClusterConfiguration {

    /** To access the members of the cluster, use {@link #clusterMembers()} instead. */
    PartialServiceConfiguration cluster();

    @JsonProperty("local-server")
    String localServer();

    List<String> clusterMembers();

    @Value.Check
    default void check() {
        Preconditions.checkState(clusterMembers().contains(localServer()),
                "The localServer '%s' must be included in the server entries: %s.", localServer(), clusterMembers());
    }
}
