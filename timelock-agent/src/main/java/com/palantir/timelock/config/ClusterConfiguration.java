/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
