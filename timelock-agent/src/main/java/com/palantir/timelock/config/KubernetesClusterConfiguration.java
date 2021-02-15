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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.timelock.utils.KubernetesHostnames;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Generates the current hostname, and those of the expected members of the cluster if the server is being executed
 * within a Kubernetes stateful set.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableKubernetesClusterConfiguration.class)
@JsonDeserialize(as = ImmutableKubernetesClusterConfiguration.class)
public interface KubernetesClusterConfiguration extends ClusterConfiguration {

    String TYPE = "kubernetes";

    /** The number of members expected to join the cluster. */
    @JsonProperty("expected-cluster-size")
    int expectedClusterSize();

    @JsonProperty("port")
    int port();

    /**
     * The path to the api uri.
     * <p>
     * Used when generating the uris for the members of the cluster.
     */
    @JsonProperty("path")
    String path();

    @Override
    default List<String> clusterMembers() {
        return KubernetesHostnames.INSTANCE.getClusterMembers(expectedClusterSize()).stream()
                .map(hostname -> String.format("%s:%s%s", hostname, port(), path()))
                .collect(Collectors.toList());
    }

    @Override
    default String localServer() {
        return String.format("%s:%s%s", KubernetesHostnames.INSTANCE.getCurrentHostname(), port(), path());
    }

    @Value.Check
    default void k8sCheck() {
        Preconditions.checkState(
                expectedClusterSize() > 0,
                "expected-cluster-size must be a positive integer; provided: %s.",
                expectedClusterSize());
        Preconditions.checkState(port() > 0 && port() < 65535, "Invalid port specified: %s", port());
    }
}
