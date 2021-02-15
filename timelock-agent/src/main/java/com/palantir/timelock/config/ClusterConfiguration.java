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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultClusterConfiguration.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DefaultClusterConfiguration.class, name = DefaultClusterConfiguration.TYPE),
    @JsonSubTypes.Type(value = KubernetesClusterConfiguration.class, name = KubernetesClusterConfiguration.TYPE)
})
public interface ClusterConfiguration {

    /** To access the members of the cluster, use {@link #clusterMembers()} instead. */
    PartialServiceConfiguration cluster();

    @JsonProperty("local-server")
    String localServer();

    List<String> clusterMembers();

    /**
     * Used as part of automated TimeLock migrations to override is-new-service checks for nodes that are about to
     * be bootstrapped in a cluster while still allowing the rest of the cluster to recognise that it is old. This flag
     * should ONLY be used if you know what you are doing.
     */
    @JsonProperty("known-new-servers-I-know-what-I-am-doing")
    List<String> knownNewServers();

    @Value.Default
    default boolean enableNonstandardAndPossiblyDangerousTopology() {
        return false;
    }

    @Value.Check
    default void checkClusterMembersIncludesLocalServer() {
        Preconditions.checkArgument(
                clusterMembers().contains(localServer()),
                "The localServer must be included in the server entries.",
                SafeArg.of("localServer", localServer()),
                SafeArg.of("clusterMembers", clusterMembers()));
    }

    @Value.Check
    default void checkTopologyOffersHighAvailability() {
        if (enableNonstandardAndPossiblyDangerousTopology()) {
            return;
        }

        Preconditions.checkArgument(
                clusterMembers().size() >= 3,
                "This TimeLock cluster is set up to use an insufficient (< 3) number of servers, which is not a"
                        + " standard configuration! With fewer than three servers, your service will not have high"
                        + " availability. In the event a node goes down, timelock will become unresponsive, meaning"
                        + " that ALL your AtlasDB clients will become unable to perform transactions. Furthermore, if"
                        + " 1-node, your TimeLock  cluster has NO resilience to failures of the underlying storage"
                        + " layer; if your disks fail, the timestamp information may be IRRECOVERABLY COMPROMISED,"
                        + " meaning that your AtlasDB deployments may become completely unusable."
                        + " If you know what you are doing and you want to run in this configuration, you must set"
                        + " 'enableNonstandardAndPossiblyDangerousTopology' to true.",
                SafeArg.of("clusterSize", clusterMembers().size()),
                SafeArg.of("minimumClusterSize", 3));
    }
}
