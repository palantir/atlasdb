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
package com.palantir.atlasdb.timelock.config;

import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

@JsonSerialize(as = ImmutableClusterConfiguration.class)
@JsonDeserialize(as = ImmutableClusterConfiguration.class)
@Value.Immutable
public abstract class ClusterConfiguration {
    public abstract String localServer();

    @Size(min = 1)
    public abstract Set<String> servers();

    @Value.Default
    public boolean enableNonstandardAndPossiblyDangerousTopology() {
        return false;
    }

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(servers().contains(localServer()),
                "The localServer '%s' must be included in the server entries %s.", localServer(), servers());
        checkServersAreWellFormed();
        if (!enableNonstandardAndPossiblyDangerousTopology()) {
            checkTopologyOffersHighAvailability();
        }
    }

    protected void checkTopologyOffersHighAvailability() {
        Preconditions.checkArgument(servers().size() >= 3,
                "This TimeLock cluster is set up to use %s (<3) servers, which is not a standard configuration!"
                        + " With fewer than three servers, your service will not have high availability. In the"
                        + " event a node goes down, timelock will become unresponsive, meaning that ALL your AtlasDB"
                        + " clients will become unable to perform transactions. Furthermore your TimeLock cluster has"
                        + " NO resilience to failures of the underlying storage layer; if ANY node loses its logs,"
                        + " the timestamp information may be IRRECOVERABLY COMPROMISED, meaning that your AtlasDB"
                        + " deployments may become completely unusable."
                        + " If you know what you are doing and you want to run in this configuration, you must set"
                        + " 'enableNonstandardAndPossiblyDangerousTopology' to true.",
                servers().size());
    }

    private void checkServersAreWellFormed() {
        servers().forEach(ClusterConfiguration::checkHostPortString);
    }

    @VisibleForTesting
    static void checkHostPortString(String server) {
        HostAndPort hostAndPort = HostAndPort.fromString(server);
        Preconditions.checkArgument(hostAndPort.hasPort(), "Port not present: '%s'", server);
    }
}
