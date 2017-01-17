/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock.config;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

import io.dropwizard.Configuration;

public class TimeLockServerConfiguration extends Configuration {
    private final TimeLockAlgorithmConfiguration algorithm;
    private final ClusterConfiguration cluster;
    private final Set<String> clients;

    public TimeLockServerConfiguration(
            @JsonProperty(value = "algorithm", required = false) TimeLockAlgorithmConfiguration algorithm,
            @JsonProperty(value = "cluster", required = true) ClusterConfiguration cluster,
            @JsonProperty(value = "clients", required = true) Set<String> clients) {
        Preconditions.checkState(!clients.isEmpty(), "'clients' should have at least one entry");
        checkClientNames(clients);

        this.algorithm = MoreObjects.firstNonNull(algorithm, AtomixConfiguration.DEFAULT);
        this.cluster = cluster;
        this.clients = clients;
    }

    private void checkClientNames(Set<String> clientNames) {
        clientNames.forEach(client -> Preconditions.checkState(
                client.matches("[a-zA-Z0-9_-]+"),
                String.format("Client names must consist of alphanumeric characters, underscores or dashes only; "
                        + "'%s' does not.", client)));
        clientNames.forEach(client -> Preconditions.checkState(
                !client.startsWith("__"),
                String.format("Names starting with two or more underscores are reserved for internal use; found "
                        + "'%s' specified as a client. Please rename it.", client)));
        Preconditions.checkState(!clientNames.contains(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                String.format("The namespace '%s' is reserved for the leader election service. Please use a different"
                        + " name.", PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE));

    }

    public TimeLockAlgorithmConfiguration algorithm() {
        return algorithm;
    }

    public ClusterConfiguration cluster() {
        return cluster;
    }

    public Set<String> clients() {
        return clients;
    }
}
