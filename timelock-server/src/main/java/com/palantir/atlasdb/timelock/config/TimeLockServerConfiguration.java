/*
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

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

public class TimeLockServerConfiguration extends Configuration {
    public static final String CLIENT_NAME_REGEX = "[a-zA-Z0-9_-]+";

    private final TimeLockAlgorithmConfiguration algorithm;
    private final ClusterConfiguration cluster;
    private final Set<String> clients;
    private final boolean useThreadPooling;

    public TimeLockServerConfiguration(
            @JsonProperty(value = "algorithm", required = false) TimeLockAlgorithmConfiguration algorithm,
            @JsonProperty(value = "cluster", required = true) ClusterConfiguration cluster,
            @JsonProperty(value = "clients", required = true) Set<String> clients,
            @JsonProperty(value = "useThreadPooling", required = false) Boolean useThreadPooling) {
        Preconditions.checkState(!clients.isEmpty(), "'clients' should have at least one entry");
        checkClientNames(clients);
        if (Boolean.TRUE.equals(useThreadPooling)) {
            Preconditions.checkState(computeNumberOfAvailableThreads() > 0,
                    "Configuration enables threadPooling, but specifies non-positive number of available threads.");
        }

        this.algorithm = MoreObjects.firstNonNull(algorithm, AtomixConfiguration.DEFAULT);
        this.cluster = cluster;
        this.clients = clients;
        this.useThreadPooling = MoreObjects.firstNonNull(useThreadPooling, false);
    }

    private void checkClientNames(Set<String> clientNames) {
        clientNames.forEach(client -> Preconditions.checkState(
                client.matches(CLIENT_NAME_REGEX),
                String.format("Client names must consist of alphanumeric characters, underscores or dashes only; "
                        + "'%s' does not.", client)));
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

    /**
     * Log at INFO if a lock request receives a response after given duration in milliseconds.
     * Default value is 10000 millis or 10 seconds.
     */
    @Value.Default
    public long slowLockLogTriggerMillis() {
        return 10000;
    }

    public boolean useThreadPooling() {
        return useThreadPooling;
    }

    public int availableThreads() {
        if (!useThreadPooling()) {
            throw new IllegalStateException("Should not call availableThreads() if useThreadPooling is disabled");
        }

        return computeNumberOfAvailableThreads();
    }

    private int computeNumberOfAvailableThreads() {
        DefaultServerFactory serverFactory = (DefaultServerFactory) getServerFactory();
        int maxServerThreads = serverFactory.getMaxThreads();
        HttpConnectorFactory connectorFactory = (HttpConnectorFactory) serverFactory.getApplicationConnectors().get(0);
        int selectorThreads = connectorFactory.getSelectorThreads();
        int acceptorThreads = connectorFactory.getAcceptorThreads();

        //TODO consider reserving numClients more threads or something similar for unlocks
        return maxServerThreads - selectorThreads - acceptorThreads - 1;
    }
}
