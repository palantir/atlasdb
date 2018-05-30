/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.timelock.config.ImmutablePaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

public class TimeLockServerConfiguration extends Configuration {
    private static final Logger log = LoggerFactory.getLogger(TimeLockServerConfiguration.class);

    public static final String CLIENT_NAME_REGEX = "[a-zA-Z0-9_-]+";

    private final TimeLockAlgorithmConfiguration algorithm;
    private final ClusterConfiguration cluster;
    private final Set<String> clients;
    private final AsyncLockConfiguration asyncLockConfiguration;
    private final boolean useClientRequestLimit;
    private final TimeLimiterConfiguration timeLimiterConfiguration;
    private final TsBoundPersisterConfiguration tsBoundPersisterConfiguration;

    public TimeLockServerConfiguration(
            @JsonProperty(value = "algorithm", required = false) TimeLockAlgorithmConfiguration algorithm,
            @JsonProperty(value = "cluster", required = true) ClusterConfiguration cluster,
            @JsonProperty(value = "clients", required = true) Set<String> clients,
            @JsonProperty(value = "asyncLock", required = false) AsyncLockConfiguration asyncLockConfiguration,
            @JsonProperty(value = "timeLimiter", required = false) TimeLimiterConfiguration timeLimiterConfiguration,
            @JsonProperty(value = "timestampBoundPersister", required = false)
                    TsBoundPersisterConfiguration tsBoundPersisterConfiguration,
            @JsonProperty(value = "useClientRequestLimit", required = false) Boolean useClientRequestLimit) {
        checkClientNames(clients);
        if (Boolean.TRUE.equals(useClientRequestLimit)) {
            Preconditions.checkState(computeNumberOfAvailableThreads() > 0,
                    "Configuration enables clientRequestLimit but specifies non-positive number of available threads.");
        }

        this.algorithm = MoreObjects.firstNonNull(algorithm, PaxosConfiguration.DEFAULT);
        this.cluster = cluster;
        this.clients = clients;
        this.asyncLockConfiguration = MoreObjects.firstNonNull(
                asyncLockConfiguration, ImmutableAsyncLockConfiguration.builder().build());
        this.useClientRequestLimit = MoreObjects.firstNonNull(useClientRequestLimit, false);
        this.timeLimiterConfiguration =
                MoreObjects.firstNonNull(timeLimiterConfiguration, TimeLimiterConfiguration.getDefaultConfiguration());
        this.tsBoundPersisterConfiguration = MoreObjects.firstNonNull(tsBoundPersisterConfiguration,
                getPaxosTsBoundPersisterConfiguration());

        if (clients.isEmpty()) {
            log.warn("TimeLockServer initialised with an empty list of 'clients'."
                    + " When adding clients, you will need to amend the config and restart TimeLock.");
        }
    }

    private TsBoundPersisterConfiguration getPaxosTsBoundPersisterConfiguration() {
        return ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
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

    public boolean useClientRequestLimit() {
        return useClientRequestLimit;
    }

    public TimeLimiterConfiguration timeLimiterConfiguration() {
        return timeLimiterConfiguration;
    }

    public AsyncLockConfiguration asyncLockConfiguration() {
        return asyncLockConfiguration;
    }

    public TsBoundPersisterConfiguration getTsBoundPersisterConfiguration() {
        return tsBoundPersisterConfiguration;
    }

    public int availableThreads() {
        if (!useClientRequestLimit()) {
            throw new IllegalStateException("Should not call availableThreads() if useClientRequestLimit is disabled");
        }

        return computeNumberOfAvailableThreads();
    }

    private int computeNumberOfAvailableThreads() {
        Preconditions.checkState(getServerFactory() instanceof DefaultServerFactory,
                "Unexpected serverFactory instance on TimeLockServerConfiguration.");
        DefaultServerFactory serverFactory = (DefaultServerFactory) getServerFactory();
        int maxServerThreads = serverFactory.getMaxThreads();

        Preconditions.checkNotNull(serverFactory.getApplicationConnectors(),
                "applicationConnectors of TimeLockServerConfiguration must not be null.");
        Preconditions.checkState(serverFactory.getApplicationConnectors().get(0) instanceof HttpConnectorFactory,
                "applicationConnectors of TimeLockServerConfiguration must have a HttpConnectorFactory instance.");
        HttpConnectorFactory connectorFactory = (HttpConnectorFactory) serverFactory.getApplicationConnectors().get(0);

        // In both of these cases, Runtime.getRuntime().availableProcessors() will be a small overestimate.
        int selectorThreads = connectorFactory.getSelectorThreads().orElse(Runtime.getRuntime().availableProcessors());
        int acceptorThreads = connectorFactory.getAcceptorThreads().orElse(Runtime.getRuntime().availableProcessors());

        // TODO(gmaretic): consider reserving numClients more threads or something similar for unlocks
        return maxServerThreads - selectorThreads - acceptorThreads - 1;
    }
}
