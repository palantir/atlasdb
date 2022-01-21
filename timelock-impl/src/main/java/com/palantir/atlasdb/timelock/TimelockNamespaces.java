/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.management.DisabledNamespaces;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public final class TimelockNamespaces {
    @VisibleForTesting
    static final String ACTIVE_CLIENTS = "activeClients";

    @VisibleForTesting
    static final String MAX_CLIENTS = "maxClients";

    private static final ImmutableSet<String> BANNED_CLIENTS = ImmutableSet.of("tl", "lw");
    private static final String PATH_REGEX =
            String.format("^(?!((%s)$))[a-zA-Z0-9_-]+$", String.join("|", BANNED_CLIENTS));

    @VisibleForTesting
    static final Predicate<String> IS_VALID_NAME = Pattern.compile(PATH_REGEX).asPredicate();

    private static final SafeLogger log = SafeLoggerFactory.get(TimelockNamespaces.class);

    private final ConcurrentMap<String, TimeLockServices> services = new ConcurrentHashMap<>();
    private final Function<String, TimeLockServices> factory;
    private final Supplier<Integer> maxNumberOfClients;
    private final DisabledNamespaces disabledNamespaces;

    public TimelockNamespaces(
            MetricsManager metrics,
            Function<String, TimeLockServices> factory,
            Supplier<Integer> maxNumberOfClients,
            DisabledNamespaces disabledNamespaces) {
        this.factory = factory;
        this.maxNumberOfClients = maxNumberOfClients;
        this.disabledNamespaces = disabledNamespaces;
        registerClientCapacityMetrics(metrics);
    }

    public TimeLockServices get(String namespace) {
        return services.computeIfAbsent(namespace, this::createNewClient);
    }

    public Set<Client> getActiveClients() {
        return services.keySet().stream().map(Client::of).collect(toSet());
    }

    public int getNumberOfActiveClients() {
        return services.size();
    }

    public int getMaxNumberOfClients() {
        return maxNumberOfClients.get();
    }

    private TimeLockServices createNewClient(String namespace) {
        Preconditions.checkArgument(
                IS_VALID_NAME.test(namespace), "Invalid namespace", SafeArg.of("namespace", namespace));
        Preconditions.checkArgument(
                !namespace.equals(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                "The client name is reserved for the leader election service, and may not be used.",
                SafeArg.of("clientName", PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE));
        Preconditions.checkArgument(
                disabledNamespaces.isEnabled(Namespace.of(namespace)),
                "Cannot create a client for namespace because the namespace has been explicitly disabled.",
                SafeArg.of("namespace", namespace));

        if (getNumberOfActiveClients() >= getMaxNumberOfClients()) {
            log.error(
                    "Unable to create timelock services for client {}, as it would exceed the maximum number of "
                            + "allowed clients ({}). If this is intentional, the maximum number of clients can be "
                            + "increased via the maximum-number-of-clients runtime config property.",
                    SafeArg.of("client", namespace),
                    SafeArg.of("maxNumberOfClients", getMaxNumberOfClients()));
            throw new SafeIllegalStateException("Maximum number of clients exceeded");
        }

        TimeLockServices services = factory.apply(namespace);
        log.info("Successfully created services for a new TimeLock client {}.", SafeArg.of("client", namespace));
        return services;
    }

    public void invalidateResourcesForClient(String namespace) {
        log.info(
                "Attempting to invalidate resources for a given timelock client",
                SafeArg.of("client", namespace),
                SafeArg.of("doResourcesPossiblyExist", services.containsKey(namespace)));
        TimeLockServices removedServices = services.remove(namespace);
        if (removedServices != null) {
            removedServices.close();
        }
    }

    public DisableNamespacesResponse disable(DisableNamespacesRequest request) {
        DisableNamespacesResponse response = disabledNamespaces.disable(request);
        response.accept(new DisableNamespacesResponse.Visitor<Void>() {
            @Override
            public Void visitSuccessful(SuccessfulDisableNamespacesResponse _unused) {
                request.getNamespaces().stream().map(Namespace::get).forEach(ns -> invalidateResourcesForClient(ns));
                return null;
            }

            @Override
            public Void visitUnsuccessful(UnsuccessfulDisableNamespacesResponse response) {
                log.info("Not invalidating resources, as the request to disable namespaces was unsuccessful",
                        SafeArg.of("response", response));
                return null;
            }

            @Override
            public Void visitUnknown(String unknownTypeThatShouldNeverHappen) {
                throw new SafeIllegalStateException("Unknown response when disabling namespaces",
                        SafeArg.of("response", unknownTypeThatShouldNeverHappen));
            }
        });
        return response;
    }

    public ReenableNamespacesResponse reEnable(ReenableNamespacesRequest request) {
        return disabledNamespaces.reEnable(request);
    }

    private void registerClientCapacityMetrics(MetricsManager metricsManager) {
        metricsManager.registerMetric(TimelockNamespaces.class, ACTIVE_CLIENTS, this::getNumberOfActiveClients);
        metricsManager.registerMetric(TimelockNamespaces.class, MAX_CLIENTS, this::getMaxNumberOfClients);
    }
}
