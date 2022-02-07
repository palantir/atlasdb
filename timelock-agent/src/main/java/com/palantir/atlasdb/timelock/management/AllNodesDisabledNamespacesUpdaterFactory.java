/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.paxos.ImmutablePaxosRemoteClients;
import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients;
import com.palantir.atlasdb.timelock.paxos.PaxosResourcesFactory.TimelockPaxosInstallationContext;
import com.palantir.atlasdb.timelock.paxos.WithDedicatedExecutor;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class AllNodesDisabledNamespacesUpdaterFactory {
    private final AuthHeader authHeader;
    private final TimelockPaxosInstallationContext install;
    private final MetricsManager metricsManager;

    public AllNodesDisabledNamespacesUpdaterFactory(
            AuthHeader authHeader, TimelockPaxosInstallationContext install, MetricsManager metricsManager) {
        this.authHeader = authHeader;
        this.install = install;
        this.metricsManager = metricsManager;
    }

    public AllNodesDisabledNamespacesUpdater create(TimelockNamespaces localNamespaces) {
        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(install, metricsManager);

        List<WithDedicatedExecutor<DisabledNamespacesUpdaterService>> remoteUpdaters = remoteClients.updaters();

        Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> remoteExecutors =
                ImmutableMap.<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService>builder()
                        .putAll(KeyedStream.of(remoteUpdaters)
                                .mapKeys(WithDedicatedExecutor::service)
                                .map(WithDedicatedExecutor::executor)
                                .collectToMap())
                        .build();

        ImmutableList<DisabledNamespacesUpdaterService> remoteServices = ImmutableList.copyOf(
                remoteUpdaters.stream().map(WithDedicatedExecutor::service).collect(Collectors.toList()));

        return AllNodesDisabledNamespacesUpdater.create(authHeader, remoteServices, remoteExecutors, localNamespaces);
    }
}
