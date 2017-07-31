/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ImmutableRemotePaxosServerSpec;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.timelock.paxos.LeadershipResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.paxos.PingableLeaderLocator;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.timelock.Observables;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public class NamespacedPaxosLeadershipCreator {
    private final TimeLockInstallConfiguration install;
    private final Observable<Optional<PaxosRuntimeConfiguration>> runtime;
    private final Consumer<Object> registrar;
    private final PingableLeaderLocator locator;

    private LeaderElectionService namespacedLeaderElectionService;
    private LeadershipResource namespacedLeadershipResource;

    public NamespacedPaxosLeadershipCreator(
            TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this.install = install;
        this.runtime = runtime.map(TimeLockRuntimeConfiguration::algorithm);
        this.registrar = registrar;
        this.locator = new PingableLeaderLocator();
    }

    public void registerLeaderLocator() {
        registrar.accept(locator);
    }

    public synchronized void registerLeaderElectionServiceForClient(String client) {
        if (locator.hasClient(client)) {
            return;
        }
        Set<String> remoteServers = PaxosRemotingUtils.getRemoteServerPaths(install);

        LeaderConfig leaderConfig = getLeaderConfig(client);

        Set<String> paxosSubresourceUris = getNamespacedUris(remoteServers,
                PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                client,
                "leadership");

        Leaders.LocalPaxosServices localPaxosServices = Leaders.createInstrumentedLocalServices(
                leaderConfig,
                ImmutableRemotePaxosServerSpec.builder()
                        .remoteLeaderUris(
                                getNamespacedUris(
                                        remoteServers,
                                        client)
                        ).remoteAcceptorUris(paxosSubresourceUris)
                        .remoteLearnerUris(paxosSubresourceUris)
                        .build(),
                "leader-election-service");

        namespacedLeadershipResource = new LeadershipResource(
                localPaxosServices.ourAcceptor(),
                localPaxosServices.ourLearner());
        namespacedLeaderElectionService = localPaxosServices.leaderElectionService();
        locator.register(client, localPaxosServices.pingableLeader());
    }

    public synchronized LeadershipResource getNamespacedLeadershipResource() {
        return namespacedLeadershipResource;
    }

    public synchronized <T> T wrapInLeadershipProxy(Supplier<T> delegateSupplier, Class<T> clazz) {
        return AwaitingLeadershipProxy.newProxyInstance(
                clazz,
                delegateSupplier::get,
                namespacedLeaderElectionService);
    }

    private LeaderConfig getLeaderConfig(String client) {
        // TODO (jkong): Live Reload Paxos Ping Rates
        PaxosRuntimeConfiguration paxosRuntimeConfiguration = Observables.blockingMostRecent(runtime).get().orElse(
                ImmutablePaxosRuntimeConfiguration.builder().build());
        return ImmutableLeaderConfig.builder()
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .leaders(PaxosRemotingUtils.addProtocols(install, PaxosRemotingUtils.getClusterAddresses(install)))
                .localServer(PaxosRemotingUtils.addProtocol(install,
                        PaxosRemotingUtils.getClusterConfiguration(install).localServer()))
                .acceptorLogDir(Paths.get(install.algorithm().dataDirectory().toString(),
                        client,
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH).toFile())
                .learnerLogDir(Paths.get(install.algorithm().dataDirectory().toString(),
                        client,
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toFile())
                .pingRateMs(paxosRuntimeConfiguration.pingRateMs())
                .quorumSize(PaxosRemotingUtils.getQuorumSize(PaxosRemotingUtils.getClusterAddresses(install)))
                .leaderPingResponseWaitMs(paxosRuntimeConfiguration.pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(paxosRuntimeConfiguration.pingRateMs())
                .build();
    }


    private static Set<String> getNamespacedUris(Set<String> addresses, String... suffixes) {
        String joinedSuffix = String.join("/", suffixes);
        return addresses.stream()
                .map(address -> String.join("/", address, joinedSuffix))
                .collect(Collectors.toSet());
    }
}
